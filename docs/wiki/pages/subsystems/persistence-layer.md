---
type: Subsystem
title: Persistence Layer (db / cache / store)
description: db.py owns all SQL, cache.py is a JSON-blob facade over cache_pages, store.py merges/dedups community records before upsert.
tags: [persistence, sqlite, cache, store, dedup]
timestamp: 2026-07-23
resource: scraper/db.py
---

# Persistence Layer (db / cache / store)

*Three files: `db.py` owns raw SQLite functions, `cache.py` is a `CacheManager` facade over the `cache_pages` JSON blob, and `store.py` merges/deduplicates communities before upsert.*

See [[sqlite-schema]] for the full table catalog and [[extraction-fingerprints]] for the fingerprint columns.

## Connection model

Every function opens its own short-lived `sqlite3.connect(db_path, timeout=30)` via `_connect` (`db.py:17`), sets `PRAGMA busy_timeout=5000` and `PRAGMA foreign_keys=ON`; `init_db` additionally sets `journal_mode=WAL`. There is **no connection pool**. WAL + `busy_timeout` let the web UI and the scraper read/write concurrently without "database is locked". Most read functions guard `if not db_path.exists(): return []` because the DB legitimately may not exist on first boot.

`foreign_keys=ON` is set but **no table declares a FOREIGN KEY** — all cross-table links are soft (string `community_id`, JSON `community_ids`). The pragma is currently a no-op; integrity is entirely application-level.

## Migration strategy

`init_db()` uses `CREATE TABLE/INDEX IF NOT EXISTS` plus guarded `ALTER TABLE` for additive schema changes. One-time data migrations use the `schema_migrations` ledger; `unicode_record_keys_v2` is the first versioned migration. It is safe to call `init_db()` repeatedly; see [[init-db-before-prompt-overrides]] for why runtime fingerprints must not be migrated there.

The `venue_fingerprint`/`person_fingerprint` columns are idempotently back-filled from the JSON blob via `json_extract`.

## cache.py — read-modify-write over a JSON blob

`CacheManager` holds only `db_path`. Every method computes `_url_hash(url)`, calls `load_cache_page` to read the blob, mutates the Python dict, and calls `save_cache_page` to write it back. `db.py` owns SQL; `cache.py` owns blob semantics. This read-modify-write across two separate connections is **not transactional** — concurrent writers to the same `url_hash` can lose updates (last-writer-wins on the whole blob). See [[cache-blob-read-modify-write]].

Fingerprint-gated reads (`get_extracted`, `get_venue_extracted`, `get_person_extracted`) return `None` (cache miss → re-extract) when the stored fingerprint ≠ current. `save_extracted` nulls all `enrich_*` markers, since a fresh extraction invalidates prior enrichment. Deletes are soft (pop keys out of the blob); only `delete_entry` removes the row.

`invalidate_extraction_cache` is the targeted exception to the facade's read-modify-write pattern: one SQL update removes community extraction and enrichment keys while retaining `raw_text` and independent venue/person results. Pair scope is resolved by URL hash from `search_cache` plus the denormalized pair columns and optional source URL; omitted scope means every cached page. False-positive add/remove uses this path so an `ai_only` run can apply new moderation rules without another fetch. For that pass, `get_scraped_cache_by_search_pair` reconstructs authoritative pair attribution from `search_cache`; unlinked/manual pages fall back to their cache metadata.

## store.py — merge, dedup

- `save_results` merges NEW records over existing (new wins on `record_key` collision).
- (`patch_results` / `_PATCHABLE_FIELDS` were removed 2026-07-23 with the fill-fields flow — see [[admin-simplification-2026-07]].)
- `_dedup` is fuzzy: same website (trailing-slash-stripped), substring-after-article-strip, or `SequenceMatcher ratio > 0.88`; on collision keeps the **richer** record (more populated fields). See [[fuzzy-dedup-and-record-key]].
- `_merge_source_urls` (`db.py:645`) unions old+new URLs new-first via `dict.fromkeys`, so re-finding a community appends provenance rather than replacing it. This idiom is re-implemented in four places (communities, venues, persons, merge) — keep them in sync.

## Key correctness rules

- **`record_key` is centralized and Unicode-safe.** `scraper.identity` hashes NFKC+casefold canonical components with entity-specific prefixes; `unicode_record_keys_v2` migrates rows and references once. Distinct from the stable `community_id`; see [[community-identity]] and [[unicode-safe-identity-keys]].
- **Done-pair detection resolves URLs→hashes, never JOINs on `cache_pages.city`/`topic`**, because those columns are last-write-wins. See [[done-pair-url-hash-not-city-topic]].
- **History `__created__` sentinel + delete/reinsert overcounting** — see [[history-created-sentinel-overcounting]].
- `replace_communities_for_topic` snapshots existing rows *before* DELETE so history can still diff old vs new; without the snapshot every save re-logs every field as a change. The snapshot also carries each row's `updated_at` (`prev_updated`) into `_bulk_upsert_communities`.
- **`communities.updated_at` only advances on a real content change.** Because `replace_communities_for_topic` DELETEs first, the re-INSERT would otherwise stamp `now` on every row every run. `_bulk_upsert_communities` compares a content fingerprint (`_community_content_fingerprint`, which drops the volatile `extracted_at`) against the pre-delete snapshot and preserves the old timestamp when unchanged — making `updated_at` a trustworthy sitemap `<lastmod>` and avoiding whole-corpus freshness churn on a fingerprint re-extraction. See [[indexing-strategy]].
