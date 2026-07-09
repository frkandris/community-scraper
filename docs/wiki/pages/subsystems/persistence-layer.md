---
type: Subsystem
title: Persistence Layer (db / cache / store)
description: db.py owns all SQL, cache.py is a JSON-blob facade over cache_pages, store.py merges/dedups community records before upsert.
tags: [persistence, sqlite, cache, store, dedup]
timestamp: 2026-07-09
resource: scraper/db.py
---

# Persistence Layer

*Three files: `db.py` (~2400 lines of raw SQLite functions), `cache.py` (a `CacheManager` facade over the `cache_pages` JSON blob), and `store.py` (community merge/dedup before upsert).*

See [[sqlite-schema]] for the full table catalog and [[extraction-fingerprints]] for the fingerprint columns.

## Connection model

Every function opens its own short-lived `sqlite3.connect(db_path, timeout=30)` via `_connect` (`db.py:17`), sets `PRAGMA busy_timeout=5000` and `PRAGMA foreign_keys=ON`; `init_db` additionally sets `journal_mode=WAL`. There is **no connection pool**. WAL + `busy_timeout` let the web UI and the scraper read/write concurrently without "database is locked". Most read functions guard `if not db_path.exists(): return []` because the DB legitimately may not exist on first boot.

`foreign_keys=ON` is set but **no table declares a FOREIGN KEY** — all cross-table links are soft (string `community_id`, JSON `community_ids`). The pragma is currently a no-op; integrity is entirely application-level.

## Migration strategy: guarded ALTER

`init_db()` is the entire migration framework — no version table. Pattern: `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`, and for new columns `try: ALTER TABLE ... ADD COLUMN / except sqlite3.OperationalError: pass` (the ALTER throws `duplicate column name` when already present, so the bare except makes it idempotent). Some columns (`runs.search_log`, `cache_pages.extract_fingerprint`) appear in **both** the CREATE and a guarded ALTER — belt-and-suspenders for DBs created before the column was added to CREATE. It is safe to call `init_db()` on every request; see [[init-db-before-prompt-overrides]] for the one thing it must *not* do (fingerprint migrations).

Two idempotent back-fills run each init: the `hidden` flag from `not_community_reports` (wrapped in try/except because that table is created later in the same function — an ordering dependency), and the `venue_fingerprint`/`person_fingerprint` columns from the JSON blob via `json_extract`.

## cache.py — read-modify-write over a JSON blob

`CacheManager` holds only `db_path`. Every method computes `_url_hash(url)`, calls `load_cache_page` to read the blob, mutates the Python dict, and calls `save_cache_page` to write it back. `db.py` owns SQL; `cache.py` owns blob semantics. This read-modify-write across two separate connections is **not transactional** — concurrent writers to the same `url_hash` can lose updates (last-writer-wins on the whole blob). See [[cache-blob-read-modify-write]].

Fingerprint-gated reads (`get_extracted`, `get_venue_extracted`, `get_person_extracted`) return `None` (cache miss → re-extract) when the stored fingerprint ≠ current. `save_extracted` nulls all `enrich_*` markers, since a fresh extraction invalidates prior enrichment. Deletes are soft (pop keys out of the blob); only `delete_entry` removes the row.

## store.py — merge, patch, dedup

- `save_results` merges NEW records over existing (new wins on `record_key` collision).
- `patch_results` fills only NULL fields, never overwriting non-null (`_PATCHABLE_FIELDS`).
- `_dedup` is fuzzy: same website (trailing-slash-stripped), substring-after-article-strip, or `SequenceMatcher ratio > 0.88`; on collision keeps the **richer** record (more populated fields). See [[fuzzy-dedup-and-record-key]].
- `_merge_source_urls` (`db.py:645`) unions old+new URLs new-first via `dict.fromkeys`, so re-finding a community appends provenance rather than replacing it. This idiom is re-implemented in four places (communities, venues, persons, merge) — keep them in sync.

## Key correctness rules

- **`record_key` = `norm(name)|norm(city)|norm(topic)`** is derived identically in `store.py` and `db.py` (`re.sub(r"[^a-z0-9]+","_", s.lower())`). Duplicated logic across two files — must stay identical or store-layer dedup and db-layer upsert disagree. Distinct from the stable `community_id`; see [[community-identity]].
- **Done-pair detection resolves URLs→hashes, never JOINs on `cache_pages.city`/`topic`**, because those columns are last-write-wins. See [[done-pair-url-hash-not-city-topic]].
- **History `__created__` sentinel + delete/reinsert overcounting** — see [[history-created-sentinel-overcounting]].
- `replace_communities_for_topic` snapshots existing rows *before* DELETE so history can still diff old vs new; without the snapshot every save re-logs every field as a change.
