---
type: Data-model
title: SQLite Schema
description: Every table in scraper.db, its purpose, and its key columns — all created idempotently by init_db().
tags: [sqlite, schema, database, tables]
timestamp: 2026-07-24
resource: scraper/db.py
---

# SQLite Schema

*All tables live in `data/scraper.db`, created by `db.py:init_db()` with `CREATE TABLE IF NOT EXISTS` + guarded `ALTER TABLE ADD COLUMN`. WAL mode.*

See [[persistence-layer]] for the connection/migration model.

# Schema

| Table | Purpose | Key columns |
|---|---|---|
| `runs` | One row per scrape run | `run_mode` (default `full`), `success`, `search_log`, `new_records`, `error`; `start_run` inserts `success=0`, `finish_run` updates |
| `communities` | **Core** — one row per unique (name, city, topic) | `record_key UNIQUE`, `community_id`, `city`, `topic`, `data` (full JSON), `hidden` (plus a legacy `revalidate_fingerprint` column on older DBs); idx on (city,topic) and community_id |
| `cache_pages` | One row per scraped URL | PK `url_hash`; `url`, `city`, `topic`, `domain`, `scraped_at`, `extracted_at`, `extract_fingerprint`, `venue_fingerprint`, `person_fingerprint`, `data` (blob) |
| `search_cache` | URL lists per (city, topic) | composite PK `(city, topic)`; `urls`/`queries` JSON, `cached_at`, `collected_at`; TTL enforced at read time |
| `venues` | Physical locations | `record_key UNIQUE`, `venue_id`, `city`, `data`; **no topic column** (spans topics via JSON `welcomed_topics`) |
| `persons` | Leaders/instructors/speakers | `record_key UNIQUE`, `person_id`, `city`, `topic`, `role`, `data` |
| `community_history` / `venue_history` / `person_history` | Field-level change logs | `<entity>_id`, `changed_at`, `changed_by` (default `scraper`), `field`, `old_value`, `new_value` |
| `false_positives` | Manually flagged bad extractions | UNIQUE (name, city, topic, fp_type); `fp_type` default `extraction` |
| `prompt_history` | Versioned prompt snapshots per fp_type | `fp_count`, `version` |
| `prompt_overrides` | Editable prompt overrides | PK `key` |
| `subscriptions` | Email alerts | `token UNIQUE`, UNIQUE (email, city, topic) |
| `city_requests` | User requests for uncovered cities | `city_name`, `email` |
| `not_community_reports` | Pending user "this isn't a community" reports | `community_id` nullable; pending rows do not affect visibility |
| `duplicate_candidates` | Fuzzy dup pairs | `entity_type`, `winner_id/loser_id`, `winner_key/loser_key`, `similarity`, `resolution`; partial UNIQUE `WHERE resolution IS NULL` |
| `wrong_city_candidates` | Communities whose text mentions another known city | `record_key`, `mentioned_city`, `field`, `snippet`, `matched_text`, `resolution`; full UNIQUE (record_key, mentioned_city) so dismissed pairs are never re-raised — see [[wrong-city-detection]] |
| `edit_requests` | User-submitted edits pending review | `change_type`, `new_value`, `status` (default `pending`) |
| `community_submissions` | User-submitted new communities | pending admin approval |
| `outclick_events` | Outbound-link click analytics | `link_type` (default `website`), `clicked_at` |
| `schema_migrations` | One-time migration ledger | `name PRIMARY KEY`, `applied_at` |

## Notes and traps

- **`hidden`**: nearly all community reads filter `hidden=0`. A few paths intentionally read hidden rows (`get_community_by_record_key`, `apply_community_edit`, `merge_community_into`) — easy to forget which.
- **Pending moderation is inert**: inserting a `not_community_reports` row never changes `communities.hidden`. Only admin approval promotes it to a false positive and hides the matching `record_key`; dismissing it only removes the report. See [[not-community-moderation-flow]].
- **Three fingerprint columns** on `cache_pages` are stored both as columns (for fast SQL filtering/counting) and inside the JSON blob (source of truth). See [[extraction-fingerprints]].
- The recategorize feature (and its `recategorize_suggestions` table guard) was removed 2026-07-23; existing production tables are left orphaned.
- **`search_cache` TTL is not persisted** — the same row is "valid" or "expired" depending on the caller's `ttl_days` argument at read time.
- **`collected_at` is terminal-attempt state, not all-success state**: it is set after the selected fetch batch finishes even if individual URLs fail; interruption before the marker leaves the pair resumable.
- **Identity migration**: `unicode_record_keys_v2` rewrites legacy ASCII-only entity keys and all persisted references; see [[unicode-safe-identity-keys]].
