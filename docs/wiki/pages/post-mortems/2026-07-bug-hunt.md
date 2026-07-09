---
type: Post-mortem
title: Repo-Wide Bug Hunt (2026-07)
description: Three-agent review found 15+ verified defects; fixed in three batches — moderation survival, domain matching, persons lookup, recategorize, venue scope, timeline dedup, and a set of hot-path optimizations.
tags: [bug-hunt, review, quality, optimization]
timestamp: 2026-07-09
resource: scraper/db.py
---

# Repo-Wide Bug Hunt (2026-07)

*Three parallel review agents (recent-changes, persistence, web layer) + manual verification. Every finding was confirmed against code before fixing. Shipped in three batches.*

## Batch A — severe

- **Hidden communities resurfaced on re-scrape**: `replace_communities_for_topic`'s DELETE+reinsert dropped `hidden` + `revalidate_fingerprint`; a merged/reported community came back publicly and could never be re-flagged (the resolved `duplicate_candidates` row blocks re-insert). Moderation state now survives the replace.
- **Blocked-domain substring false positives**: `'x.com' in host` blocked linux.com, maxx.com… Now domain-boundary matching (`host == d or host.endswith('.'+d)`) shared by fetch, the pipeline pre-filter, and PlaywrightFetcher.
- **Coverage page: 2 cities/page** — a shipped "for fast loading during testing" leftover (Hungary = 170 pages). Now 50.
- **Persons missing from public community pages**: the record_key LIKE anchored on the wrong key segments (~never matched) and the JSON fallback was case-sensitive. Now normalized community_name matching.
- **Recategorize left records publicly under "other"**: only the JSON topic changed, not the `topic` column every query filters on; plus an uncaught IntegrityError on key collision. Both fixed.

## Batch B — medium

- Venues-only admin runs were a no-op after the venue cost-gate; the gate now applies only when the communities pass actually ran.
- `scrape_submitted_url`/`reextract_community` no longer leak `ExtractorUnavailableError` into the BackgroundTasks runner (loud log + False instead).
- `og:url` follows the cross-domain canonical ([[seo-cross-domain-canonical]]) — share and search signals consolidate to the same URL.
- Activity-timeline `new_communities` got the same MIN(changed_at) dedup as venues/persons (delete+reinsert churn double-counted; see [[history-created-sentinel-overcounting]]).
- 8 typo'd `topics.yaml` search terms fixed (they were literal broken paid queries).

## Batch C — optimizations + small fixes

- `/terkep`: ~17,500 SELECTs/request → 1 (`get_city_totals`).
- `_render_explore`: per-topic double-loading → one `get_city_topic_counts` query for the chips.
- Coverage cell endpoint: global state recompute per 3 s poll → ~3 s memo (`_coverage_state`).
- Pipeline double-save removed: per-URL `save_results` was redundant with the pair-final batch save (each page re-ran O(n²) dedup + full topic DELETE+reinsert + a city-wide duplicate scan).
- New indexes: `cache_pages(extract_fingerprint)`, `{community,venue,person}_history(field, changed_at)`.
- `get_cache_index` no longer deserializes every page text (json_extract of small keys).
- `_slugify` lru_cache (774-city linear scans per request got cheap).
- `clear_person_cache` also nulls the `person_fingerprint` column (scope-stats drift).
- `apply_community_edit` recomputes `record_key` on name/city/topic edits (stale key caused duplicate rows on next scrape) with collision guard.
- RTL `lang_dir` uses `RTL_LANGS`; `request_city_en` delegates instead of dropping form data; dead templates (`stats_clicks.html`, `stats.html`) and dead db counters removed; public search LIKE-escapes wildcards; `get_search_cache` got the missing exists-guard; repo is now ruff-clean.

## Checked-OK worth remembering

No SQL injection surface (all parameterized); admin GETs are side-effect-free; `|safe` sinks are escaped; the cache read-modify-write is race-free *today* only because everything shares one event loop — one inserted `await` or a second uvicorn worker makes it a real lost-update path ([[cache-blob-read-modify-write]]).
