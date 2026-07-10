---
type: Architecture
title: End-to-End Pair Walkthrough
description: One worked example — (Szentendre, running) — traced from scheduler wake-up through search, fetch, extraction, storage, and the public page, naming every file on the path.
tags: [architecture, walkthrough, pipeline, example]
timestamp: 2026-07-10
resource: scraper/pipeline.py
---

# End-to-End Pair Walkthrough

*Follow one city×topic pair — (Szentendre, running) — through the whole system. Every
step names the module that owns it.*

## 1. Wake-up — `scraper/main.py`

APScheduler fires the collector cron at 01:00 UTC (`search_only`, boxed until 16:20).
`main.py` partitions cities into three sequential `run_pipeline()` calls (Hungary →
Sweden → world), so Szentendre is in pass 1.

## 2. Skip-or-work — `scraper/pipeline.py`

`run_pipeline()` first calls `get_fully_processed_pairs()` — one SQL query comparing
`search_cache` coverage and `cache_pages` fingerprints against the current
[[extraction-fingerprint-cache]] value. A fully covered pair is skipped with **zero**
log lines or API calls. Assume (Szentendre, running) is stale: it enters the loop and
`on_pair_start` publishes it to `/admin/api/coverage/current` (live "jump to active").

## 3. Search — `scraper/search.py` → [[dataforseo]]

`FallbackSearchClient.search_all()` issues the locale-built queries ("futóklub
Szentendre", …) in standard (queued) mode, stopping early once `stop_after` unique
URLs are found. Success — even with zero results — is written to `search_cache`;
quota/transient failures raise and are NOT cached.

## 4. Fetch — `scraper/fetch.py`

Each result URL passes the SSRF gauntlet ([[server-side-url-safety]]): scheme, public
DNS, blocked-domain list (Facebook/Instagram/… return `None` immediately), redirect
re-validation. Clean text lands in `cache_pages` keyed by SHA-256(url)[:16]
([[url-hash-triplicated]]).

## 5. Extract — `scraper/extract.py` → [[deepseek]]

In the evening `ai_only` window the extractor cron re-reads those cached texts. For
each URL the fingerprint cache is checked first; a miss calls DeepSeek with the
community prompt (+ injected [[false-positive-injection]] negatives). Persons and
venues are skipped entirely when a URL yields no communities. Results are cached
under the current fingerprint; failures raise and skip caching.

## 6. Store — `scraper/store.py` → `scraper/db.py`

`save_results()` merges new records with existing ones, fuzzy-dedups
([[fuzzy-dedup-and-record-key]]), and upserts via `record_key`; `hidden` and
`revalidate_fingerprint` survive the replace. `community_history` gets `__created__`
rows for genuinely new records ([[history-created-sentinel-overcounting]]), then
[[duplicate-detection]] re-scans the city.

## 7. Serve — `scraper/web/app.py`

`https://kozossegek.com/szentendre` → Host header picks the site
([[two-domain-single-container]]), `_render_explore` builds one two-column card grid
(icon + name + description only) with client-side topic-chip filtering. The page
self-canonicalizes to kozossegek.com; the same path on meetapedia.com canonicalizes
cross-domain ([[seo-cross-domain-canonical]]).

## 8. Report — [[daily-report]]

Next morning 04:30 UTC the daily email counts the new Szentendre records in the
Változások table and the updated stock totals, with GA4 visitor numbers on top.
