---
type: Post-mortem
title: Dead Search Provider Produced 4972 Per-Pair Failures From 3 Real Errors
description: Unmapped city locales made every task_post fail with 40501 Invalid Field location_name; the fail-fast then amplified 3 poisoned pairs into 4972 logged failures while the email lost the original error.
tags: [post-mortem, search, dataforseo, saver, daily-report, observability, locale]
timestamp: 2026-07-23
resource: scraper/pipeline.py
---

# Dead Search Provider Produced 4972 Per-Pair Failures From 3 Real Errors

*A provider outage was amplified into thousands of bookkeeping "failures" while the one string that identified the outage was dropped before it reached the daily email.*

## Symptom

The 2026-07-22 daily report showed `❌ search_only · 4972 város–téma páros · 0 rekord — hibák: 4972 keresés`. Production logs showed the same pattern: `processing_pair` / `search_unavailable_pair_skipped` alternating for every remaining international pair, twice (main pass 2486 + catch-up pass 2486).

## Root cause

Two stacked defects, found on 2026-07-23 via the DataForSEO account error dashboard (29,879 errors, 16–23 July):

1. **The actual provider error** (~2026-07-16 onward): 12 city locales (`sk`, `sr`, `hr`, `sl`, `bg`, `lv`, `et`, `lt`, `el`, `ja`, `ko`, `zh` — Bratislava, Belgrade, Zagreb, … Taipei) were missing from `LOCALE_TO_DATAFORSEO_LOCATION`, so their tasks had no `location_code`. The live endpoint tolerates that; **`task_post` rejects it** with per-task 40501 `Invalid Field: 'location_name'` (965 rejects). The rejected task still returns an id, which the client then polled for the full 5-minute window (28,808 × 40401 `Task Not Found` — ~30 polls per reject, an exact match). These pairs are self-poisoning: they never succeed, so they never enter `search_cache`, so they sit at the front of the un-searched frontier every day and hit the fail-fast breaker before any valid pair runs.
2. **The amplification**: `FallbackSearchClient` correctly disables the provider after 3 consecutive `SearchUnavailableError`s, but `_run_full` handled `exhausted` per pair — appending a `search_failed` entry and `continue`-ing — so every remaining pair became a counted "failure", and the catch-up pass repeated the walk with a fresh client. Neither the pair logs nor the `runs` row carried the original error string, so the email could not say *why* search died.

Timeline fingerprint: each nightly run posted exactly 6 rejected tasks at 5-minute intervals (3 strikes × Sweden group + 3 × world group; Hungary was pre-filtered), 01:00→01:30 UTC, then walked the remaining ~4,969 pairs as instant skips.

## Fix (2026-07-23)

- `FallbackSearchClient.failure_reason` retains the original provider error (including the missing-credentials case) and it is woven into the `_raise_exhausted` / `search_all` exception messages (`scraper/search.py`).
- `_run_full` **aborts** at the first pair that needs a live search while the client is exhausted: one marker pair-log entry (`search_failed` + `search_error`), one `search_provider_down_run_aborted` warning, return. Real failures also record `search_error`.
- `run_pipeline` builds the client once, shares it with the catch-up pass, and skips catch-up entirely when the provider died (`catchup_skipped_search_provider_down`).
- `get_daily_summary` lifts the first `search_error` out of the pair logs; `build_report_html` renders it as `· ok: <original error>` in the run row.

A scheduled run still retries per geographic group (Sweden → world → Hungary each build a fresh client via `run_pipeline`), so a dead provider costs at most ~3 real attempts per group, not thousands.

Root-cause fix (same day): all 12 missing locales added to `LOCALE_TO_DATAFORSEO_LOCATION` (codes are 2000 + ISO 3166-1 numeric), a locale-coverage test locks `cities.yaml` to the map, unmapped locales fall back to US (2840) with a warning in standard mode, and a rejected `task_post` fails fast with the API's own status message instead of polling a dead task id for 5 minutes.

## Lessons

Fail-fast state must terminate the loop that consults it — checking it per iteration converts one outage into O(n) noise. An error that is only logged is an error that is lost: the string identifying the root cause must travel with the run record to the surface the operator actually reads (the daily email). A permanently-failing input that is retried daily migrates to the front of the work queue and, combined with a consecutive-failure breaker, starves every valid input behind it — breakers need to distinguish "this provider is down" from "these specific inputs are poison". And the provider's own error dashboard (`app.dataforseo.com` → Errors, with per-request metadata) beat every server-side log for diagnosis.

See [[search-layer]], [[daily-report]], [[dataforseo]], and [[cost-saver-schedule]].
