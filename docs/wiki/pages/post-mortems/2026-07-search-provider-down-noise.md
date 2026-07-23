---
type: Post-mortem
title: Dead Search Provider Produced 4972 Per-Pair Failures From 3 Real Errors
description: When DataForSEO died, the collector walked every remaining pair logging one failure each and the daily email lost the original provider error, making the outage undiagnosable from the report.
tags: [post-mortem, search, dataforseo, saver, daily-report, observability]
timestamp: 2026-07-23
resource: scraper/pipeline.py
---

# Dead Search Provider Produced 4972 Per-Pair Failures From 3 Real Errors

*A provider outage was amplified into thousands of bookkeeping "failures" while the one string that identified the outage was dropped before it reached the daily email.*

## Symptom

The 2026-07-22 daily report showed `❌ search_only · 4972 város–téma páros · 0 rekord — hibák: 4972 keresés`. Production logs showed the same pattern: `processing_pair` / `search_unavailable_pair_skipped` alternating for every remaining international pair, twice (main pass 2486 + catch-up pass 2486).

## Root cause

`FallbackSearchClient` correctly disables DataForSEO after 3 consecutive `SearchUnavailableError`s (or 1 quota error). But `_run_full` handled the resulting `exhausted` state per pair: it appended a `search_failed` pair-log entry and `continue`d, so every remaining pair became a counted "failure". The catch-up pass then built a **fresh** client and repeated the walk. Neither the pair logs nor the `runs` row carried the original provider error string — it existed only in the (rotated) container log — so the email could not say *why* search died.

## Fix (2026-07-23)

- `FallbackSearchClient.failure_reason` retains the original provider error (including the missing-credentials case) and it is woven into the `_raise_exhausted` / `search_all` exception messages (`scraper/search.py`).
- `_run_full` **aborts** at the first pair that needs a live search while the client is exhausted: one marker pair-log entry (`search_failed` + `search_error`), one `search_provider_down_run_aborted` warning, return. Real failures also record `search_error`.
- `run_pipeline` builds the client once, shares it with the catch-up pass, and skips catch-up entirely when the provider died (`catchup_skipped_search_provider_down`).
- `get_daily_summary` lifts the first `search_error` out of the pair logs; `build_report_html` renders it as `· ok: <original error>` in the run row.

A scheduled run still retries per geographic group (Sweden → world → Hungary each build a fresh client via `run_pipeline`), so a dead provider costs at most ~3 real attempts per group, not thousands.

## Lessons

Fail-fast state must terminate the loop that consults it — checking it per iteration converts one outage into O(n) noise. And an error that is only logged is an error that is lost: the string identifying the root cause must travel with the run record all the way to the surface the operator actually reads (here: the daily email).

See [[search-layer]], [[daily-report]], and [[cost-saver-schedule]].
