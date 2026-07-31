---
type: Hack
title: Extractor Circuit Breaker and Preflight
description: A dead LLM provider now fails a run in seconds — one live preflight extraction before the pair loops, plus a breaker that opens after 20 consecutive failed calls.
tags: [extraction, failure-handling, circuit-breaker, preflight, run-abort]
timestamp: 2026-07-25
resource: scraper/extract.py
---

# Extractor Circuit Breaker and Preflight

*Two guards added after a retired model name burned a whole extraction window: a live
check before any work starts, and a counter that stops a run once the provider is
clearly dead.*

## The failure this prevents

[[2026-07-deepseek-model-retired]]: `deepseek-chat` was retired, every call 400'd, and
`_run_ai_only` dutifully walked 1368 pages logging one `extract_failed` each. Degradation
was *correct* — nothing was cached, everything was retried later — but nothing was
*loud*, and the 16:35→00:20 UTC off-peak window produced 5 records.

The search side already had this covered ([[2026-07-search-provider-down-noise]]); the
extractor side did not. Only HTTP 402 (quota) could exhaust a provider.

## Preflight

`run_pipeline()` calls `extractor.preflight()` after the done-pair pre-filter and before
any pair loop: one `extract()` against a short synthetic listing
(`FallbackExtractor._PREFLIGHT_TEXT`). An empty result passes — only an exception fails.
On failure the run raises `ExtractorUnavailableError("extractor preflight failed, no work
attempted: …")`, which `main.py` and the admin run route persist as the run's `error`,
so the daily email names the cause ([[daily-report]]).

- `search_only` skips it — that mode never calls the LLM.
- No provider configured → no-op (a deliberate no-key run must still search and fetch).
- Cost: one small call per `run_pipeline()`; a bounded saver run makes three (one per
  city group, see [[hungary-sweden-intl-three-passes]]).

It catches more than a bad model name: a revoked key, a changed error contract, or a
response the parser can no longer read all surface here instead of page by page.

## Circuit breaker

`FallbackExtractor` counts *consecutive* failed `_call()`s. One success resets the
counter, so scattered transient errors never trip it. At `_FAILURE_THRESHOLD` (20) every
provider is marked exhausted and `failure_reason` records the last error plus the count.

Since 2026-07-31 `_call` also catches bare `Exception` — an untyped bug inside a
provider method (a parser `AttributeError`, an unanticipated response shape) counts as a
transient failure instead of unwinding the whole run. `CancelledError` is a
`BaseException`, so stop/cancel is unaffected ([[asyncio-task-cancellation]]). Before
that net existed, one bad page aborted an entire off-peak window:
[[2026-07-llm-bare-array-run-abort]].

## exhausted vs providers_down

`exhausted` is also true when **no** provider is configured, which is a legitimate
no-LLM setup — aborting on it would break search-and-fetch-only runs. The new
`providers_down` property (`primaries and all(_exhausted)`) is the abort signal:

- `_run_full` / `_run_ai_only`: on `providers_down` the pair log gets `extract_error`,
  the current pair is saved, and the run stops instead of walking the rest.
- The reason reaches the run-detail page (red banner) and the daily email
  (`· ok: <error>`), the same path `search_error` takes.

Venue and person extraction failures now also increment `extract_failed`, so a run that
silently lost every venue can no longer report a clean ✓.

## Gotchas

- The breaker lives in the extractor, not the pipeline, so *every* call site benefits —
  including `scrape_submitted_url` and the admin re-extract flow.
- Test stubs that duck-type an extractor need a `providers_down` attribute
  (`tests/test_false_positive_cache.py`).
- Threshold 20 is a guess tuned to "obviously dead, but survive a bad minute". Lower it
  and a flaky API aborts real work; raise it and the breaker stops earning its keep.
