---
type: Post-mortem
title: LLM Bare Array Aborted A Whole Run
description: DeepSeek answered one page with a top-level JSON array, `.get()` hit a list, and the untyped AttributeError escaped the extractor chain and killed the 2026-07-30 ai_only window with 0 pairs processed.
tags: [deepseek, incident, json-parsing, error-handling, ai-only]
timestamp: 2026-07-31
resource: scraper/extract.py
---

# LLM Bare Array Aborted A Whole Run

*One malformed model response shaped like a list instead of an object took down an entire off-peak extraction window, because only typed extractor errors were ever caught.*

## Symptom

The 2026-07-30 daily report showed the `ai_only` run (16:35 UTC) as:

> ❌ ai_only · 16:35 UTC · **0 város–téma páros · 0 rekord** — futási hiba:
> `'list' object has no attribute 'get'`

Zero pairs, zero records, no `extract_failed` counter — a whole off-peak
discount window produced nothing. The `search_only` collector the same day was
healthy (1413 pairs, 6016 pages fetched), so the pages were waiting; nothing
consumed them.

## Root cause

`_parse_communities()` did `json.loads(raw).get("communities", [])`. The
`try` around it caught `json.JSONDecodeError` only. DeepSeek is called with
`response_format: {"type": "json_object"}`, but that constrains the response to
*valid JSON*, not to an object — a model occasionally answers with a bare array
(`[{"name": …}]`). `.get()` on a list raises `AttributeError`, which is not one
of the typed extractor errors:

- `FallbackExtractor._call` catches `ExtractorQuotaError`,
  `ExtractorRateLimitError` and `ExtractorUnavailableError` — nothing else.
- `_run_ai_only` catches `ExtractorUnavailableError` per page.
- So the `AttributeError` travelled up through `run_pipeline()` into
  `_cron_run`'s generic handler (`scraper/main.py:225`), which recorded it as
  the run error and stopped everything.

The pair log is only appended after a pair finishes (`pipeline.py:1001`), which
is why the report showed **0 pairs**: the crash happened inside the very first
pair, discarding the pairs that had already been walked in that group.

`extract_venues` / `extract_persons` / `enrich` each wrap their call in
`except Exception` and were therefore immune — only the community path, the one
without a net, was exposed.

## Fix (2026-07-31)

Two layers, because either alone leaves a gap:

1. **Shape check at the parser** — `_json_items(raw, key, kind, source_url)`
   in `extract.py` is now the single entry point for all three parsers. A
   non-`dict` top level raises `ExtractorUnavailableError`, so the page is
   retried and never cached as empty ([[non-quota-errors-drop-page]]).
   `_apply_enrich` returns the record unchanged on a non-dict payload.
   The same helper closes a **pre-existing** silent-loss hole found while
   reviewing the fix: `.get(key, [])` read a renamed wrapper
   (`{"data": [...]}`) as zero results, which the caller then cached as an
   empty page forever. A populated object without the expected key is now an
   error; a bare `{}` still counts as a legitimate empty extraction.
2. **Last-resort net in the chain** — `FallbackExtractor._call` catches bare
   `Exception` and converts it to a transient failure: retried once, counted by
   `_note_failure`, and still able to open the circuit breaker after 20
   consecutive occurrences ([[extractor-circuit-breaker]]). `CancelledError` is
   a `BaseException`, so stop/cancel still works
   ([[asyncio-task-cancellation]]).

3. **The same net on the search side** — the review round that followed the fix
   found the exact twin still open: `FallbackSearchClient.search` /
   `search_all` caught only `SearchQuotaError` / `SearchUnavailableError`, while
   the DataForSEO parsers assume the documented object shape all the way down
   (`search.py:132`, `:192`, `:224`). A bare array or null task list would have
   killed a collector window the same way. Both methods now catch bare
   `Exception` as a transient, uncached failure that counts toward
   `_record_unavailable` ([[2026-07-search-provider-down-noise]]).

Regressions: `tests/test_deferred_fixes.py::test_bare_array_response_raises_instead_of_attribute_error`
(all three parsers × array/string/number/null) and
`tests/test_error_handling.py::test_unexpected_error_becomes_unavailable_not_a_run_abort`
plus the breaker variant, and
`tests/test_error_handling.py::test_untyped_search_error_does_not_escape_the_chain`
for the search side.

## Lessons

- A typed-error contract is only as strong as its fallback. Every `except`
  list in the extraction chain enumerated the *expected* failures; the first
  unexpected one cost a full window. The chain now degrades one page at a time
  by default, and only escalates deliberately.
- `response_format: json_object` is a provider hint, not a schema guarantee —
  validate the shape, not just the syntax. Same class of surprise as
  [[2026-07-deepseek-model-retired]]: the provider changed what came back, and
  the code assumed.
- "0 pairs" in the daily report means the crash preceded the first pair
  *completion*, not that no work was attempted — pair logs are written per
  finished pair. Read it as "aborted early", then look for the run error text.
