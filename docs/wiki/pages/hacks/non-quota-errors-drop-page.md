---
type: Hack
title: Extractor Errors No Longer Cache Empty Results (fixed)
description: FIXED 2026-07-09 — transient/quota extractor failures now raise ExtractorUnavailableError; the pipeline skips caching so the page is retried next run.
tags: [extraction, errors, fallback, gotcha]
timestamp: 2026-07-24
resource: scraper/extract.py
---

# Extractor Errors No Longer Cache Empty Results (fixed)

**Historical bug:** `_ApiExtractor._post` returned `{}` on network errors and non-402/429 HTTP failures, so a transient DeepSeek 500 was parsed as "0 communities" and **cached under the current fingerprint — permanent silent data loss** (the page was never retried; broad excepts even swallowed rate-limit errors into empty persons/venues caches).

**Current model:**
- `_post` raises **`ExtractorUnavailableError`** on network errors / non-402/429 HTTP failures.
- `FallbackExtractor._call` (shared failover runner): quota → provider exhausted for the run; rate-limit → waits out the shortest window (max 5 min) and retries instead of failing the page; transient → one immediate retry; otherwise raises `ExtractorUnavailableError`.
- The pipeline catches it per-page and **skips `save_extracted`** — the raw text stays cached, the page is retried next run. `extract_failed` counts land in the pair log; `run_completed_with_failures` summarizes at run end.
- `FallbackExtractor.exhausted` lets the pipeline fail fast for the rest of a run after a 402.

See [[extraction-layer]] and [[extraction-provider-fallback-chain]].

Related edge case: `Retry-After` is parsed with `float(...)`; an HTTP-date-style header (not seconds) would raise inside `_post` outside the caught path and propagate as a generic error rather than a clean rate-limit.

**2026-07-24 follow-up:** the same doctrine now covers *malformed LLM output*: `_parse_communities` / `_parse_venues` / `_parse_persons` raise `ExtractorUnavailableError` on invalid JSON (or a non-list payload) instead of returning `[]` — a truncated DeepSeek response used to be cached as a successful empty extraction under the current fingerprint, permanently suppressing retries.
