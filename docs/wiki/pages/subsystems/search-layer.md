---
type: Subsystem
title: Search Layer
description: DataForSEO is the sole search client (live or standard mode) behind the FallbackSearchClient wrapper with per-run exhaustion state.
tags: [search, dataforseo, fallback, cost]
timestamp: 2026-07-23
resource: scraper/search.py
---

# Search Layer

*Since the 2026-07 provider cleanup: **DataForSEO is the only search client**, behind the `FallbackSearchClient` wrapper (per-run exhaustion state, single provider).*

See [[search-provider-fallback-chain]] for history (Google Playwright / Serper / DuckDuckGo removal rationale), [[fetch-layer]] for what happens to the URLs next, and [[cost-optimization-2026-07]] for the cost levers.

## DataForSEOClient

- **live mode** (default): `live/regular` endpoint, $2/1K queries, instant.
- **standard mode** (`search.dataforseo_mode: standard`): `task_post` + `task_get` polling. Production uses high priority (`standard_priority: 2`, ~$1.2/1K, normally ≤1 minute) because normal priority can exceed the 5-minute poll timeout. Enrichment always builds a live-mode client.
- Locale → `location_code` via `LOCALE_TO_DATAFORSEO_LOCATION` (HU=2348); unmapped locales omit the code. `str(locale)` guards the [[pyyaml-no-norway-boolean]] trap.
- `SearchQuotaError` on HTTP 402/429 and API status 40201 (top-level, per-task, and in standard-mode polling).

## FallbackSearchClient

`search_all(queries, stop_after=…)` iterates queries one-by-one, deduplicates by
exact URL, and **stops issuing paid queries** once `stop_after` unique results are
collected (the pipeline passes `search_max_pages * 2`). A quota error immediately
marks the provider exhausted. Transient unavailability is tolerated twice and
reset by a successful request; the third consecutive failure disables the provider
for the rest of the geographic pass so a stuck queue cannot consume the whole
collector window. The fail-fast error remains `SearchUnavailableError`, rather
than being mislabeled as quota exhaustion. Already-collected results are kept and
the next day constructs a fresh client.

Since 2026-07-23 the client keeps the **original provider error** in
`failure_reason` (set on quota block, on third-strike disable, and at construction
when credentials are missing) and includes it in the fail-fast exception messages.
The pipeline consumes it: `_run_full` aborts the pair loop when the client is
exhausted (one `search_error`-carrying marker entry instead of one failure per
remaining pair), and `run_pipeline` shares one client between the main and
catch-up passes, skipping catch-up when the provider died. See
[[2026-07-search-provider-down-noise]].

Since 2026-07-31 both `search()` and `search_all()` also catch bare `Exception`
and route it through `_record_unavailable` as a transient failure. The
DataForSEO parsers assume the documented response shape (`.get()` on tasks,
results and items), so an unexpected payload raised an untyped error that
escaped the chain and aborted the whole run — the search-side twin of
[[2026-07-llm-bare-array-run-abort]]. Crucially it is *not* converted to an
empty result: an empty search is cached, a failure must not be.

## Query construction

`build_queries(city, variants, terms)` emits at most **3** queries (`terms[:2]` × primary variant + `terms[0]` × second variant). With the short-circuit, fruitful pairs typically pay for 1.
