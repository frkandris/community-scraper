---
type: Subsystem
title: Search Layer
description: DataForSEO is the sole search client (live or standard mode) behind the FallbackSearchClient wrapper with per-run exhaustion state.
tags: [search, dataforseo, fallback, cost]
timestamp: 2026-07-21
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

`search_all(queries, stop_after=…)` iterates queries one-by-one, deduplicates by exact URL, and **stops issuing paid queries** once `stop_after` unique results are collected (the pipeline passes `search_max_pages * 2`). A quota or persistent unavailable error marks the provider exhausted (`float('inf')`) for the rest of the geographic pass; already-collected results are kept. The next pass/day constructs a fresh client. With one provider the chain semantics are trivial, but the wrapper stays so a fallback can be re-added with one line.

## Query construction

`build_queries(city, variants, terms)` emits at most **3** queries (`terms[:2]` × primary variant + `terms[0]` × second variant). With the short-circuit, fruitful pairs typically pay for 1.
