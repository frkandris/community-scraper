---
type: Hack
title: SearchQuotaError Must Stay Distinct from Transient Failure
description: DataForSEO and its wrapper preserve quota versus transient errors so the pipeline never caches provider failure as a legitimate empty search.
tags: [search, exceptions, failover, cache]
timestamp: 2026-07-10
resource: scraper/search.py
---

# SearchQuotaError Must Stay Distinct from Transient Failure

*Quota exhaustion, transient provider failure, and a successful zero-result search have different retry/cache semantics.*

- `SearchQuotaError`: HTTP 402/429 or DataForSEO quota status; blocks that provider for the rest of the run.
- `SearchUnavailableError`: network error, other HTTP/API failure, bad JSON, or standard-queue timeout; abandons the provider for this call.
- `[]`: a search actually ran and found nothing; this is a valid result and is cached so it is not repaid every run.

`FallbackSearchClient.search_all` preserves already-collected results, raises a typed error when nothing was successfully searched, and only returns an empty list after legitimate attempts. `_run_full` catches the typed errors before `save_search_cache`. The removed Serper/Google clients once required an explicit `except SearchQuotaError: raise` before a broad handler; the current DataForSEO client avoids that trap by raising typed failures directly. See [[search-provider-fallback-chain]], [[search-layer]], and [[cost-saver-schedule]].
