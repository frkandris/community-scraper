---
type: Concept
title: Search Provider Fallback Chain
description: DataForSEO is the sole search provider (2026-07 cleanup); FallbackSearchClient remains as a single-provider wrapper with per-run exhaustion.
tags: [search, fallback, providers, quota]
timestamp: 2026-07-10
resource: scraper/search.py
---

# Search Provider Fallback Chain

*The current chain contains one provider: DataForSEO in live or standard queue mode.*

`FallbackSearchClient` still accepts a provider list and tracks permanent quota exhaustion for the run. With one provider, HTTP 402/429 or API quota status makes subsequent pairs fail fast. `SearchUnavailableError` is transient for the current call; the pipeline must not write an empty `search_cache` entry for either failure class. Legitimate zero-result searches are cached. See [[search-layer]] and [[searchquotaerror-reraise-ordering]].

## Historical chain

Google Playwright search, Serper, and the experimental DuckDuckGo client were removed in 2026-07: browser search was blocked on datacenter and residential automation, and the secondary paid client was unused. The removed residential worker is documented in [[local-search-worker]]. Root `PROJECT.md` still describes an even older Serper/Brave/SearXNG design; see [[doc-drift-project-readme]].

The wrapper remains because it centralizes exhaustion, already-collected-result retention, and query failover if another provider is added later. Related LLM architecture: [[extraction-provider-fallback-chain]].
