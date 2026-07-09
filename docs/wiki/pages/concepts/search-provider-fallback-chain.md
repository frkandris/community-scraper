---
type: Concept
title: Search Provider Fallback Chain
description: DataForSEO is the sole search provider (2026-07 cleanup); FallbackSearchClient remains as a single-provider wrapper with per-run exhaustion.
tags: [search, fallback, providers, captcha]
timestamp: 2026-07-09
resource: scraper/search.py
---

# Search Provider Fallback Chain

*Google Playwright → DataForSEO → Serper. Each provider can be permanently exhausted or temporarily rate-limited within a run.*

## Providers in order

1. **GooglePlaywrightSearchClient** — headless Chromium scraping Google directly. No API key. 8-second delay between requests. CAPTCHA detection raises `SearchQuotaError`, which permanently exhausts this provider for the run.

2. **DataForSEOClient** — paid API. Requires `DATAFORSEO_LOGIN` + `DATAFORSEO_PASSWORD` env vars. Not available if credentials are missing.

3. **SerperSearchClient** — paid API. Requires `SERPER_DEV_API_KEY`. Not available if key is missing.

## State management

`FallbackSearchClient` tracks `_exhausted` (bool, permanent for this run) and `_blocked_until` (float timestamp, temporary rate limit) per provider. On each search request, it iterates providers and skips unavailable ones.

`SearchQuotaError` → permanent exhaustion  
Rate limit error → temporary block until `wait_seconds` has passed

## Implications

- If Google Playwright hits a CAPTCHA, it falls back to DataForSEO for the rest of the run — even if CAPTCHA is later gone
- Running without API keys means Playwright is the only option; CAPTCHA = no search for that run
- Playwright is also used for Google Search specifically, NOT for page fetching (see [[playwright-vs-blocked-domain-ordering]])

## Related

- [[playwright-vs-blocked-domain-ordering]]
- [[extraction-provider-fallback-chain]]


## 2026-07-09: provider cleanup

Google Playwright search, Serper, and the experimental DuckDuckGo client were **removed** — unused in practice (the Playwright client always CAPTCHA'd on the datacenter IP, Serper had no credits in use). **DataForSEO is the only provider** (live or standard mode). `FallbackSearchClient` remains as a thin wrapper for exhaustion state and easy re-addition. The historical chain below is kept for context.
