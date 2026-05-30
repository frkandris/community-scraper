# Playwright vs. Blocked Domain Check Order

*`fetch_and_clean()` checks `playwright_fetcher.matches(url)` BEFORE `_is_blocked()`. A domain in both lists gets Playwright-fetched, not blocked.*

## The rule

If a domain appears in both `playwright_domains` and `blocked_domains`, Playwright wins. The blocked-domain list is only consulted after the Playwright check fails.

## Why this matters

Social-media domains (Twitter, Instagram, Facebook, TikTok, LinkedIn, YouTube, Reddit) are in `blocked_domains` and should never be in `playwright_domains`. If you accidentally add `twitter.com` to `playwright_domains`, the block is bypassed and the scraper attempts to Playwright-fetch Twitter — which wastes time and triggers rate limits.

## Current state

`playwright_domains` in `settings.yaml` is intentionally empty. Playwright is used for Google Search (via `GooglePlaywrightSearchClient`) but NOT for page fetching.

## Related

- [[blocked-domains]]
- [[search-provider-fallback-chain]]
