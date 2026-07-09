---
type: Hack
title: Non-402/429 Extractor Errors Silently Drop the Page
description: Any non-402/429 extractor error returns {} and the page is silently treated as having no communities — and since the 2026-07 cleanup there is no fallback provider at all.
tags: [extraction, errors, fallback, gotcha]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# Non-402/429 Extractor Errors Silently Drop the Page

*In `_ApiExtractor._post`: network exceptions and any HTTP ≥ 400 other than 402/429 are logged and return `{}`. Only 402 (`ExtractorQuotaError`) and 429 (`ExtractorRateLimitError`) engage the fallback chain.*

Consequence (sharper since Groq was removed in the 2026-07 cleanup): a **500/503 from DeepSeek has nowhere to fall through** — the page is silently treated as "no communities found" and dropped. Transient primary-provider outages therefore cause silent data loss for that page (it will be retried next run only if its fingerprint still marks it not-done). See [[extraction-layer]] and [[extraction-provider-fallback-chain]].

Related edge case: `Retry-After` is parsed with `float(...)`; an HTTP-date-style header (not seconds) would raise inside `_post` outside the caught path and propagate as a generic error rather than a clean rate-limit.
