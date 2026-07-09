---
type: Hack
title: Non-402/429 Extractor Errors Silently Drop the Page
description: Only quota (402) and rate-limit (429) errors trigger the DeepSeek→Groq fallback; any other error returns {} and the page is treated as having no communities.
tags: [extraction, errors, fallback, gotcha]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# Non-402/429 Extractor Errors Silently Drop the Page

*In `_ApiExtractor._post`: network exceptions and any HTTP ≥ 400 other than 402/429 are logged and return `{}`. Only 402 (`ExtractorQuotaError`) and 429 (`ExtractorRateLimitError`) engage the fallback chain.*

Consequence: a **500 or 503 from DeepSeek does not fall through to Groq** — the page is silently treated as "no communities found" and dropped. Transient primary-provider outages therefore cause silent data loss for that page (it will be retried next run only if its fingerprint still marks it not-done). See [[extraction-layer]] and [[extraction-provider-fallback-chain]].

Related edge case: `Retry-After` is parsed with `float(...)`; an HTTP-date-style header (not seconds) would raise inside `_post` outside the caught path and propagate as a generic error rather than a clean rate-limit.
