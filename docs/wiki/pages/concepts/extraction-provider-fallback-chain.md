---
type: Concept
title: Extraction Provider Fallback Chain
description: DeepSeek is the sole extractor (2026-07 cleanup); FallbackExtractor remains as a single-provider wrapper.
tags: [llm, extraction, fallback, providers]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# Extraction Provider Fallback Chain

*DeepSeek → Groq. Same pattern as search providers: permanent exhaustion or temporary rate-limiting.*

## Providers in order

1. **DeepSeekExtractor** — `deepseek-chat` model. Primary. Requires `DEEPSEEK_API_KEY`.
2. **GroqExtractor** — `llama-3.3-70b-versatile` model. Fallback. Requires `GROQ_API_KEY`.

## Fingerprint note

The **extraction fingerprint** is computed from `system_prompt + model_name`. DeepSeek and Groq have different model names, so pages extracted by each have different fingerprints. If DeepSeek is exhausted mid-run and Groq takes over, subsequent pages have a different fingerprint than earlier ones in the same run.

## Rate limits

DeepSeek: 1.0 s/request  
Groq: 7.0 s/request (much slower — Groq has strict rate limits)

`max_text_chars`:  
DeepSeek: 8000 chars (handles longer pages)  
Groq: 3000 chars (truncates long pages)

## Quota exhaustion vs. rate limiting

`ExtractorQuotaError` → permanent skip for this provider for this run  
`ExtractorRateLimitError` → temporary block, retried after `wait_seconds`

## Related

- [[extraction-fingerprint-cache]]
- [[search-provider-fallback-chain]]


## 2026-07-09: provider cleanup

`GroqExtractor` was **removed** (unused fallback). **DeepSeek is the only extractor.** `FallbackExtractor` and the canonical-fingerprint machinery remain — with a single primary, `canonical_* == model_fingerprint` trivially, but the structure allows re-adding a fallback with one primaries.append. Consequence: a DeepSeek outage now means extraction simply fails for that page (no fallback) — see [[non-quota-errors-drop-page]].
