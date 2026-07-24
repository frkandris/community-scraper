---
type: Integration
title: DeepSeek
description: The sole LLM extractor — OpenAI-compatible chat API with a 50–75% off-peak discount window (UTC 16:30–00:30) that the extract cron is boxed into.
tags: [integration, llm, deepseek, extraction, off-peak, cost]
timestamp: 2026-07-24
resource: scraper/extract.py
---

# DeepSeek

*An OpenAI-compatible chat-completions API that does all community/venue/person
extraction. Pricing is time-of-day dependent, which shapes the whole schedule.*

## Contract

- **Auth**: `DEEPSEEK_API_KEY` env var; `DeepSeekExtractor` (on the `_ApiExtractor`
  base) in `scraper/extract.py`.
- **Model + prompts**: four prompt families (community, venue, person, enrich),
  all live-editable from `/admin/prompts`. Any prompt or model change rotates the
  extraction fingerprint and stales the corresponding cache
  ([[extraction-fingerprint-cache]]).
- **Off-peak discount**: UTC 16:30–00:30, roughly 50–75% cheaper depending on model.
  The extractor cron (16:35→00:20 UTC, `ai_only` mode, `stop_at`-boxed) lives entirely
  inside this window — see [[cost-saver-schedule]].

## Quirks and hard-won rules

- **Typed failures, never empty results.** `_post` raises on network errors and
  non-402/429 HTTP errors; quota → `ExtractorQuotaError`, rate limit →
  `ExtractorRateLimitError` (waits ≤5 min honoring a guarded Retry-After parse),
  transient → one retry then `ExtractorUnavailableError`. The pipeline skips caching
  on failure so the page is retried next run — caching `[]` under the current
  fingerprint would be permanent silent data loss ([[non-quota-errors-drop-page]]).
- **JSON tail bleed**: the model sometimes appends following JSON fields into the
  `name` string; `_LEAKED_JSON_RE` strips it ([[name-json-tail-bleed]]).
- **Language bias**: non-English example strings in the system prompt make the model
  answer in that language for every city ([[llm-prompt-language-bias]]).
- The `joinable` flag it emits is the primary quality gate
  ([[joinable-quality-gate]]).

## Where it runs

Only in `ai_only` and `full` runs; `search_only` collector runs make zero LLM calls.
The search side is [[dataforseo]].
