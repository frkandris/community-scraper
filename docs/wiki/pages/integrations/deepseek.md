---
type: Integration
title: DeepSeek
description: The sole LLM extractor — OpenAI-compatible chat API; the 2026-07 peak-valley pricing (2× at UTC 01–04 and 06–10) and the v4 model rename shape the extract schedule and the fingerprint_model cache pin.
tags: [integration, llm, deepseek, extraction, off-peak, cost]
timestamp: 2026-07-25
resource: scraper/extract.py
---

# DeepSeek

*An OpenAI-compatible chat-completions API that does all community/venue/person
extraction. Pricing is time-of-day dependent, which shapes the whole schedule.*

## Contract

- **Auth**: `DEEPSEEK_API_KEY` env var; `DeepSeekExtractor` (on the `_ApiExtractor`
  base) in `scraper/extract.py`.
- **Model names (2026-07)**: DeepSeek retired `deepseek-chat`; supported names are
  `deepseek-v4-pro` and `deepseek-v4-flash`. Production runs `deepseek-v4-flash`
  with `fingerprint_model: deepseek-chat` pinning the cache identity — see
  [[2026-07-deepseek-model-retired]].
- **Model + prompts**: four prompt families (community, venue, person, enrich),
  all live-editable from `/admin/prompts`. Any prompt change rotates the extraction
  fingerprint and stales the corresponding cache ([[extraction-fingerprint-cache]]);
  a model change does too **unless** `deepseek.fingerprint_model` pins the old name.
- **Peak-valley pricing (2026-07)**: peak hours UTC 01:00–04:00 and 06:00–10:00 cost
  2× on all billing items (replacing the old flat off-peak discount). The extractor
  cron (16:35→00:20 UTC, `ai_only` mode, `stop_at`-boxed) sits entirely outside the
  peak windows, so the schedule needed no change — see [[cost-saver-schedule]].

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
