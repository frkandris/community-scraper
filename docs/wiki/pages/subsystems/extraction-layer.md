---
type: Subsystem
title: Extraction Layer
description: DeepSeek → Groq LLM extraction of communities, venues, and persons from page text, with four prompt families, live-editable prompts, and fingerprint-keyed caching.
tags: [extraction, llm, deepseek, groq, prompts, fingerprint]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# Extraction Layer

*`FallbackExtractor` (DeepSeek → Groq) turns clean page text into `CommunityRecord` / `VenueRecord` / `PersonRecord` objects via four separate prompt+schema families.*

See [[extraction-provider-fallback-chain]], [[extraction-fingerprint-cache]], [[community-record]], [[joinable-quality-gate]].

## Providers

`_ApiExtractor` is the shared OpenAI-compatible base. `DeepSeekExtractor` (`api.deepseek.com/v1`, `deepseek-chat`) is primary; `GroqExtractor` (`api.groq.com/openai/v1`, `llama-3.3-70b-versatile`) is fallback. Per-provider state: `_exhausted[i]` (permanent, set on HTTP 402 `ExtractorQuotaError`) and `_blocked_until[i]` (temporary, set on HTTP 429 `ExtractorRateLimitError`, respects `Retry-After`, default 60 s). State is per-instance; a new `FallbackExtractor` is built each run, so exhaustion resets.

**Only 402/429 trigger the fallback.** Network errors and any other HTTP ≥ 400 are logged and return `{}` — the page is silently treated as "no communities." A 500 from DeepSeek does **not** fall through to Groq. See [[non-quota-errors-drop-page]].

## Effective config beats class defaults

Runtime config (settings.yaml) overrides class defaults: Groq's effective truncation is **3000 chars** (class default 4000) and rate limit **7.0 s** (class default 4.0). The YAML wins — a latent surprise if settings are trimmed. Community extraction uses `temperature` (0.1); venue/person/enrich hard-code `temperature 0.0` (only community extraction is stochastic).

## Four prompt+schema families

`SYSTEM_PROMPT`+`EXTRACTION_SCHEMA` (community), `VENUE_SYSTEM_PROMPT`+`VENUE_SCHEMA`, `PERSON_SYSTEM_PROMPT`+`PERSON_SCHEMA`, `ENRICH_SYSTEM_PROMPT`+`ENRICH_SCHEMA`. **The schema dicts are documentation only** — the API request uses `response_format: {"type": "json_object"}` plus a hand-written JSON-shape suffix string (`_API_EXTRACT_SUFFIX`), not JSON-schema grammar. The `SYSTEM_PROMPT` repeatedly instructs "output field values in the **original language of the page**" (see [[llm-prompt-language-bias]]).

## Live-editable prompts

`get_prompt(key)` returns `_PROMPT_OVERRIDES.get(key) or PROMPT_KEYS[key]()`. The admin `/admin/prompts` page writes overrides to the DB; every extractor method calls `get_prompt()` at call time, so edits take effect without restart. **Trap: `get_prompt` uses `or`**, so an override set to `""` silently reverts to the default — you cannot blank a prompt. `enrich_user` has no override key (hard-coded inline). See [[get-prompt-empty-override-falls-back]].

## False-positive injection

Negative examples are appended to the system message **at call time, after** `get_prompt(...)`: `get_prompt("extraction_system") + false_positive_examples + _API_EXTRACT_SUFFIX`. Because they land after the fingerprinted prompt, **adding/removing false positives does not invalidate the extraction cache.** See [[false-positive-injection]].

## Person extraction skip

Persons are extracted only when `community_names` is non-empty for a page (people are meaningful only relative to a known community). Zero communities → no person LLM call, saving a request per page (most pages yield 0 communities). A second cache layer skips the call when persons were already extracted at the current fingerprint.

## Enrichment

A record with no website/social/contact and `confidence ≥ 0.7` gets a second pass: search `"name" city`, fetch top results, and `enrich()` fills **only empty fields** via `model_copy(update=…)`. Enrichment happens only in `_run_full`, not `ai_only`.

## Fingerprints

Each extractor exposes `model_fingerprint` / `venue_fingerprint` / `person_fingerprint` (SHA-256[:12] of `prompt + model`) plus `canonical_fingerprint`. The canonical one always uses `primaries[0]` so pages extracted by the *fallback* provider still store under the primary's key — see [[canonical-fingerprint-provider-shift]] and [[extraction-fingerprints]].
