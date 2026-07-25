---
type: Post-mortem
title: DeepSeek Model Retired
description: DeepSeek dropped the deepseek-chat model name and the whole 2026-07-24 ai_only window failed with 1368 uncached pages; the fix swaps to deepseek-v4-flash while fingerprint_model pins the cache identity so 74K cached extractions survive.
tags: [deepseek, incident, model-rename, fingerprint, cache]
timestamp: 2026-07-25
resource: scraper/extract.py
---

# DeepSeek Model Retired

*A provider-side model rename silently zeroed a full night of extraction; the durable fix separates the wire model name from the cache-identity name.*

## Symptom

The 2026-07-24 daily report showed the `ai_only` run (16:35 UTC) finishing with
❌: 2578 pairs walked, **1368 pages extract_failed, 5 records, 0 AI-processed
pages**. DeepSeek balance was healthy ($44.28), so it wasn't quota.

## Root cause

Container logs (Coolify → Logs, filter `api_request_failed`) showed 2736
identical rejections:

> `status=400 · "The supported API model names are deepseek-v4-pro or
> deepseek-v4-flash, but you passed deepseek-chat."`

DeepSeek retired the `deepseek-chat` model name (announced alongside their
mid-July peak-valley pricing change). `settings.yaml` still said
`model: deepseek-chat`, so every LLM call 400'd for the whole window. The
system degraded correctly: typed failures were **not** cached
([[non-quota-errors-drop-page]]) and all pages queued for retry.

## Why the fix wasn't a one-liner

`extract_fingerprint = SHA-256[:12](prompt + model_name)` — renaming the model
in config would rotate all four fingerprints and stale the entire **74K-page**
extraction cache, triggering weeks of re-extraction the low-cost indexing
strategy doesn't want.

## Fix (2026-07-25)

- `deepseek.fingerprint_model` setting: fingerprints hash this name when set,
  the wire request uses `model`. Production: `model: deepseek-v4-flash` +
  `fingerprint_model: deepseek-chat` → cache stays valid.
- All four `DeepSeekExtractor` construction sites pass the pin; the done-pair
  pre-filter derives its fingerprint from `primaries[0].fingerprint_model`.
- **Volume gotcha**: `/app/config` is a persisted volume, so repo edits to
  `settings.yaml` never reach production — the prod file was edited in the
  Coolify container terminal *before* pushing, so the deploy restart picked up
  code + config together ([[deployment-coolify]]).

## Lessons

- A provider can invalidate a hardcoded model name overnight; the failure mode
  (100% typed errors, healthy balance) looks like an outage until the response
  body is read. The body was only visible via `api_request_failed` log lines.
- Cache identity and wire identity are different concerns; hashing the wire
  model name into the fingerprint coupled them ([[extraction-fingerprints]]).
- Trade-off accepted: v4-flash extractions now write under the deepseek-chat
  fingerprint, so the cache mixes two model generations. Remove the pin if a
  full re-extraction is ever intended.

## Follow-up (2026-07-25)

The rename fix restored extraction but not visibility, so the detection gap was closed
separately: `run_pipeline()` now runs a live `preflight()` extraction before any pair
loop, and `FallbackExtractor` opens a circuit breaker after 20 consecutive failures. The
same incident today would fail the run in seconds with the provider's own error text in
the daily email. See [[extractor-circuit-breaker]].
