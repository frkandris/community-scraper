---
type: PostMortem
title: Cerebras Free Tier Ended
description: Cerebras closed its free API tier on 2026-08-17; every call since answered HTTP 402, and because a 402 only retired the provider for one run the worker handed it 283 first-choice picks in a day.
tags: [providers, quota, router, billing, post-mortem, cerebras]
timestamp: 2026-08-23
resource: scraper/router.py
---

# Cerebras Free Tier Ended

*A provider can stop being free without anything in our code changing, and the
router had no way to notice.*

## What happened

The 2026-08-22 daily report showed **cerebras: 283 calls, 283 errors** — a
100% failure rate — while the fleet processed 78 pages against the previous
day's 387. `GET /v1/quota` disagreed with reality: `used: 477`,
`remaining: 473`, `blocked: false`.

A real call through the gateway answered in under a second:

```
gateway_upstream_unavailable  model=cerebras:gemma-4-31b
  error=completion unavailable: OpenAICompatExtractor billing limit (HTTP 402)
```

Cerebras **ended its free API tier on 2026-08-17** and moved existing accounts
to a credit-based plan requiring a payment method. Six days before the report.
Nothing in our repository changed; the provider did.

## Why it cost a whole day of extraction

`ExtractorQuotaError` set `_exhausted[i] = True`, which skips the provider for
the rest of **that run**. The continuous worker starts a run every few minutes
and builds a fresh extractor each time, so the flag never survived to the next
one. `gemma-4-31b` scores **80** — joint-highest in the catalogue — so it was
the router's first pick on every run, and every run spent that pick on a
refusal before falling through.

The ledger recorded each 402 as one more failed call and nothing else.
`blocked_until` was only ever written for a 429. So the one persistent
structure that could have remembered "this provider has no credit" was the one
place the information never reached.

## The fix

- `QuotaLedger.note_call(..., billing_blocked=True)` blocks the provider until
  the **next UTC midnight** — when free allowances and trial credit both reset —
  and persists it, so a new run does not restart the argument.
- `FallbackExtractor` passes that flag when the error names a 402. An ordinary
  500 still does not block: a bad minute must not retire the fleet.
- `cerebras: enabled: false` in `config/providers.yaml`. The ledger block alone
  would cost one wasted first-choice call a day and keep a dead provider in
  every `model_router_ready` log line.

While reading the numbers against the providers' own, Gemini's `rpm` was
corrected from 15 to 10: Google publishes 15 RPM for Flash-Lite but **10** for
Flash, and one number covers both models here, so the faster one was always
slightly over. Gemini took **301** 429s across 1,200 calls on 2026-08-22 and
the breaker had it blocked with 924 requests unspent. There is no published
tokens-per-day cap for Gemini — the limits are RPM, TPM and RPD — so `tpd`
stays unset, unlike Groq's.

## The lesson

The [[free-tier-model-router]] plans against numbers written from vendor docs,
and a free tier is a commercial decision that can be withdrawn between two
runs. **A 100% provider failure rate is a catalogue question, not a bug** — and
the ledger must be able to record "gone", not only "busy". The
`/free-models` skill already said to ask the API rather than the docs; what
this adds is that the answer has a shelf life measured in days.

Related: [[ai-provider-quota-runbook]], [[extractor-circuit-breaker]],
[[free-tier-model-router]].
