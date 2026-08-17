---
type: Post-mortem
title: Rate Limits Opened the Circuit Breaker
description: The first full night on the free fleet ended 45 minutes in — the breaker counted "wait, you are going too fast" as "you are broken" and aborted the run with 13,523 calls still unspent.
tags: [post-mortem, router, extraction, circuit-breaker, rate-limit, enrichment]
timestamp: 2026-08-17
resource: scraper/extract.py
---

# Rate Limits Opened the Circuit Breaker

*The window ran 00:30–10:00. The run died at 01:15 and nothing used the other
8 hours 45 minutes.*

## Symptom

```
01:15:52  extract_provider_down_run_aborted
          reason='no extraction provider configured (20 consecutive failures)'
```

Overnight counts: 150 `extracted`, 131 `extractor_awaiting_rpm`, 81
`provider_minute_limit`, 75 `extractor_rate_limited`, 368 `enrich_call_failed`,
29 `extract_failed_pages`. Records grew 41,824 → 41,896 — about 72 in the night
the fleet was meant to spend ~16.5K calls.

`GET /v1/quota` afterwards: **13,523 Groq calls still available**. Nothing was
exhausted. Nothing was broken.

## Root cause

Two independent limits were collapsed into one counter.

`FallbackExtractor` opens a circuit breaker after 20 *consecutive* failed calls,
which is the right response to a fleet that is genuinely dead. But a 429 from a
per-minute limit is not a failure — it is the provider saying "not yet". Both
went through `_note_failure()`.

Once every live provider was inside its rpm cooldown at the same moment,
`_call` waited up to `_RATE_LIMIT_MAX_WAIT` (300 s), gave up, and counted a
failure. Twenty of those, and a fleet with five working providers and thirteen
thousand calls of budget declared itself absent.

The 300 s ceiling made it likely rather than merely possible: Gemini's daily
budget is large but its per-minute allowance is small, so the provider that
carries most of the volume also spends most of its time in cooldown.

`enrich_call_failed` ×368 is the same event seen from the enrichment job. Its
extractor reported no provider, and the per-record `except` did what it was
written to do — `continue`, do not mark, retry next round — 368 times in a few
seconds, **fetching a source page before each one**. The records were fine. The
fetches were not, and neither were the sites on the other end.

## Fix

| Change | File |
|---|---|
| Rate-limit waits never call `_note_failure` | `scraper/extract.py` |
| `_all_temporarily_blocked()` → raise with `rate_limited_out`, not a failure | `scraper/extract.py` |
| `_RATE_LIMIT_MAX_WAIT` 300 s → 900 s | `scraper/extract.py` |
| A `rate_limited_out` pause ends the extraction pass without marking the run aborted | `scraper/pipeline.py` |
| Enrichment stops the batch on the *first* failure once the extractor says it has nothing | `scraper/enrich.py` |

The distinction now runs all the way through: **"come back later" is not
"gone"**. A paused run resumes with its window; an aborted one wastes it.

## Lessons

- A circuit breaker must count only the failures it is meant to protect
  against. Ours guarded against a broken key or a retired model, then fired on
  a healthy provider working exactly as documented. Every retryable condition
  needs its own path out of the failure counter — see
  [[extractor-circuit-breaker]].
- The deeper cause is [structural and still open](#what-this-does-not-fix): the
  chain is serial, so "all providers are in cooldown" is a state it can reach
  at all. A work-conserving scheduler would be issuing a request to one of the
  four providers that are not.
- A retry loop that leaves side effects outside the retried call is not free.
  Enrichment's `continue` was correct about the database and wrong about the
  network.

## What this does not fix

Throughput. `FallbackExtractor._call` still issues one request at a time across
five providers with independent limits, which is why a full day's budget takes
~9 hours to spend and why simultaneous cooldown happens at all. Parallelising
the chain is the real fix (arXiv:2504.07347, work-conserving schedulers), and it
touches the code path every extraction goes through — its own change, its own
review round. See
[the research notes](/docs/wiki/sources/2026-08-16-evaluation-and-throughput-arxiv.md).
