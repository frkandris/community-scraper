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

## What the review rounds found

Seven rounds, and the first two fixes were each nearly worse than the bug.

**The exemption almost swallowed the breaker whole.** The quota ledger stamps a
provider's rpm clock on *every* attempt, failures included — so a fleet
answering 500s ends a call looking exactly like a fleet in cooldown, and the
new exemption would have applied to it. A run against a genuinely dead API
would never have aborted. Only an error actually seen during the call may open
the breaker now (`real_failure_seen`), which pointedly excludes a retired model
and a spent quota: both already have their own handling.

**`rate_limited_out` was latched.** Set once and never cleared, one unlucky
moment where every provider happened to be in cooldown would have stopped
extraction for the rest of the window — and, because callers stop before the
chain can look again, permanently masked a fleet that died afterwards. It is
cleared at the top of every call, as `quota_exhausted` already was.

**`pair_log["extract_error"]` was a crash.** Three branches set `extract_dead`
and only one of them sets that key, so the shipped fix turned a clean pause into
a `KeyError` in `_run_full`. Both sites now branch on `aborted`, and a pause
also leaves the *city* loop — otherwise the next city went on paying DataForSEO
for pages nothing could extract.

**The breaker is now per provider.** One endpoint stuck on 500s used to drive a
single global counter to 20 and retire the whole fleet with it, healthy
providers included. A provider retires itself; `providers_down` — all of them
retired — is what aborts a run. Two subtleties the rounds surfaced: `_call`
retries once, so counting per *attempt* silently halved the configured
threshold; and a provider that fails an attempt and succeeds on the retry is
working, so it must be excluded from the tally its own call collected.

**A 429 back-off no longer sleeps past the window.** `_RATE_LIMIT_MAX_WAIT` is
15 minutes; without a deadline a call starting at 09:58 slept into the collector
window that follows. `extractor.deadline` is the run's `stop_at`.

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
- Two of the seven review rounds found bugs *in the previous round's fix*. The
  pattern is specific: each one relaxed a safety rule without asking what else
  relied on it. Exempting rate limits from the breaker also exempted a dead
  fleet; stopping the pass on a flag also crashed on a key only one branch set.
- There was no measurement. The run achieved 3.3 extractions/min against a
  combined fleet ceiling of 185 calls/min and nothing recorded whether the gap
  was latency or pacing. `extractor_throughput` now logs `calls`, `call_s`,
  `wait_s` and `calls_per_min` at the end of every run.

## What this does not fix

Throughput — and it did not, for one more day. `FallbackExtractor._call` issued
one request at a time across five providers with independent limits, which is
why a full day's budget took ~9 hours to spend and why simultaneous cooldown was
reachable at all. That was fixed the same day in its own change, with its own
review rounds: [[concurrent-extraction]]. The research behind it is in
[the notes](/docs/wiki/sources/2026-08-16-evaluation-and-throughput-arxiv.md)
(arXiv:2504.07347, work-conserving schedulers).
