---
type: Decision
title: Concurrent Extraction
description: Why a pair's pages are extracted several at a time, what had to be true first, and the config knob that turns it off.
tags: [pipeline, extraction, concurrency, router, throughput]
timestamp: 2026-08-17
resource: scraper/pipeline.py
---

# Concurrent Extraction

*The fleet's combined ceiling is 185 calls per minute. The run was doing 3.3.*

## The measurement that forced it

On 2026-08-17 the extraction window managed **3.3 extractions/min** and left
**15,690 free calls** to expire at midnight. Six free-tier providers with
independent rate limits, and `FallbackExtractor._call` issued one request at a
time — so the fleet spent the night waiting on latency it could have overlapped
([[2026-08-rate-limits-opened-the-breaker]] is what surfaced the number;
arXiv:2504.07347 is why idle capacity under a solvable constraint is lost
throughput, not merely slow).

Nothing about the work required serialisation. A pair's pages are independent:
nothing one page extracts affects another. The loop was serial because it was
written as a loop.

## What had to be true first

Two things were unsafe under concurrency and had to change *before* it, as
separate behaviour-preserving commits (Fowler's preparatory refactoring — make
the change easy, then make the easy change):

**Provenance was read after the fact.** Which model served a page came from
`extractor.last_model`, read after the await returned. Correct only because no
await separates the two — an invisible ordering requirement, and the moment two
pages run together the page is cached under whichever model finished last.
`cache_pages.extract_quality` is exactly the number that drives or blocks the
upgrade sweep. `extract_traced()` returns `(records, model, quality)` together.

**The quota ledger recorded a call when it returned.** While a call is in flight
the provider still looked idle and under budget, so every waiting page would
have picked the same one, blown its rpm together, and collected 429s instead of
using the other five. `reserve_call()` claims the slot at selection time;
`note_call(reserved=True)` records the outcome. Stamping at start is also the
more correct reading of rpm, which limits requests *started* per minute.

## The change

`_extract_pair_pages()` runs a pair's cache-missing pages under a semaphore and
returns `({url: result-or-error}, stop)`. The page loop is otherwise untouched:
it reads an answer already in hand instead of awaiting one, so cache writes,
venue and person extraction, and enrichment stay serial and in page order.

`pipeline.extract_concurrency` (settings.yaml, a **mounted volume** in
production) bounds it. **1 reproduces the serial chain exactly** — the kill
switch, and the first thing to try when diagnosing anything in this area.

`_run_full` is deliberately still serial. It is not on the twin schedule, so
converting it would widen the blast radius without buying a window.

## Invariants concurrency introduced

Each of these is a bug that the review rounds found, phrased as the rule that
now holds:

- **A page absent from the result map was never attempted; a page mapped to an
  exception was.** They are not the same: the first retries for free, the second
  is a counted failure.
- **The stop is recorded once, before the loop.** A stop can arrive with every
  page already attempted, leaving nothing absent to notice it by — and recording
  it per url turned one outage into one `extract_failed` per queued page.
- **The loop always runs to the end of the pair.** `extract_dead` suppresses
  further calls; it must never skip a cache write, because with concurrency the
  fleet has already been charged for results still sitting in the map.
- **Only breaker retirements are reversible.** A provider that answers has
  proven itself alive, even if another page's failures retired it a moment
  earlier. A 402 or a retired model name stands.
- **A failure that a success outran is not consecutive.** Each provider carries
  a success generation; a failure recorded after the provider answered someone
  else is dropped.
- **A reservation is settled exactly once.** `note_call` on any outcome, and a
  `finally` releases it on cancellation — `asyncio.CancelledError` is a
  `BaseException` and slips past every `except Exception`.
- **A stop seen mid-flight is a snapshot, not a verdict.** A request already in
  flight can revive the very provider whose failures caused it, and an early
  rate-limit pause would otherwise mask a real outage that happened later. The
  state after everything has landed decides — and if it says the fleet is fine,
  the pages that stepped aside get another turn rather than being written off.
- **Pacing waits by the clock, not by attempt count.** A sibling can claim the
  slot this task just waited for; three fixed retries lost that race often
  enough to have the chain call a healthy fleet "all rate limited".

## What to watch

`extractor_throughput` at the end of every run: `calls_per_min` against the
fleet's 185/min ceiling, and `wait_s` against `call_s`. A large gap that
concurrency does not close means pacing binds, and raising the knob further will
only collect 429s.
