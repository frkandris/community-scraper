---
type: Decision
title: Continuous Worker
description: Why the twin time windows were deleted, what decides the work now, and the ten defects the reviews found in getting there.
tags: [schedule, pipeline, worker, operations, control-api]
timestamp: 2026-08-18
resource: scraper/main.py
---

# Continuous Worker

*Nothing is clock-bound any more. The work chooses itself.*

## Why the windows went

The twin schedule — collect 10:30-23:50, extract 00:30-10:00 — existed for two
reasons, and by 2026-08-18 neither was true:

- **DeepSeek's off-peak discount.** Extraction moved to a free-tier fleet whose
  allowance resets at 00:00 UTC ([[free-tier-model-router]]), so the discount
  window stopped meaning anything.
- **A belief that DataForSEO was cheaper at certain hours.** It is not.
  Verified against their published pricing: the price is set by *queue*, not by
  clock — $0.6/1K normal, $1.2/1K priority, $2/1K live. There was never a
  cheaper hour.

What remained was one real constraint: only one pipeline run at a time. That is
a condition, not a schedule.

## What decides the work

    free quota left  ->  extract, because it expires at 00:00 UTC
    none left        ->  collect, because that is what money buys

The daily reset is the only thing that happens "at" a time, and even that is
not scheduled. The collector runs with a `should_stop` predicate that asks "has
the quota come back?", so at midnight it winds up between pairs and extraction
takes over on its own. `run_pipeline` gained that predicate alongside `stop_at`
precisely because a deadline cannot express a condition.

A restart is indistinguishable from a continuation: the worker starts on boot
and picks the right work immediately, and every finished pair is already cached
(the fingerprint-keyed extraction cache and `search_cache`). **This is what stopped a deploy from
costing a collection run** — on 2026-08-17 three collector runs died as `run
cancelled` with 0 pairs because deploys landed mid-window, and the day yielded
206 pairs instead of ~800. Startup recovery is skipped entirely now; there is
nothing to recover.

Shipped behind `schedule.worker_enabled`, an ops toggle configured in source
control, with the twin crons left in place and disabled until the worker has
proven itself ([feature toggles](https://martinfowler.com/articles/feature-toggles.html),
[strangler fig](https://martinfowler.com/bliki/StranglerFigApplication.html)).

## Operating it

`/v1/control/{status,run,stop,resume}`, Bearer auth. Deliberately **not** part
of the OpenAI-compatible surface: `/v1/chat/completions` is a published
interface other software depends on, so the operator endpoints get their own
prefix and their own key — `CONTROL_API_KEY`, falling back to `ROUTER_API_KEY`
with a warning ([published interface](https://martinfowler.com/bliki/PublishedInterface.html)).

`GET /v1/backlog` answers "is there work?" directly, and echoes the settings the
running process actually has. Both exist because those questions kept being
answered by inference from a log buffer that holds a few minutes.

`launch_pipeline_run()` is the one place a run is reserved, started, recorded
and classified. The admin form, the control API and the worker all call it.

## What the reviews found

Ten defects across three rounds, and the shape is worth keeping:

| | |
|---|---|
| The worker inherited the launcher's defaults, which are the admin form's **Full Refresh** | would have re-bought all 45,570 pairs of search, and re-extracted every finished page — so the extractor never looks idle and collection never runs |
| "Did extraction find work?" was `len(pair_logs)` | `ai_only` logs a pair even with no cached pages, and every never-searched pair is in the filter, so an empty pass looked busy |
| Stop cancelled the run but did not pause the worker | the next run started within the minute; nothing could be stopped |
| The cancellation check read `_run_task` after the run's `finally` cleared it | a cancelled pass was always read as "found nothing" |
| Enrichment could run twice, and a cancel during preflight leaked its slot forever | the guard was set after an `await` |
| An empty collection pass cleared the extraction cooldown | a caught-up system alternated empty runs with no pause, writing a run record each time |
| Fixing that made the cooldown a **ratchet** | every empty pass pushed it 15 minutes further out, so extraction never resumed after the quota reset |
| The admin Run button did not clear the pause | one run, then paused until a restart |

Two of these were introduced by the previous round's fix. Both times the
mistake was the same: changing when a flag is set without asking what else
reads it.

## The one that was not the worker's fault

Minutes after the collector went to eight-way concurrency, the container ran
out of file descriptors:

```
providers_config_unreadable  [Errno 24] Too many open files
quota_ledger_unreadable      unable to open database file
```

Every HTTP request opened its own client, and the standard-mode search opened
one **per poll** — up to 150 per search at normal priority. The visible symptom
was SQLite and a YAML file failing to open: socket exhaustion three levels from
where it hurt. Clients are pooled per event loop now (per loop, because a client
holds connections belonging to the loop that created them).

Concurrency did not cause this. It revealed it — one connection per request was
always wrong.
