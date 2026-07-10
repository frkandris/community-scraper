---
type: Subsystem
title: Pipeline Orchestration
description: run_pipeline() sequences ai_only + full passes with a done-pair pre-filter; main.py runs it three times (Hungary → Sweden → world).
tags: [pipeline, orchestration, run-modes, done-pairs, scheduler]
timestamp: 2026-07-10
resource: scraper/pipeline.py
---

# Pipeline Orchestration

*`run_pipeline()` is the single entry; a "full" run actually does `_run_ai_only` (re-extract cached pages) **then** `_run_full` (fresh search+fetch+extract).*

See [[pipeline-run-modes]] for the mode overview, [[done-pair-url-hash-not-city-topic]] for the pre-filter correctness rule.

## What "full" really means

`run_pipeline(run_mode="full")` runs `_run_ai_only` first (cheap, cache-only re-extraction of stale pages), then `_run_full`, then a catch-up `_run_full` over `all_pairs - covered - done_pairs` to guarantee every never-covered pair gets one pass. `run_mode="ai_only"` runs only the cache pass. `run_mode="revalidate"` is **not handled here** — it lives entirely in `app.py:_run_revalidate` (a pure LLM QA pass over the DB). Every `run_pipeline` ends by firing `detect_all()` (duplicate scan) fire-and-forget via `asyncio.to_thread`.

## Done-pair pre-filter

When `skip_extracted` is on, `run_pipeline` computes mode-aware `done_pairs` and threads `pairs_filter = all_pairs - done_pairs` into every sub-call. `search_only` checks capped fetch completion. AI modes check community, venue, and person fingerprints according to the enabled phase flags and the same community-presence gates used during extraction. Changing any enabled prompt/model invalidates done-status even when the pair already has visible records.

## Three geographic passes (main.py)

Both `_scheduled_run` and `_startup_run` partition cities and call `run_pipeline` **three times**: Hungary (339 cities) → Sweden (290) → rest-of-world (145). See [[hungary-sweden-intl-three-passes]]. Each call does its own done-pair pre-filter, its own extractor/search-client construction, and its own `detect_all` (so the dup scan runs 3× per sweep — idempotent). Scale: 774 cities × 36 topics ≈ **27,900 pairs** per sweep, which is why the pre-filter and the 3650-day search-cache TTL matter.

## Startup state machine

`_startup_run` inspects the last run: if it was interrupted/failed, it retries the **same** mode (revalidate falls back to `ai_only`); if it succeeded, it escalates `revalidate → ai_only → full → full` (full is the steady state). This resumes interrupted work after a redeploy and climbs to more expensive passes once stable.

## Callbacks and cancellation

`on_pair_start(city, topic)` and `on_progress(phase, url)` mutate `app_state` in place (`current_city`, `current_topic`, `current_phase` ∈ {scrape, extract, enrich_scrape, enrich_extract}), feeding the coverage live view. Every run stores its task in the single shared slot `app_state._run_task`; `POST /admin/api/stop` cancels it. **Trap:** revalidate guards on a *different* flag (`_revalidate_state["running"]`) than the pipeline's `is_running`, so it can clobber `_run_task` and make stop cancel the wrong task. See [[shared-run-task-slot]] and [[asyncio-task-cancellation]].

## The scheduler is a no-op

An `AsyncIOScheduler` is started but **never given a job** — `CronTrigger` is imported but unused, and `schedule.cron` in settings is ignored. Runs come only from startup (when `auto_run_on_startup` is true), manual `/admin/api/run`, or `--run-once`. See [[scheduler-disabled-no-cron]].

## Known inconsistency

`_run_ai_only` omits `false_positive_examples` from its `extract()` call, whereas `_run_full` includes them — cache-only re-extractions miss per-pair false-positive hints.
