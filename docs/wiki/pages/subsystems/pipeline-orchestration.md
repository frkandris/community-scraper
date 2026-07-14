---
type: Subsystem
title: Pipeline Orchestration
description: run_pipeline() sequences mode-specific passes with a done-pair pre-filter; bounded saver jobs prioritize Sweden while startup recovery remains Hungary-first.
tags: [pipeline, orchestration, run-modes, done-pairs, scheduler]
timestamp: 2026-07-14
resource: scraper/pipeline.py
---

# Pipeline Orchestration

*`run_pipeline()` is the single entry; a "full" run actually does `_run_ai_only` (re-extract cached pages) **then** `_run_full` (fresh search+fetch+extract).*

See [[pipeline-run-modes]] for the mode overview, [[done-pair-url-hash-not-city-topic]] for the pre-filter correctness rule, and [[end-to-end-pair-walkthrough]] for one pair traced through every module.

## What "full" really means

`run_pipeline(run_mode="full")` runs `_run_ai_only` first (cheap, cache-only re-extraction of stale pages), then `_run_full`, then a catch-up `_run_full` over `all_pairs - covered - done_pairs` to guarantee every never-covered pair gets one pass. `run_mode="ai_only"` runs only the cache pass. `run_mode="revalidate"` is **not handled here** — it lives entirely in `app.py:_run_revalidate` (a pure LLM QA pass over the DB). Every `run_pipeline` ends by firing `detect_all()` (duplicate scan) fire-and-forget via `asyncio.to_thread`.

## Done-pair pre-filter

When `skip_extracted` is on, `run_pipeline` computes mode-aware `done_pairs` and threads `pairs_filter = all_pairs - done_pairs` into every sub-call. `search_only` checks the pair-level `collected_at` marker written after the selected URL batch was attempted. AI modes check community, venue, and person fingerprints according to the enabled phase flags and the same community-presence gates used during extraction. Changing any enabled prompt/model invalidates done-status even when the pair already has visible records.

False-positive changes take the same route without changing the global fingerprint: pair examples explicitly clear only that pair's community extraction metadata, while global extraction rules clear it for all cached pages. Both `_run_ai_only` and `_run_full` pass the current pair-scoped negative examples to the extractor, so the next selected run uses the new rule consistently. `_run_ai_only` also attributes scraped pages through `search_cache` URL hashes rather than last-write-wins `cache_pages.city/topic`; one shared URL can therefore be reprocessed for every pair that uses it.

## Three geographic passes (main.py)

Both scheduled and startup paths partition cities into Hungary (339), Sweden (290), and rest-of-world (145), but their priority differs. Bounded saver jobs run Sweden → world → Hungary so expansion work receives the window first; startup recovery retains Hungary → Sweden → world. See [[hungary-sweden-intl-three-passes]]. Each call does its own done-pair pre-filter and client construction. Scale: 774 cities × 36 topics ≈ **27,900 pairs** per sweep, which is why the pre-filter and the 3650-day search-cache TTL matter.

## Startup state machine

`_startup_run` inspects the last run: if it was interrupted/failed, it retries the **same** mode (revalidate falls back to `ai_only`); if it succeeded, it escalates `revalidate → ai_only → full → full` (full is the steady state). This resumes interrupted work after a redeploy and climbs to more expensive passes once stable.

## Callbacks and cancellation

`on_pair_start(city, topic)` and `on_progress(phase, url)` mutate `app_state` progress fields, feeding the coverage live view. `RunCoordinator` reserves the one long-run slot synchronously and owns cancellation/identity-safe cleanup across manual, scheduled, startup, and revalidate paths. See [[shared-run-task-slot]] and [[asyncio-task-cancellation]].

## Scheduling lives in `main.py`

`main.py` registers the enabled cost-saver collector/extractor jobs, optional legacy combined run, and daily report with `AsyncIOScheduler`. Pipeline jobs enter through `_cron_run`, use the same geographic passes and `RunCoordinator` slot as manual/startup work, and persist one run record. See [[scheduler-disabled-no-cron]], [[cost-saver-schedule]], and [[run-modes-and-startup]].
