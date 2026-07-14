---
type: Post-mortem
title: search_only Replayed Cached Communities During Collection
description: The first saver collector replayed extraction-cache records into Hungarian communities and retried pairs forever when any selected URL could not be fetched.
tags: [post-mortem, search-only, cache, scheduler, history, sweden]
timestamp: 2026-07-14
resource: scraper/pipeline.py
---

# search_only Replayed Cached Communities During Collection

*A nominally read-only collection mode caused 395 field-history changes across 31 Hungarian communities and spent most of its bounded window revisiting mature pairs.*

## Symptom

The 2026-07-13 daily report showed a 14,357-pair `search_only` run, 31 changed Hungarian communities with 395 field changes, and only 1,590 new international searches. The following `ai_only` run failed before logging its first pair.

## Root cause

`run_pipeline` disabled all extraction families for `search_only`, but `_run_full` still read current community records from the extraction cache and unconditionally called `save_results` at pair end. Collection therefore replayed cached entity snapshots through history-generating upserts.

Separately, `get_collected_pairs` defined completion as every capped search URL having a successful cached page. A permanently unreadable result made its whole pair runnable every day, so mature Hungarian pairs consumed the window before Sweden.

The top-level scheduler exception was logged but not stored in `runs`, leaving the zero-pair `ai_only` failure opaque in the email.

## Fix

- `search_only` exits immediately after the fetch batch and cannot touch extraction caches or entity persistence.
- `search_cache.collected_at` records that every selected URL was attempted; only process interruption leaves the pair resumable.
- Bounded saver jobs run Sweden → world → Hungary.
- `runs.error` persists scheduled/startup exceptions and the daily report renders it safely.

## Lessons

A mode flag is not an isolation boundary unless the code returns before unrelated cache and persistence paths. Completion state must distinguish “attempted terminally” from “all external I/O succeeded,” especially when some URLs are permanently inaccessible.

See [[cost-saver-schedule]], [[pipeline-run-modes]], and [[done-pair-url-hash-not-city-topic]].
