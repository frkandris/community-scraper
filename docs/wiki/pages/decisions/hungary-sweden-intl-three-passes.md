---
type: Decision
title: Three Geographic Passes with Mode-Specific Priority
description: main.py partitions Hungary, Sweden, and world into independent passes; bounded saver jobs are expansion-first while startup recovery is Hungary-first.
tags: [pipeline, priority, sweden, hungary, main]
timestamp: 2026-07-14
resource: scraper/main.py
---

# Three Geographic Passes with Mode-Specific Priority

*Both `_scheduled_run` and `_startup_run` partition `app_state.cities` into three lists and call `run_pipeline` three times sequentially.*

- `hu_cities` = `country == "Hungary"` — **339** cities
- `se_cities` = `country == "Sweden"` — **290** cities (all Swedish kommuner)
- `intl_cities` = everything else — **145** cities

Total **774 cities × 36 topics ≈ 27,900 pairs** per sweep.

## Why the orders differ

Bounded saver runs use Sweden → world → Hungary because the daily stop time makes first position scarce capacity. Startup recovery retains Hungary → Sweden → world to preserve its existing resume behavior. Sweden remains a distinct pass because its 290 municipalities make it the largest expansion block. See [[sweden-pipeline-priority]].

## Consequences

The three calls are independent `run_pipeline` invocations, so each does its own done-pair pre-filter, its own extractor/search-client construction, and its own `detect_all` (the duplicate scan runs **3× per sweep** — idempotent). Each call's `total_new` return is discarded; only `pair_logs` is kept and persisted via `finish_run`. This split lives in `main.py`, not `pipeline.py`.
