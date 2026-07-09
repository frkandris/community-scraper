---
type: Decision
title: Three Geographic Passes (Hungary → Sweden → World)
description: main.py runs run_pipeline three times over partitioned city lists; order is business priority — home market, biggest expansion market, then the long tail.
tags: [pipeline, priority, sweden, hungary, main]
timestamp: 2026-07-09
resource: scraper/main.py
---

# Three Geographic Passes (Hungary → Sweden → World)

*Both `_scheduled_run` and `_startup_run` partition `app_state.cities` into three lists and call `run_pipeline` three times sequentially.*

- `hu_cities` = `country == "Hungary"` — **339** cities
- `se_cities` = `country == "Sweden"` — **290** cities (all Swedish kommuner)
- `intl_cities` = everything else — **145** cities

Total **774 cities × 36 topics ≈ 27,900 pairs** per sweep.

## Why this order

Order = business priority: home market first, biggest expansion market second, long tail last. Sweden is broken out as its own pass (not lumped into "international") because its 290-municipality list makes it the second-largest national block after Hungary, and it is the primary expansion target — so its data should refresh right after the home market, before the 145 scattered world cities. See [[sweden-pipeline-priority]].

## Consequences

The three calls are independent `run_pipeline` invocations, so each does its own done-pair pre-filter, its own extractor/search-client construction, and its own `detect_all` (the duplicate scan runs **3× per sweep** — idempotent). Each call's `total_new` return is discarded; only `pair_logs` is kept and persisted via `finish_run`. This split lives in `main.py`, not `pipeline.py`.
