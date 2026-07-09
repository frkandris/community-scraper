---
type: Decision
title: Scheduler Registered But Empty (No Cron Jobs)
description: An AsyncIOScheduler is started but never given a job; runs come only from startup, manual trigger, or --run-once.
tags: [scheduler, cron, decision, main]
timestamp: 2026-07-09
resource: scraper/main.py
---

# Scheduler Registered But Empty (No Cron Jobs)

*`main()` starts an `AsyncIOScheduler` and stores it on `app_state.scheduler`, but **never calls `add_job`**. `CronTrigger` is imported yet unused; `schedule.cron` in settings is parsed but never wired to a trigger.*

Despite `config/settings.yaml` setting `cron: "* * * * *"` (every minute), **no periodic runs happen.** Runs are triggered only by:

1. **Startup** — `if _settings_auto_run_on_startup(): asyncio.create_task(_startup_run())`, gated on `schedule.auto_run_on_startup` (default `false`).
2. **Manual** — `POST /admin/api/run`.
3. **CLI** — `--run-once` (runs all cities together, no HU/SE/intl split).

The scheduler object exists as scaffolding for future cron re-activation. Anyone expecting the settings cron to fire will get nothing — flag this. See [[pipeline-run-modes]] and [[pipeline-orchestration]].
