---
type: Decision
title: Scheduler Registered But Empty (No Cron Jobs)
description: An AsyncIOScheduler is started but never given a job; runs come only from startup, manual trigger, or --run-once.
tags: [scheduler, cron, decision, main]
timestamp: 2026-07-09
resource: scraper/main.py
---

# Scheduler Registered But Empty (No Cron Jobs)

*Updated 2026-07-09: the cron is now wired but **opt-in** — `schedule.cron_enabled: false` by default. When enabled, `schedule.cron` (preset to `35 16 * * *` UTC, the start of DeepSeek's off-peak discount window) schedules `_scheduled_run` with a 900 s misfire grace.*

With `cron_enabled: false` (the default), **no periodic runs happen.** Runs are triggered only by:

1. **Startup** — `if _settings_auto_run_on_startup(): asyncio.create_task(_startup_run())`, gated on `schedule.auto_run_on_startup` (default `false`).
2. **Manual** — `POST /admin/api/run`.
3. **CLI** — `--run-once` (runs all cities together, no HU/SE/intl split).
4. **Cron** — only when `schedule.cron_enabled: true`; pairing it with the off-peak window (UTC 16:30–00:30) halves DeepSeek costs. See [[cost-optimization-2026-07]], [[pipeline-run-modes]] and [[pipeline-orchestration]].
