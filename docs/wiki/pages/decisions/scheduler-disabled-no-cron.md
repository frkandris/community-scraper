---
type: Decision
title: Scheduler and Cost-Saver Cron Configuration
description: APScheduler registers the enabled twin cost-saver jobs and daily report; the legacy combined cron remains opt-in.
tags: [scheduler, cron, decision, main]
timestamp: 2026-07-10
resource: scraper/main.py
---

# Scheduler and Cost-Saver Cron Configuration

*The scheduler was once empty; it now registers jobs from three independent settings, with the cost-saver and report schedules enabled by default.*

## Current jobs

- `schedule.saver_enabled: true` registers the `search_only` collector and `ai_only` off-peak extractor. See [[cost-saver-schedule]].
- `schedule.report_enabled: true` registers the daily summary email job.
- `schedule.cron_enabled: false` controls the older combined `full` run. It stays available, but is off so it cannot duplicate the twin schedule.

The dashboard (`POST /admin/api/run`), optional startup run, and `--run-once` CLI remain additional triggers. All long-running scheduled and manual paths reserve the same [[shared-run-task-slot|RunCoordinator]] slot, so an overrun skips rather than overlaps another pipeline run.
