---
type: Post-mortem
title: Mid-Window Deploys Silently Truncated the Daily Collector
description: A deploy landing inside the 15 h search_only window kills the in-flight collector; with auto_run_on_startup off it never resumed and lost the rest of the day's page collection — invisible because the evening extractor lived off the cached-page backlog.
tags: [post-mortem, saver, deployment, startup, search, observability]
timestamp: 2026-07-26
resource: scraper/main.py
---

# Mid-Window Deploys Silently Truncated the Daily Collector

*The 01:00 UTC `search_only` collector runs for over 15 hours; any deploy in that window kills it, and startup recovery was disabled — so the day's remaining collection was silently dropped.*

## Symptom

The 2026-07-25 daily email showed `❌ search_only · 01:00 UTC · 0 város–téma páros · 0 rekord — futási hiba: run cancelled (deploy, restart, or manual stop)`. The day still produced 1717 new communities, so nothing looked broken. Two consecutive days carried the same `run cancelled` marker.

## Root cause

Confirmed against the production `runs` table (`ssh root@157.180.21.144` → `docker exec … sqlite3`):

| Day | collector finished | state |
|---|---|---|
| 07-16 → 07-20 | 16:20 (full window) | ok |
| **07-24** | **05:45** | ❌ run cancelled |
| **07-25** | **07:08** | ❌ run cancelled |
| 07-26 | 16:20 (full window) | ok |

The collector *started* at 01:00 and was cancelled **hours later** (05:45, 07:08) — not at 01:00. The saver collector window is `01:00 → 16:20 UTC` (15 h 20 m), so **any** deploy during the working day lands inside it. `asyncio.CancelledError` propagates from the killed `run_pipeline`, `finish_run` records `run cancelled (deploy, restart, or manual stop)`, and the container comes up on the new image. Those two days coincided with actively shipping a large changeset (commit `409be8f` and predecessors) → frequent deploys.

Two independent reasons nothing re-ran it:

1. **Startup recovery was off** — `schedule.auto_run_on_startup: false`, so `_startup_run()` never fired. (Its retry set already included `search_only`; the machinery existed but was disabled.)
2. **APScheduler won't re-fire a job that already fired.** The 01:00 job had fired (that's why a run row exists) and was killed mid-run, so the next trigger is *tomorrow* 01:00. `misfire_grace_time` only helps when the process was down *across* the cron instant.

Impact was masked: the evening `ai_only` extractor works off the already-cached page backlog, so new-community counts stayed healthy while the *fresh-page* backlog quietly thinned on deploy-heavy days.

Adjacent finding (already self-resolved): `ai_only` rows on 07-16→07-20 had `finished_at IS NULL` — hard-kill/OOM before the `finally` ran (the old whole-cache materialization). From 07-21 the one-pair-at-a-time load lands cleanly. See root `CLAUDE.md`.

## Fix (2026-07-26)

Turned startup recovery into a **saver-aware crash-recovery net** rather than a driver:

- New pure `_startup_plan(last_row, schedule_cfg, now)` in `main.py` (unit-tested in `tests/test_startup_recovery.py`). Under `saver_enabled`: an *interrupted* `search_only`/`ai_only` run resumes that same mode **boxed to its window** (`_startup_until` → `search_until` / `extract_until`, via `_next_window_end`); a *clean* boot returns `(None, None)` — do nothing. Startup must never launch `full` under the saver split (it would run DeepSeek outside the off-peak window).
- Saver disabled → legacy escalation (`ai_only → full`, unbounded) preserved verbatim.
- `_startup_run()` consumes the plan, passes `stop_at`, and breaks between city groups once the window closes (matching `_cron_run`).
- `schedule.auto_run_on_startup: true`.

Now a mid-window deploy-kill resumes collection for the rest of that day's window; a normal deploy is a no-op on startup; the `RunCoordinator` still prevents overlap with an active scheduled run.

## Lessons

A recovery mechanism that is disabled is worse than absent — it reads as present in the code and lulls review. A long-running scheduled window (15 h) turns *every* deploy into a data-loss event, so recovery has to be automatic, not "re-trigger manually after deploying." Enabling a blanket startup run would have over-corrected — every clean deploy launching a `full` LLM run — so the recovery had to be *scoped to the interruption* and *boxed to the same window* its cron twin uses. And the failure was only diagnosable because each cancellation leaves a fingerprint in the `runs` table: `run cancelled` + an early `finished_at` inside the window is the signature of a deploy-kill, distinct from a clean early finish (no more work) or a hard kill (`finished_at IS NULL`).

See [[run-modes-and-startup]], [[cost-saver-schedule]], [[deployment-coolify]], [[asyncio-task-cancellation]], and [[shared-run-task-slot]].
