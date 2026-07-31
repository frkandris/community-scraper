---
type: Runbook
title: Run Modes and Startup State Machine
description: How to trigger runs (dashboard cards, manual, startup) and how the startup escalates ai_only → full.
tags: [operations, run-modes, startup, dashboard]
timestamp: 2026-07-23
resource: scraper/main.py
---

# Run Modes and Startup State Machine

*Runs can start from the dashboard/API, startup recovery, CLI, or the enabled cost-saver schedule; every long run shares one coordinator-owned slot.*

## Dashboard cards

| Card | run_mode | skip_scraped | skip_extracted | Effect |
|---|---|---|---|---|
| Smart | full | on | on | Uses all caches; re-extracts stale pages, then searches new pairs |
| Full Refresh | full | off | off | Ignores caches; complete rescrape + re-extract |
| Re-AI | ai_only | — | off | Re-runs the LLM on cached texts; no web requests |

"Smart" (`full`) internally runs `ai_only` first, then `_run_full`, then a catch-up pass — see [[pipeline-orchestration]]. Revalidate is a separate flow (LLM QA over the DB, no scraping).

## Startup recovery

`_startup_run` (gated on `schedule.auto_run_on_startup`, **now on**) delegates the
decision to the pure `_startup_plan(last_row, schedule_cfg, now)`:

- **Saver schedule on (production)** — startup is a *crash-recovery net only*. An
  interrupted `search_only`/`ai_only` run (deploy killed it mid-window) resumes the
  **same** mode, boxed to its window (`search_until`/`extract_until` via
  `_next_window_end`). A clean boot, or an interrupted non-bounded mode, does
  **nothing** — the twin crons drive the day and startup must never launch a `full`
  (LLM) run outside the off-peak split.
- **Saver off (legacy)** — unchanged: interrupted/failed retries the same mode
  (historical `revalidate` → `ai_only`); succeeded escalates `ai_only → full → full`,
  unbounded.

"Interrupted" means `finished_at IS NULL` **or** `outcome='aborted'`. Since
2026-07-31 a `warning` run — it finished, some pairs failed and were not cached —
counts as a clean boot and is not recovered; the next scheduled run picks those
pairs up anyway ([[run-outcome-three-states]]). `_startup_plan` falls back to the
legacy `success` boolean for rows written before the column existed.

This exists because deploys during the 15 h collector window (01:00→16:20 UTC) were
silently truncating the day's collection — see [[2026-07-deploy-truncates-collector]].
Recovery still reserves the shared coordinator slot, so it is skipped if a scheduled
run is already active.

## Scheduled runs

The default schedule separates collection and extraction: a daytime `search_only` job fills raw-page cache through DataForSEO, then an off-peak `ai_only` job uses DeepSeek. The legacy combined `full` cron is separately opt-in. See [[cost-saver-schedule]] and [[scheduler-disabled-no-cron]].

## Stopping a run

`POST /admin/api/stop` asks `RunCoordinator` to cancel its current task. Pipeline, cron, and startup runs all reserve the same slot; task-identity cleanup prevents an older task from clearing newer state. See [[shared-run-task-slot]] and [[asyncio-task-cancellation]].

Related deployment controls: [[deployment-coolify]].

## Changing the extraction model or prompt

Editing the model (settings.yaml) or a prompt (`/admin/prompts`) changes the [[extraction-fingerprints|fingerprint]], which invalidates all done-pairs and triggers re-extraction on the next run. Use Smart mode to avoid re-fetching. If results are still valid after a prompt tweak, `POST /admin/api/restamp-fingerprints` bulk-updates fingerprints without reprocessing.
