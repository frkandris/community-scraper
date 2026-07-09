---
type: Runbook
title: Run Modes and Startup State Machine
description: How to trigger runs (dashboard cards, manual, startup) and how the startup escalates revalidate → ai_only → full.
tags: [operations, run-modes, startup, dashboard]
timestamp: 2026-07-09
resource: scraper/main.py
---

# Run Modes and Startup State Machine

*The pipeline runs only from the dashboard, a manual API call, or startup — the scheduler is a no-op ([[scheduler-disabled-no-cron]]).*

## Dashboard cards

| Card | run_mode | skip_scraped | skip_extracted | Effect |
|---|---|---|---|---|
| Smart | full | on | on | Uses all caches; re-extracts stale pages, then searches new pairs |
| Full Refresh | full | off | off | Ignores caches; complete rescrape + re-extract |
| Re-AI | ai_only | — | off | Re-runs the LLM on cached texts; no web requests |

"Smart" (`full`) internally runs `ai_only` first, then `_run_full`, then a catch-up pass — see [[pipeline-orchestration]]. Revalidate is a separate flow (LLM QA over the DB, no scraping).

## Startup escalation

`_startup_run` (gated on `schedule.auto_run_on_startup`) inspects the last run:

- interrupted/failed → retry the **same** mode (revalidate → falls back to `ai_only`).
- succeeded → escalate `revalidate → ai_only → full → full` (full is the steady state).

This resumes work after a redeploy and climbs to more expensive passes once stable.

## Stopping a run

`POST /admin/api/stop` cancels `app_state._run_task`. Only one run at a time (guarded by `is_running`; revalidate uses its own flag — see [[shared-run-task-slot]]).

## Changing the extraction model or prompt

Editing the model (settings.yaml) or a prompt (`/admin/prompts`) changes the [[extraction-fingerprints|fingerprint]], which invalidates all done-pairs and triggers re-extraction on the next run. Use Smart mode to avoid re-fetching. If results are still valid after a prompt tweak, `POST /admin/api/restamp-fingerprints` bulk-updates fingerprints without reprocessing.
