---
type: Runbook
title: Run Modes and Startup State Machine
description: How to trigger runs (dashboard cards, manual, startup) and how the startup escalates revalidate → ai_only → full.
tags: [operations, run-modes, startup, dashboard]
timestamp: 2026-07-10
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

## Startup escalation

`_startup_run` (gated on `schedule.auto_run_on_startup`) inspects the last run:

- interrupted/failed → retry the **same** mode (revalidate → falls back to `ai_only`).
- succeeded → escalate `revalidate → ai_only → full → full` (full is the steady state).

This resumes work after a redeploy and climbs to more expensive passes once stable.

## Scheduled runs

The default schedule separates collection and extraction: a daytime `search_only` job fills raw-page cache through DataForSEO, then an off-peak `ai_only` job uses DeepSeek. The legacy combined `full` cron is separately opt-in. See [[cost-saver-schedule]] and [[scheduler-disabled-no-cron]].

## Stopping a run

`POST /admin/api/stop` asks `RunCoordinator` to cancel its current task. Pipeline, cron, startup, and revalidate all reserve the same slot; task-identity cleanup prevents an older task from clearing newer state. See [[shared-run-task-slot]] and [[asyncio-task-cancellation]].

Related deployment controls: [[deployment-coolify]].

## Changing the extraction model or prompt

Editing the model (settings.yaml) or a prompt (`/admin/prompts`) changes the [[extraction-fingerprints|fingerprint]], which invalidates all done-pairs and triggers re-extraction on the next run. Use Smart mode to avoid re-fetching. If results are still valid after a prompt tweak, `POST /admin/api/restamp-fingerprints` bulk-updates fingerprints without reprocessing.
