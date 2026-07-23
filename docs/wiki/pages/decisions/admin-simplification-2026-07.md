---
type: Decision
title: Admin Simplification for the World-Indexing Focus
description: Removed the revalidate, recategorize, description-maintenance and Full Rebuild admin flows; the admin now centers on low-cost world indexing plus a user-interaction Inbox with pending badges.
tags: [decision, admin, simplification, run-modes, inbox]
timestamp: 2026-07-23
resource: scraper/web/app.py
---

# Admin Simplification for the World-Indexing Focus

*With the strategy set to "index the whole world slowly at low cost", every admin flow that wasn't collection, extraction, quality, or user feedback was deleted rather than maintained.*

## What was removed (2026-07-23)

- **Revalidate** — the LLM QA pass over existing communities: `/admin/revalidate/*` routes, `_run_revalidate`, the `revalidate_fingerprint` DB helpers and column guard, the dashboard preset + progress UI, and the prompts-page trigger card. Historical `revalidate` rows remain in `runs` and are handled defensively (startup maps them to `ai_only`; the dashboard renders unknown modes generically).
- **Recategorize** — AI topic re-classification of "other" communities: routes, worker, `recategorize_suggestions` table guard and helpers, template, nav links. Existing production tables are left orphaned (no data loss, no reads).
- **Maintenance / Description re-AI** — `/admin/maintenance*` routes, `_run_fill_descriptions`, template. (`/admin/cache/fill-fields` is separate and stays.)
- **Full Rebuild preset** — the dashboard card that re-fetched and re-extracted everything (run_mode=full with both skips off). The route still accepts skip flags; only the one-click expensive button is gone.
- Orphaned `history.html` / `history_detail.html` templates (no route rendered them).

## What stays

`full` (Smart) and `ai_only` (Re-AI) manual presets with the country/city scope filter — the manual counterparts of the [[cost-saver-schedule]] twin crons (`search_only` collect + `ai_only` extract). Coverage, progress, logs, config, prompts, stats, entity browsing, duplicates, false positives, subscriptions, and the daily report all remain.

## The Inbox

User-submitted items moved out of the Moderation dropdown into a dedicated **Inbox** nav group with live pending-count badges (per item + total). See [[web-app]] for the Jinja-global-callable mechanism. Moderation now holds only Duplicates.

## Why

Every named run mode is a maintenance surface, a scheduling hazard (all modes share one run slot) and an LLM cost temptation. The 2026-07 outages showed that unused modes still break: the more paths write to `runs` and pair logs, the harder incident diagnosis is. Deleting beats disabling — the code is in git history if a flow is ever needed again.
