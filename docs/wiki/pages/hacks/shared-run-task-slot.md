---
type: Hack
title: One Shared _run_task Slot for Pipeline and Revalidate
description: Pipeline runs and revalidate both store their task in app_state._run_task but guard on different flags, so stop can cancel the wrong task.
tags: [asyncio, cancellation, revalidate, app-state, gotcha]
timestamp: 2026-07-09
resource: scraper/web/app.py
---

# One Shared _run_task Slot for Pipeline and Revalidate

*Every run path assigns its asyncio task to the single slot `app_state._run_task`; `POST /admin/api/stop` cancels whatever is in that slot.*

The pipeline guards concurrency on `app_state.is_running`, but **revalidate guards on a separate flag** (`_revalidate_state["running"]`). So a revalidate can be started while `is_running` is false, overwrite the `_run_task` pointer, and make `/admin/api/stop` cancel the wrong task (or nothing useful). The `is_running` checks mostly prevent overlap for the main pipeline, but the two-flag split is the gap. See [[asyncio-task-cancellation]] and [[pipeline-orchestration]].

Cleanup is deliberately split: the run's `finally` handles normal completion, while an `add_done_callback(_clear_cancelled_run)` resets `is_running`/`current_phase`/`current_url` for the cancelled case, because a cancelled task's `finally` may not run the intended path.
