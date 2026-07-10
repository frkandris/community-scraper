---
type: Hack
title: Single RunCoordinator Owns the Task Slot
description: Pipeline, scheduled, startup, and revalidate runs reserve one coordinator-owned task slot with identity-safe cleanup.
tags: [asyncio, cancellation, revalidate, app-state, gotcha]
timestamp: 2026-07-10
resource: scraper/web/state.py
---

# Single RunCoordinator Owns the Task Slot

*Fixed 2026-07-10: all cancellable long runs reserve, attach, release, and cancel through `app_state.run_coordinator`.*

`RunCoordinator.reserve(mode)` synchronously claims the slot before `create_task`, so two route handlers cannot both start. `attach(task)` records the cancellable owner, and scheduled/startup runners reserve with their current task. Revalidate no longer has an independent concurrency guard.

Both `finally` and a coordinator-owned done callback call `release(task)`. Release succeeds only if that exact task still owns the slot, so a stale callback/finally cannot clear a newer run. The callback also covers a task cancelled before its coroutine body begins. `/admin/api/stop` delegates to `RunCoordinator.cancel()`.

See [[asyncio-task-cancellation]] and [[pipeline-orchestration]].
