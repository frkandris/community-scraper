---
type: Hack
title: AsyncIO Task Cancellation
description: Long runs use asyncio.create_task through RunCoordinator; BackgroundTasks cannot be cancelled and CancelledError is a BaseException.
tags: [asyncio, cancellation, stop-button]
timestamp: 2026-07-24
resource: scraper/web/state.py
---

# AsyncIO Task Cancellation

*FastAPI `BackgroundTasks` cannot be cancelled. Long pipeline runs use `asyncio.create_task()` and attach the task to `app_state.run_coordinator` so the stop button can cancel the single owner.*

## Why BackgroundTasks don't work

FastAPI's `BackgroundTasks` run after the response is sent, in a context that has no handle for cancellation. There is no way to interrupt them from another request.

## The pattern

```python
if app_state.run_coordinator.reserve("smart"):
    task = asyncio.create_task(_run())
    app_state.run_coordinator.attach(task)
```

The stop route calls `app_state.run_coordinator.cancel()`. Cleanup calls `release(current_task())`; an identity check prevents an old task from clearing newer state, and the coordinator's done callback covers pre-start cancellation.

## Critical: CancelledError is a BaseException

In Python 3.8+, `asyncio.CancelledError` inherits from `BaseException`, not `Exception`. A bare `except Exception` will NOT catch it. Always use `finally` for cleanup:

```python
try:
    ...
except Exception as exc:
    log.error("run_failed", error=str(exc))
finally:
    app_state.run_coordinator.release(asyncio.current_task())
```

## Related

- [[shared-run-task-slot]]
