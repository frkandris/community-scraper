---
type: Hack
title: AsyncIO Task Cancellation
description: Use asyncio.create_task + app_state._run_task; BackgroundTasks cannot be cancelled and CancelledError is a BaseException.
tags: [asyncio, cancellation, stop-button]
timestamp: 2026-07-09
resource: scraper/web/app.py
---

# AsyncIO Task Cancellation: Use create_task, Not BackgroundTasks

*FastAPI `BackgroundTasks` cannot be cancelled. Long-running pipeline runs must use `asyncio.create_task()` and store the task in `app_state._run_task` so the stop button can cancel them.*

## Why BackgroundTasks don't work

FastAPI's `BackgroundTasks` run after the response is sent, in a context that has no handle for cancellation. There is no way to interrupt them from another request.

## The pattern

```python
task = asyncio.create_task(_run())
app_state._run_task = task
task.add_done_callback(_clear_cancelled_run)
```

The stop route calls `app_state._run_task.cancel()`. The task's `finally` block cleans up state regardless of whether it was cancelled or completed normally.

## Critical: CancelledError is a BaseException

In Python 3.8+, `asyncio.CancelledError` inherits from `BaseException`, not `Exception`. A bare `except Exception` will NOT catch it. Always use `finally` for cleanup:

```python
try:
    ...
except Exception as exc:
    log.error("run_failed", error=str(exc))
finally:
    app_state.is_running = False  # always runs, even on cancel
    app_state.current_phase = None
    app_state.current_url = None
    app_state.current_city = None
    app_state.current_topic = None
```

## Related

- [[app-state-singleton]]
