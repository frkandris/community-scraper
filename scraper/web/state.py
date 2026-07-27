from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


class RunCoordinator:
    """Own the one cancellable long-running task and its shared UI state."""

    def __init__(self, state: "AppState") -> None:
        self._state = state

    def reserve(self, mode: str, task: Any = None) -> bool:
        """Atomically reserve the run slot on the event-loop thread."""
        if self._state.is_running:
            return False
        self._state.is_running = True
        self._state.current_run_mode = mode
        self._state._run_task = task
        if task is not None:
            task.add_done_callback(self._on_done)
        return True

    def attach(self, task: Any) -> None:
        """Attach the task created immediately after an unbound reservation."""
        if not self._state.is_running or self._state._run_task is not None:
            raise RuntimeError("run slot is not available for task attachment")
        self._state._run_task = task
        task.add_done_callback(self._on_done)

    def release(self, task: Any) -> bool:
        """Clear state only when the releasing task still owns the slot."""
        if task is None or self._state._run_task is not task:
            return False
        self._state.is_running = False
        self._state.current_phase = None
        self._state.current_url = None
        self._state.current_run_mode = None
        self._state.current_city = None
        self._state.current_topic = None
        self._state._run_task = None
        return True

    def cancel(self) -> bool:
        task = self._state._run_task
        if task is None or task.done():
            return False
        task.cancel()
        return True

    def _on_done(self, task: Any) -> None:
        self.release(task)


@dataclass
class AppState:
    is_running: bool = False
    last_run_at: datetime | None = None
    cities: list = field(default_factory=list)
    topics: list = field(default_factory=list)
    pipeline_cfg: Any = None
    scheduler: Any = None
    cache_manager: Any = None
    db_path: Path | None = None
    version: str = "v.unknown"
    current_phase: str | None = None     # "scrape" | "extract" | "enrich_scrape" | "enrich_extract"
    current_url: str | None = None       # source URL of the cache row being processed
    current_run_mode: str | None = None  # "full" | "ai_only" | "search_only"
    current_city: str | None = None      # city being processed in current pair
    current_topic: str | None = None     # topic being processed in current pair
    _run_task: Any = None
    last_enrich_result: dict | None = None   # result of the most recent /api/enrich batch
    _enrich_running: bool = False            # guards the managed off-peak enrichment job
    _enrich_task: Any = None                 # manual /api/enrich task (cancellable via /api/stop)
    # Task queue — queue_items is the authoritative ordered list
    queue_items: list = field(default_factory=list)   # list of item dicts (pending/running/done)
    _queue_fns: dict = field(default_factory=dict)    # item_id -> coroutine fn
    _queue_event: Any = None                          # asyncio.Event, lazy
    _queue_worker_task: Any = None
    run_coordinator: RunCoordinator = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.run_coordinator = RunCoordinator(self)

    def get_queue_event(self):
        import asyncio
        if self._queue_event is None:
            self._queue_event = asyncio.Event()
        return self._queue_event


app_state = AppState()
