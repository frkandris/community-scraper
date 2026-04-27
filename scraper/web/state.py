from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


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
    current_phase: str | None = None  # "scrape" | "extract" | "enrich_scrape" | "enrich_extract"
    current_url: str | None = None    # source URL of the cache row being processed
    _run_task: Any = None
    # Task queue — queue_items is the authoritative ordered list
    queue_items: list = field(default_factory=list)   # list of item dicts (pending/running/done)
    _queue_fns: dict = field(default_factory=dict)    # item_id -> coroutine fn
    _queue_event: Any = None                          # asyncio.Event, lazy
    _queue_worker_task: Any = None

    def get_queue_event(self):
        import asyncio
        if self._queue_event is None:
            self._queue_event = asyncio.Event()
        return self._queue_event


app_state = AppState()
