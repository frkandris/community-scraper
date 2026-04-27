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
    _task_queue: Any = None           # asyncio.Queue, created lazily
    queue_items: list = field(default_factory=list)
    _queue_worker_task: Any = None

    def get_queue(self):
        import asyncio
        if self._task_queue is None:
            self._task_queue = asyncio.Queue()
        return self._task_queue


app_state = AppState()
