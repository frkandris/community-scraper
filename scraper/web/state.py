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
    current_url: str | None = None  # URL currently being AI-extracted


app_state = AppState()
