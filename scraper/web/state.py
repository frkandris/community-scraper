from dataclasses import dataclass, field
from datetime import datetime
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
    version: str = "v.unknown"


app_state = AppState()
