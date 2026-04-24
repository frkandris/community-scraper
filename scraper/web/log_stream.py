from collections import deque
from datetime import datetime, timezone


class LogBroadcaster:
    def __init__(self, maxlen: int = 500):
        self._lines: deque = deque(maxlen=maxlen)
        self._seq: int = 0

    def add_line(self, event_dict: dict) -> None:
        self._seq += 1
        level = event_dict.get("log_level", "info")
        event = str(event_dict.get("event", ""))
        extras = {
            k: str(v) for k, v in event_dict.items()
            if k not in ("timestamp", "log_level", "event")
        }
        text = event
        if extras:
            text += "  " + "  ".join(f"{k}={v}" for k, v in extras.items())
        self._lines.append({
            "seq": self._seq,
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "level": level,
            "text": text,
        })

    def get_lines_after(self, seq: int) -> list:
        return [line for line in self._lines if line["seq"] > seq]

    def get_all(self) -> list:
        return list(self._lines)


broadcaster = LogBroadcaster()
