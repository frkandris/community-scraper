"""Application log: a live ring for the admin tail, a rotating file for history.

The ring alone held 500 lines. Under the continuous worker — eight searches and
four extractions in flight — that is a few minutes, and every attempt to answer
"what happened last night?" this week ran into a buffer that had already
forgotten. The file is the record; the ring is only what the live view needs.

Rotation and compression are the standard library's, with a two-line gzip hook.
A log-rotating dependency would carry more than it saves here: this writes one
stream from one process, which is the case `RotatingFileHandler` was written
for.
"""
from __future__ import annotations

import gzip
import json
import logging
import logging.handlers
import os
import shutil
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

#: Roughly a day of the current volume per file, five days kept. Compressed
#: backups run about a tenth of that, so the whole history costs a few tens of
#: megabytes on the same volume as the database.
_MAX_BYTES = int(os.environ.get("LOG_MAX_BYTES") or 20 * 1024 * 1024)
_BACKUPS = int(os.environ.get("LOG_BACKUPS") or 5)

#: Lines the in-memory ring keeps for the admin live tail. Small on purpose —
#: history comes from the file now.
_RING = 500


def _gzip_rotator(source: str, dest: str) -> None:
    with open(source, "rb") as f_in, gzip.open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(source)


def _namer(name: str) -> str:
    return name + ".gz"


class LogBroadcaster:
    """One line in, two places out: the ring and the rotating file."""

    def __init__(self, maxlen: int = _RING) -> None:
        self._lines: deque = deque(maxlen=maxlen)
        self._seq: int = 0
        self._file: logging.Logger | None = None
        self._path: Path | None = None

    # ── writing ──────────────────────────────────────────────────────────────

    def attach_file(self, directory: Path) -> None:
        """Start writing to `<directory>/app.log`. Safe to call more than once.

        Called once at startup with the persisted data directory. Failing to
        open it must not stop the app: losing history is bad, refusing to serve
        is worse.
        """
        try:
            directory.mkdir(parents=True, exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                directory / "app.log", maxBytes=_MAX_BYTES,
                backupCount=_BACKUPS, encoding="utf-8")
            handler.rotator, handler.namer = _gzip_rotator, _namer
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger = logging.getLogger("meetapedia.applog")
            logger.setLevel(logging.INFO)
            logger.propagate = False
            for old in list(logger.handlers):
                logger.removeHandler(old)
            logger.addHandler(handler)
            self._file, self._path = logger, directory / "app.log"
        except Exception:  # noqa: BLE001 - see docstring
            self._file, self._path = None, None

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
        now = datetime.now(timezone.utc)
        self._lines.append({
            "seq": self._seq,
            "ts": now.strftime("%H:%M:%S"),
            "level": level,
            "text": text,
        })
        if self._file is not None:
            # Full ISO date in the file: a ring only ever holds today, a file
            # spans days and "05:31:55" alone cannot say which one.
            self._file.info(json.dumps(
                {"ts": now.isoformat(timespec="seconds"), "level": level, "text": text},
                ensure_ascii=False))

    # ── reading ──────────────────────────────────────────────────────────────

    def get_lines_after(self, seq: int) -> list:
        return [line for line in self._lines if line["seq"] > seq]

    def get_all(self) -> list:
        return list(self._lines)

    def _files_newest_first(self) -> list[Path]:
        if self._path is None:
            return []
        # app.log, then app.log.1.gz, app.log.2.gz … as RotatingFileHandler
        # numbers them: 1 is the most recent backup.
        found = [self._path] if self._path.exists() else []
        for i in range(1, _BACKUPS + 1):
            for candidate in (Path(f"{self._path}.{i}.gz"), Path(f"{self._path}.{i}")):
                if candidate.exists():
                    found.append(candidate)
                    break
        return found

    def history(self, limit: int = 200, grep: str = "", level: str = "") -> list:
        """Up to `limit` matching lines, oldest first, from the rotating files.

        Walks backwards through the files so a narrow `grep` can reach into
        yesterday without loading today into memory first. Falls back to the
        ring when no file is attached (tests, and the moments before startup
        finishes).
        """
        # Case-insensitive, over the whole row: an operator greps for "Errno"
        # or a provider name without thinking about which field it lands in.
        needle, want = grep.lower(), level.lower()

        def _match(row: dict) -> bool:
            if want and str(row.get("level", "")).lower() != want:
                return False
            return not needle or needle in json.dumps(row, ensure_ascii=False).lower()

        files = self._files_newest_first()
        if not files:
            return [r for r in self._lines if _match(r)][-limit:]

        out: list = []
        for path in files:
            opener = gzip.open if path.suffix == ".gz" else open
            try:
                with opener(path, "rt", encoding="utf-8", errors="replace") as fh:
                    rows = fh.readlines()
            except Exception:  # noqa: BLE001 — a damaged backup must not 500
                continue
            for raw in reversed(rows):
                try:
                    row = json.loads(raw)
                except ValueError:
                    continue
                if not _match(row):
                    continue
                out.append(row)
                if len(out) >= limit:
                    return list(reversed(out))
        return list(reversed(out))


broadcaster = LogBroadcaster()
