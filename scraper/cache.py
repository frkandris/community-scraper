import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import structlog

from .models import CommunityRecord

log = structlog.get_logger()


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


class CacheManager:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.pages_dir = cache_dir / "pages"
        self.pages_dir.mkdir(parents=True, exist_ok=True)

    def _page_path(self, url_hash: str) -> Path:
        return self.pages_dir / f"{url_hash}.json"

    def _load(self, url_hash: str) -> dict | None:
        path = self._page_path(url_hash)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _save(self, entry: dict) -> None:
        path = self._page_path(entry["url_hash"])
        path.write_text(json.dumps(entry, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_scraped(self, url: str) -> str | None:
        entry = self._load(_url_hash(url))
        return entry.get("raw_text") if entry else None

    def save_scraped(self, url: str, text: str, city: str, topic: str) -> None:
        h = _url_hash(url)
        entry = self._load(h) or {}
        entry.update({
            "url": url,
            "url_hash": h,
            "domain": _domain(url),
            "city": city,
            "topic": topic,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "raw_text": text,
        })
        self._save(entry)
        log.debug("cache_saved_scrape", url=url)

    def get_extracted(self, url: str) -> list[CommunityRecord] | None:
        entry = self._load(_url_hash(url))
        if not entry or not entry.get("extracted_at") or entry.get("records") is None:
            return None
        try:
            return [CommunityRecord.model_validate(r) for r in entry["records"]]
        except Exception:
            return None

    def save_extracted(self, url: str, records: list[CommunityRecord]) -> None:
        h = _url_hash(url)
        entry = self._load(h) or {"url": url, "url_hash": h, "domain": _domain(url)}
        entry.update({
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "records": [r.model_dump() for r in records],
        })
        self._save(entry)
        log.debug("cache_saved_extract", url=url, records=len(records))

    def get_all_scraped(self) -> list[tuple[str, str, str, str]]:
        """Returns (url, raw_text, city, topic) for all cached scraped pages."""
        result = []
        for path in self.pages_dir.glob("*.json"):
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
                if entry.get("raw_text"):
                    result.append((
                        entry["url"],
                        entry["raw_text"],
                        entry.get("city", ""),
                        entry.get("topic", ""),
                    ))
            except Exception:
                continue
        return result

    def get_index(self) -> list[dict]:
        entries = []
        for path in sorted(self.pages_dir.glob("*.json"), key=lambda p: p.name):
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
                entries.append({
                    "url_hash": entry.get("url_hash", path.stem),
                    "url": entry.get("url", ""),
                    "domain": entry.get("domain", ""),
                    "city": entry.get("city", ""),
                    "topic": entry.get("topic", ""),
                    "scraped_at": entry.get("scraped_at"),
                    "extracted_at": entry.get("extracted_at"),
                    "record_count": len(entry.get("records") or []),
                    "has_text": bool(entry.get("raw_text")),
                })
            except Exception:
                continue
        return sorted(entries, key=lambda e: e.get("scraped_at") or "", reverse=True)

    def delete_scraped(self, url_hash: str) -> bool:
        entry = self._load(url_hash)
        if not entry:
            return False
        entry.pop("raw_text", None)
        entry.pop("scraped_at", None)
        self._save(entry)
        return True

    def delete_extracted(self, url_hash: str) -> bool:
        entry = self._load(url_hash)
        if not entry:
            return False
        entry.pop("records", None)
        entry.pop("extracted_at", None)
        self._save(entry)
        return True

    def get_entry(self, url_hash: str) -> dict | None:
        return self._load(url_hash)

    def delete_entry(self, url_hash: str) -> bool:
        path = self._page_path(url_hash)
        if path.exists():
            path.unlink()
            return True
        return False
