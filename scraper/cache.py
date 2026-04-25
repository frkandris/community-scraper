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

    # ── Scrape ──────────────────────────────────────────────────────────────

    def get_scraped(self, url: str) -> str | None:
        entry = self._load(_url_hash(url))
        return entry.get("raw_text") if entry else None

    def save_scraped(self, url: str, text: str, city: str, topic: str,
                     duration_s: float | None = None,
                     source_queries: list[str] | None = None) -> None:
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
        if duration_s is not None:
            entry["scrape_duration_s"] = round(duration_s, 2)
        if source_queries is not None:
            entry["source_queries"] = source_queries
        self._save(entry)
        log.debug("cache_saved_scrape", url=url)

    # ── Extract ─────────────────────────────────────────────────────────────

    def get_extracted(self, url: str) -> list[CommunityRecord] | None:
        entry = self._load(_url_hash(url))
        if not entry or not entry.get("extracted_at") or entry.get("records") is None:
            return None
        try:
            return [CommunityRecord.model_validate(r) for r in entry["records"]]
        except Exception:
            return None

    def save_extracted(self, url: str, records: list[CommunityRecord],
                       duration_s: float | None = None) -> None:
        h = _url_hash(url)
        entry = self._load(h) or {"url": url, "url_hash": h, "domain": _domain(url)}
        entry.update({
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "records": [r.model_dump() for r in records],
            # Clear stale enrich timing — pipeline will re-set it immediately after
            "enrich_scraped_at": None,
            "enrich_scrape_duration_s": None,
            "enrich_extracted_at": None,
            "enrich_extract_duration_s": None,
            "enrich_count": None,
            "enrich_log": None,
        })
        if duration_s is not None:
            entry["extract_duration_s"] = round(duration_s, 2)
        self._save(entry)
        log.debug("cache_saved_extract", url=url, records=len(records))

    def save_enriched_records(self, url: str, records: list[CommunityRecord]) -> None:
        """Update records in the cache entry without touching timestamps or timing fields."""
        h = _url_hash(url)
        entry = self._load(h)
        if not entry:
            return
        entry["records"] = [r.model_dump() for r in records]
        self._save(entry)

    # ── Enrich timing markers ────────────────────────────────────────────────

    def mark_enrich_scraped(self, url: str, duration_s: float) -> None:
        h = _url_hash(url)
        entry = self._load(h)
        if not entry:
            return
        entry["enrich_scraped_at"] = datetime.now(timezone.utc).isoformat()
        entry["enrich_scrape_duration_s"] = round(duration_s, 2)
        self._save(entry)

    def save_enrich_log(self, url: str, enrich_log: list[dict]) -> None:
        h = _url_hash(url)
        entry = self._load(h)
        if not entry:
            return
        entry["enrich_log"] = enrich_log
        self._save(entry)

    def mark_enrich_extracted(self, url: str, count: int, duration_s: float) -> None:
        h = _url_hash(url)
        entry = self._load(h)
        if not entry:
            return
        entry["enrich_extracted_at"] = datetime.now(timezone.utc).isoformat()
        entry["enrich_extract_duration_s"] = round(duration_s, 2)
        entry["enrich_count"] = count
        self._save(entry)

    # ── Bulk read ────────────────────────────────────────────────────────────

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
                    "url_hash":               entry.get("url_hash", path.stem),
                    "url":                    entry.get("url", ""),
                    "domain":                 entry.get("domain", ""),
                    "city":                   entry.get("city", ""),
                    "topic":                  entry.get("topic", ""),
                    "scraped_at":             entry.get("scraped_at"),
                    "scrape_duration_s":      entry.get("scrape_duration_s"),
                    "extracted_at":           entry.get("extracted_at"),
                    "extract_duration_s":     entry.get("extract_duration_s"),
                    "enrich_scraped_at":      entry.get("enrich_scraped_at"),
                    "enrich_scrape_duration_s": entry.get("enrich_scrape_duration_s"),
                    "enrich_extracted_at":    entry.get("enrich_extracted_at"),
                    "enrich_extract_duration_s": entry.get("enrich_extract_duration_s"),
                    "enrich_count":           entry.get("enrich_count"),
                    "record_count":           len(entry.get("records") or []),
                    "has_text":               bool(entry.get("raw_text")),
                })
            except Exception:
                continue
        def _sort_key(e: dict) -> tuple:
            # Incomplete entries (missing extract) float to the top; within each tier, newest first
            complete = 1 if (e.get("scraped_at") and e.get("extracted_at")) else 0
            ts = e.get("scraped_at") or "0000-00-00T00:00:00+00:00"
            return (complete, tuple(-ord(c) for c in ts))

        return sorted(entries, key=_sort_key)

    # ── Delete ───────────────────────────────────────────────────────────────

    def delete_scraped(self, url_hash: str) -> bool:
        entry = self._load(url_hash)
        if not entry:
            return False
        entry.pop("raw_text", None)
        entry.pop("scraped_at", None)
        entry.pop("scrape_duration_s", None)
        self._save(entry)
        return True

    def delete_extracted(self, url_hash: str) -> bool:
        entry = self._load(url_hash)
        if not entry:
            return False
        for key in ("records", "extracted_at", "extract_duration_s",
                    "enrich_scraped_at", "enrich_scrape_duration_s",
                    "enrich_extracted_at", "enrich_extract_duration_s", "enrich_count",
                    "enrich_log"):
            entry.pop(key, None)
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

    def clear_all(self) -> int:
        count = 0
        for path in self.pages_dir.glob("*.json"):
            path.unlink()
            count += 1
        log.info("cache_cleared_all", deleted=count)
        return count
