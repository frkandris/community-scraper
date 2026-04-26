import hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import structlog

from .db import (
    clear_all_cache_pages,
    delete_cache_page,
    get_all_scraped_cache,
    get_cache_index,
    load_cache_page,
    save_cache_page,
)
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
    def __init__(self, db_path: Path):
        self.db_path = db_path

    # ── Scrape ──────────────────────────────────────────────────────────────

    def get_scraped(self, url: str) -> str | None:
        entry = load_cache_page(self.db_path, _url_hash(url))
        return entry.get("raw_text") if entry else None

    def save_scraped(self, url: str, text: str, city: str, topic: str,
                     duration_s: float | None = None,
                     source_queries: list[str] | None = None) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h) or {}
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
        save_cache_page(self.db_path, entry)
        log.debug("cache_saved_scrape", url=url)

    # ── Extract ─────────────────────────────────────────────────────────────

    def get_extracted(self, url: str) -> list[CommunityRecord] | None:
        entry = load_cache_page(self.db_path, _url_hash(url))
        if not entry or not entry.get("extracted_at") or entry.get("records") is None:
            return None
        try:
            return [CommunityRecord.model_validate(r) for r in entry["records"]]
        except Exception:
            return None

    def save_extracted(self, url: str, records: list[CommunityRecord],
                       duration_s: float | None = None) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h) or {"url": url, "url_hash": h, "domain": _domain(url)}
        entry.update({
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "records": [r.model_dump() for r in records],
            "enrich_scraped_at": None,
            "enrich_scrape_duration_s": None,
            "enrich_extracted_at": None,
            "enrich_extract_duration_s": None,
            "enrich_count": None,
            "enrich_log": None,
        })
        if duration_s is not None:
            entry["extract_duration_s"] = round(duration_s, 2)
        save_cache_page(self.db_path, entry)
        log.debug("cache_saved_extract", url=url, records=len(records))

    def save_enriched_records(self, url: str, records: list[CommunityRecord]) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h)
        if not entry:
            return
        entry["records"] = [r.model_dump() for r in records]
        save_cache_page(self.db_path, entry)

    # ── Enrich timing markers ────────────────────────────────────────────────

    def mark_enrich_scraped(self, url: str, duration_s: float) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h)
        if not entry:
            return
        entry["enrich_scraped_at"] = datetime.now(timezone.utc).isoformat()
        entry["enrich_scrape_duration_s"] = round(duration_s, 2)
        save_cache_page(self.db_path, entry)

    def save_enrich_log(self, url: str, enrich_log: list[dict]) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h)
        if not entry:
            return
        entry["enrich_log"] = enrich_log
        save_cache_page(self.db_path, entry)

    def mark_enrich_extracted(self, url: str, count: int, duration_s: float) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h)
        if not entry:
            return
        entry["enrich_extracted_at"] = datetime.now(timezone.utc).isoformat()
        entry["enrich_extract_duration_s"] = round(duration_s, 2)
        entry["enrich_count"] = count
        save_cache_page(self.db_path, entry)

    # ── Bulk read ────────────────────────────────────────────────────────────

    def get_all_scraped(self) -> list[tuple[str, str, str, str]]:
        """Returns (url, raw_text, city, topic) for all cached scraped pages."""
        return get_all_scraped_cache(self.db_path)

    def get_index(self) -> list[dict]:
        return get_cache_index(self.db_path)

    # ── Delete ───────────────────────────────────────────────────────────────

    def delete_scraped(self, url_hash: str) -> bool:
        entry = load_cache_page(self.db_path, url_hash)
        if not entry:
            return False
        entry.pop("raw_text", None)
        entry.pop("scraped_at", None)
        entry.pop("scrape_duration_s", None)
        save_cache_page(self.db_path, entry)
        return True

    def delete_extracted(self, url_hash: str) -> bool:
        entry = load_cache_page(self.db_path, url_hash)
        if not entry:
            return False
        for key in ("records", "extracted_at", "extract_duration_s",
                    "enrich_scraped_at", "enrich_scrape_duration_s",
                    "enrich_extracted_at", "enrich_extract_duration_s", "enrich_count",
                    "enrich_log"):
            entry.pop(key, None)
        save_cache_page(self.db_path, entry)
        return True

    def get_entry(self, url_hash: str) -> dict | None:
        return load_cache_page(self.db_path, url_hash)

    def delete_entry(self, url_hash: str) -> bool:
        return delete_cache_page(self.db_path, url_hash)

    def clear_all(self) -> int:
        count = clear_all_cache_pages(self.db_path)
        log.info("cache_cleared_all", deleted=count)
        return count
