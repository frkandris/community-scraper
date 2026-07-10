import hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import structlog

from .db import (
    clear_all_cache_pages,
    clear_person_cache,
    delete_cache_page,
    get_all_scraped_cache,
    get_cache_index,
    get_scraped_cache_by_search_pair,
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

    def get_extracted(self, url: str,
                      fingerprint: str | None = None) -> list[CommunityRecord] | None:
        entry = load_cache_page(self.db_path, _url_hash(url))
        if not entry or not entry.get("extracted_at") or entry.get("records") is None:
            return None
        if fingerprint and entry.get("extract_fingerprint") != fingerprint:
            log.debug("cache_fingerprint_mismatch", url=url,
                      stored=entry.get("extract_fingerprint"), current=fingerprint)
            return None
        try:
            return [CommunityRecord.model_validate(r) for r in entry["records"]]
        except Exception:
            return None

    def save_extracted(self, url: str, records: list[CommunityRecord],
                       duration_s: float | None = None,
                       fingerprint: str | None = None,
                       model: str | None = None) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h) or {"url": url, "url_hash": h, "domain": _domain(url)}
        entry.update({
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "extract_fingerprint": fingerprint,
            "extract_model": model,
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
        log.debug("cache_saved_extract", url=url, records=len(records),
                  fingerprint=fingerprint, model=model)

    def save_enriched_records(self, url: str, records: list[CommunityRecord]) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h)
        if not entry:
            return
        entry["records"] = [r.model_dump() for r in records]
        save_cache_page(self.db_path, entry)

    # ── Venue extraction cache ───────────────────────────────────────────────

    def get_venue_extracted(self, url: str, fingerprint: str | None = None) -> list[dict] | None:
        entry = load_cache_page(self.db_path, _url_hash(url))
        if not entry or not entry.get("venue_extracted_at"):
            return None
        if fingerprint and entry.get("venue_fingerprint") != fingerprint:
            return None
        return entry.get("venues_data") or []

    def save_venue_extracted(self, url: str, venues: list[dict],
                              fingerprint: str | None = None, model: str | None = None) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h) or {"url": url, "url_hash": h, "domain": _domain(url)}
        entry["venue_extracted_at"] = datetime.now(timezone.utc).isoformat()
        entry["venue_fingerprint"] = fingerprint
        entry["venue_model"] = model
        entry["venues_data"] = venues
        save_cache_page(self.db_path, entry)

    # ── Person extraction cache ──────────────────────────────────────────────

    def get_person_extracted(self, url: str, city: str, topic: str,
                              fingerprint: str | None = None) -> list[dict] | None:
        entry = load_cache_page(self.db_path, _url_hash(url))
        if not entry or not entry.get("person_extracted_at"):
            return None
        if fingerprint and entry.get("person_fingerprint") != fingerprint:
            return None
        persons_data = entry.get("persons_data") or {}
        key = f"{city}/{topic}"
        if key not in persons_data:
            return None
        return persons_data[key]

    def save_person_extracted(self, url: str, city: str, topic: str, persons: list[dict],
                               fingerprint: str | None = None, model: str | None = None) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h) or {"url": url, "url_hash": h, "domain": _domain(url)}
        persons_data = entry.get("persons_data") or {}
        persons_data[f"{city}/{topic}"] = persons
        entry["person_extracted_at"] = datetime.now(timezone.utc).isoformat()
        entry["person_fingerprint"] = fingerprint
        entry["person_model"] = model
        entry["persons_data"] = persons_data
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

    def mark_enrich_extracted(self, url: str, count: int, duration_s: float,
                              model: str | None = None) -> None:
        h = _url_hash(url)
        entry = load_cache_page(self.db_path, h)
        if not entry:
            return
        entry["enrich_extracted_at"] = datetime.now(timezone.utc).isoformat()
        entry["enrich_extract_duration_s"] = round(duration_s, 2)
        entry["enrich_count"] = count
        if model is not None:
            entry["enrich_model"] = model
        save_cache_page(self.db_path, entry)

    # ── Bulk read ────────────────────────────────────────────────────────────

    def get_all_scraped(self) -> list[tuple[str, str, str, str]]:
        """Returns (url, raw_text, city, topic) for all cached scraped pages."""
        return get_all_scraped_cache(self.db_path)

    def get_scraped_by_search_pair(self) -> list[tuple[str, str, str, str]]:
        """Returns scraped pages attributed to every search-cache pair using them."""
        return get_scraped_cache_by_search_pair(self.db_path)

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
                    "extract_fingerprint", "extract_model",
                    "enrich_scraped_at", "enrich_scrape_duration_s",
                    "enrich_extracted_at", "enrich_extract_duration_s", "enrich_count",
                    "enrich_model", "enrich_log"):
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

    def clear_person_extracted(self) -> int:
        count = clear_person_cache(self.db_path)
        log.info("person_cache_cleared", updated=count)
        return count
