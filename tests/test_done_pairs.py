import hashlib
from pathlib import Path

from scraper.db import (
    bulk_upsert_communities,
    get_collected_pairs,
    get_fully_processed_pairs,
    init_db,
    save_cache_page,
    save_search_cache,
)


URL = "https://example.com/community"


def _db(tmp_path: Path) -> Path:
    db = tmp_path / "scraper.db"
    init_db(db)
    save_search_cache(db, "Budapest", "running", [URL], ["query"])
    return db


def _save_page(db: Path, **updates) -> None:
    entry = {
        "url": URL,
        "url_hash": hashlib.sha256(URL.encode()).hexdigest()[:16],
        "city": "Budapest",
        "topic": "running",
        "scraped_at": "2026-01-01T00:00:00+00:00",
        "extract_fingerprint": "community-v2",
        "venue_fingerprint": "venue-v2",
        "person_fingerprint": "person-v2",
        "records": [{"name": "Futók"}],
        "venues_data": [],
        "persons_data": {"Budapest/running": []},
    }
    entry.update(updates)
    save_cache_page(db, entry)


def _done(db: Path, **flags) -> set[tuple[str, str]]:
    return get_fully_processed_pairs(
        db,
        "community-v2",
        "venue-v2",
        "person-v2",
        **flags,
    )


def test_stale_community_fingerprint_keeps_green_pair_runnable(tmp_path):
    db = _db(tmp_path)
    bulk_upsert_communities(db, [{
        "name": "Budapest Futók",
        "city": "Budapest",
        "topic": "running",
        "community_id": "visible-community",
    }])
    _save_page(db, extract_fingerprint="community-v1")

    assert _done(db, run_communities=True) == set()


def test_enabled_venue_and_person_fingerprints_are_required(tmp_path):
    db = _db(tmp_path)
    _save_page(db, venue_fingerprint="venue-v1")
    assert _done(db, run_communities=True, run_venues=True) == set()
    assert _done(db, run_communities=True, run_venues=False) == {
        ("Budapest", "running")
    }

    _save_page(db, persons_data={})
    assert _done(db, run_communities=True, run_persons=True) == set()


def test_empty_community_result_skips_gated_venue_and_person_requirements(tmp_path):
    db = _db(tmp_path)
    _save_page(
        db,
        records=[],
        venue_fingerprint=None,
        person_fingerprint=None,
        persons_data={},
    )

    assert _done(
        db,
        run_communities=True,
        run_venues=True,
        run_persons=True,
    ) == {("Budapest", "running")}


def test_search_collection_requires_the_capped_urls_to_be_scraped(tmp_path):
    db = _db(tmp_path)
    assert get_collected_pairs(db, max_pages=1) == set()

    _save_page(db)

    assert get_collected_pairs(db, max_pages=1) == {("Budapest", "running")}


def test_extraction_done_check_ignores_urls_beyond_fetch_cap(tmp_path):
    db = _db(tmp_path)
    extra_url = "https://example.com/beyond-cap"
    save_search_cache(db, "Budapest", "running", [URL, extra_url], ["query"])
    _save_page(db)
    save_cache_page(db, {
        "url": extra_url,
        "url_hash": hashlib.sha256(extra_url.encode()).hexdigest()[:16],
        "scraped_at": "2026-01-01T00:00:00+00:00",
        "extract_fingerprint": "community-v1",
        "records": [{"name": "Stale"}],
    })

    assert get_fully_processed_pairs(
        db,
        "community-v2",
        run_communities=True,
        max_pages=1,
    ) == {("Budapest", "running")}
