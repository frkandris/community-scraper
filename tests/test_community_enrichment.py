from pathlib import Path
from scraper.db import init_db, upsert_venues, upsert_persons, get_venue_for_community, get_persons_for_community
from scraper.store import save_results
from scraper.models import VenueRecord, PersonRecord, CommunityRecord


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_get_venue_for_community_found(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    cid = r.community_id
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"], community_ids=[cid],
    )
    upsert_venues(db, [v.model_dump()])

    result = get_venue_for_community(db, cid, "Budapest")
    assert result is not None
    assert result["name"] == "Müpa Budapest"


def test_get_venue_for_community_not_found(tmp_path):
    db = _db(tmp_path)
    result = get_venue_for_community(db, "nonexistent-id", "Budapest")
    assert result is None


def test_get_venue_for_community_empty_id(tmp_path):
    db = _db(tmp_path)
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"],
    )
    upsert_venues(db, [v.model_dump()])
    result = get_venue_for_community(db, "", "Budapest")
    assert result is None
