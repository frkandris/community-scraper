from pathlib import Path
from scraper.db import (
    init_db, upsert_venues, upsert_persons,
    get_communities_for_venue,
)
from scraper.models import VenueRecord, PersonRecord
from scraper.store import save_results
from scraper.models import CommunityRecord


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _venue(name="Müpa Budapest", city="Budapest", community_ids=None):
    return VenueRecord(
        name=name, city=city, locale="hu",
        source_url="https://mupa.hu",
        extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"],
        community_ids=community_ids or [],
    )


def test_get_communities_for_venue_by_community_ids(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Zenei Kör", topic="music", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "music", [r], db)
    cid = r.community_id
    upsert_venues(db, [_venue(community_ids=[cid]).model_dump()])

    result = get_communities_for_venue(db, [cid], "Müpa Budapest", "Budapest")
    assert len(result) == 1
    assert result[0]["name"] == "Zenei Kör"


def test_get_communities_for_venue_fallback_location(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Tánc Csoport", topic="dance", city="Budapest", locale="hu",
        source_url="https://b.test", extracted_at="2026-01-01T00:00:00+00:00",
        location="Müpa Budapest nagyszínpad",
    )
    save_results("Budapest", "dance", [r], db)
    upsert_venues(db, [_venue().model_dump()])

    result = get_communities_for_venue(db, [], "Müpa Budapest", "Budapest")
    assert len(result) == 1
    assert result[0]["name"] == "Tánc Csoport"


def test_get_communities_for_venue_empty(tmp_path):
    db = _db(tmp_path)
    upsert_venues(db, [_venue().model_dump()])
    result = get_communities_for_venue(db, [], "Müpa Budapest", "Budapest")
    assert result == []


def test_get_communities_for_venue_stale_ids_fallback(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Tánc Csoport", topic="dance", city="Budapest", locale="hu",
        source_url="https://b.test", extracted_at="2026-01-01T00:00:00+00:00",
        location="Müpa Budapest nagyszínpad",
    )
    save_results("Budapest", "dance", [r], db)
    # Stale/non-existent ID — should fall through to LIKE fallback
    result = get_communities_for_venue(db, ["deadbeef1234"], "Müpa Budapest", "Budapest")
    assert len(result) == 1
    assert result[0]["name"] == "Tánc Csoport"
