from pathlib import Path
import pytest
from scraper.db import init_db, search_all
from scraper.store import save_results
from scraper.models import CommunityRecord, VenueRecord, PersonRecord
from scraper.db import upsert_venues, upsert_persons


@pytest.fixture
def db(tmp_path: Path) -> Path:
    """Initialize a test database."""
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_search_finds_community_by_name(db):
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    result = search_all(db, "Futók")
    assert len(result["communities"]) == 1
    assert result["communities"][0]["name"] == "Budapest Futók"


def test_search_finds_venue_by_name(db):
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"],
    )
    upsert_venues(db, [v.model_dump()])
    result = search_all(db, "Müpa")
    assert len(result["venues"]) == 1
    assert result["venues"][0]["name"] == "Müpa Budapest"


def test_search_finds_person_by_name(db):
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])
    result = search_all(db, "Kovács")
    assert len(result["persons"]) == 1
    assert result["persons"][0]["name"] == "Kovács János"


def test_search_empty_query_returns_empty(db):
    result = search_all(db, "")
    assert result == {"communities": [], "venues": [], "persons": []}


def test_search_no_match_returns_empty(db):
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    result = search_all(db, "xyznomatch")
    assert result == {"communities": [], "venues": [], "persons": []}


def test_search_case_insensitive(db):
    r = CommunityRecord(
        name="Budapest Running Group", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    result = search_all(db, "running")
    assert len(result["communities"]) == 1
    result = search_all(db, "RUNNING")
    assert len(result["communities"]) == 1


def test_search_partial_match(db):
    r = CommunityRecord(
        name="Budapest Futók Klub", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    result = search_all(db, "Futók")
    assert len(result["communities"]) == 1


def test_search_finds_description_match(db):
    r = CommunityRecord(
        name="Running Group", topic="running", city="Budapest", locale="hu",
        description="Futók csoportja",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    result = search_all(db, "csoportja")
    assert len(result["communities"]) == 1


def test_search_respects_limit(db):
    # Create 5 different communities with different names
    for i in range(5):
        r = CommunityRecord(
            name=f"Group {i}", topic=f"topic{i}", city="Budapest", locale="hu",
            source_url=f"https://test{i}.test", extracted_at="2026-01-01T00:00:00+00:00",
        )
        save_results("Budapest", f"topic{i}", [r], db)
    # Search for "Group" should find all 5, but limit=3 should return only 3
    result = search_all(db, "Group", limit=3)
    assert len(result["communities"]) == 3


def test_search_excludes_hidden_communities(db):
    # Save a community
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)

    # Mark it as hidden by updating the database directly
    from scraper.db import _connect
    with _connect(db) as conn:
        conn.execute("UPDATE communities SET hidden = 1 WHERE json_extract(data, '$.name') = ?", ("Budapest Futók",))
        conn.commit()

    result = search_all(db, "Futók")
    assert len(result["communities"]) == 0


def test_search_multiple_matches_across_types(db):
    # Add a community
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)

    # Add a venue
    v = VenueRecord(
        name="Budapest Sports Center", city="Budapest", locale="hu",
        source_url="https://bsc.test", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["running"],
    )
    upsert_venues(db, [v.model_dump()])

    # Add a person
    p = PersonRecord(
        name="Budapest János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    result = search_all(db, "Budapest")
    assert len(result["communities"]) == 1
    assert len(result["venues"]) == 1
    assert len(result["persons"]) == 1


def test_search_nonexistent_db_returns_empty(tmp_path):
    db_path = tmp_path / "nonexistent.db"
    result = search_all(db_path, "test")
    assert result == {"communities": [], "venues": [], "persons": []}
