from pathlib import Path
from scraper.db import (
    init_db, upsert_venues, upsert_persons,
    get_communities_for_venue,
)
from scraper.models import VenueRecord, PersonRecord
from scraper.store import save_results
from scraper.models import CommunityRecord
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


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


def test_venue_detail_page_returns_200(tmp_path):
    db = _db(tmp_path)
    v = _venue(name="Müpa Budapest", city="Budapest")
    upsert_venues(db, [v.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/budapest/helyszin/mupa-budapest")
        assert resp.status_code == 200
        assert "Müpa Budapest" in resp.text
    finally:
        app_state.db_path = old_db


def test_venue_detail_page_404_redirects(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get(
            "/budapest/helyszin/nem-letezik", follow_redirects=False
        )
        assert resp.status_code == 302
        assert resp.headers["location"] == "/helyszinek"
    finally:
        app_state.db_path = old_db


def test_person_detail_page_returns_200(tmp_path):
    db = _db(tmp_path)
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/budapest/ember/kovacs-janos")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "Futók" in resp.text
    finally:
        app_state.db_path = old_db


def test_person_detail_merges_multiple_communities(tmp_path):
    db = _db(tmp_path)
    for community in ["Futók", "Kerékpárosok"]:
        p = PersonRecord(
            name="Kovács János", role="leader", city="Budapest", topic="running",
            community_name=community, source_url="https://a.test",
            extracted_at="2026-01-01T00:00:00+00:00",
        )
        upsert_persons(db, [p.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/budapest/ember/kovacs-janos")
        assert resp.status_code == 200
        assert "Futók" in resp.text
        assert "Kerékpárosok" in resp.text
    finally:
        app_state.db_path = old_db


def test_person_detail_404_redirects(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get(
            "/budapest/ember/nem-letezik", follow_redirects=False
        )
        assert resp.status_code == 302
        assert resp.headers["location"] == "/emberek"
    finally:
        app_state.db_path = old_db


def test_venues_list_contains_detail_links(tmp_path):
    db = _db(tmp_path)
    v = _venue(name="Müpa Budapest", city="Budapest")
    upsert_venues(db, [v.model_dump()])

    old_db = app_state.db_path
    old_cities = app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", locale="hu", search_variants=[], country="Hungary")]
        resp = TestClient(web_app.app).get("/helyszinek")
        assert resp.status_code == 200
        assert "/budapest/helyszin/mupa-budapest" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_community_page_links_leader(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
        leader="Kovács János",
    )
    save_results("Budapest", "running", [r], db)

    old_db = app_state.db_path
    old_topics = app_state.topics
    old_cities = app_state.cities
    try:
        app_state.db_path = db
        app_state.topics = []
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/budapest/budapest-futok")
        assert resp.status_code == 200
        assert "/budapest/ember/kovacs-janos" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.topics = old_topics
        app_state.cities = old_cities


def test_emberek_page_lists_persons(tmp_path):
    db = _db(tmp_path)
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    old_db = app_state.db_path
    old_cities = app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        # People list is empty until a city is chosen (post-2026-07-26 rework).
        resp = TestClient(web_app.app).get("/emberek?city=Budapest")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "/budapest/ember/kovacs-janos" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
