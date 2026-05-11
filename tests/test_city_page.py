from pathlib import Path
from scraper.db import init_db, upsert_venues, upsert_persons
from scraper.models import VenueRecord, PersonRecord
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_city_page_shows_venue_link(tmp_path):
    db = _db(tmp_path)
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"],
    )
    upsert_venues(db, [v.model_dump()])

    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/budapest")
        assert resp.status_code == 200
        assert "/budapest/helyszin/mupa-budapest" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_city_page_shows_person_link(tmp_path):
    db = _db(tmp_path)
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/budapest")
        assert resp.status_code == 200
        assert "/budapest/ember/kovacs-janos" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_city_page_deduplicates_persons(tmp_path):
    db = _db(tmp_path)
    for community in ["Futók", "Kerékpárosok"]:
        p = PersonRecord(
            name="Kovács János", role="leader", city="Budapest", topic="running",
            community_name=community, source_url="https://a.test",
            extracted_at="2026-01-01T00:00:00+00:00",
        )
        upsert_persons(db, [p.model_dump()])

    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/budapest")
        assert resp.status_code == 200
        # Person appears once — count occurrences of the detail link
        assert resp.text.count("/budapest/ember/kovacs-janos") == 1
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
