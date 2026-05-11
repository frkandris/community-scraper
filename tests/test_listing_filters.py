from pathlib import Path
from scraper.db import init_db, upsert_persons, upsert_venues
from scraper.models import PersonRecord, VenueRecord
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _cities():
    return [
        CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[]),
        CityConfig(name="Debrecen", country="Hungary", locale="hu", search_variants=[]),
    ]


def _setup_persons(db):
    upsert_persons(db, [
        PersonRecord(name="Kovács János", role="leader", city="Budapest", topic="running",
                     community_name="Futók", source_url="https://a.test",
                     extracted_at="2026-01-01T00:00:00+00:00").model_dump(),
        PersonRecord(name="Nagy Éva", role="organizer", city="Budapest", topic="cycling",
                     community_name="Kerékpárosok", source_url="https://a.test",
                     extracted_at="2026-01-01T00:00:00+00:00").model_dump(),
        PersonRecord(name="Szabó Péter", role="coach", city="Debrecen", topic="running",
                     community_name="Futók DE", source_url="https://a.test",
                     extracted_at="2026-01-01T00:00:00+00:00").model_dump(),
    ])


def test_people_city_filter(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek?city=Budapest")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "Szabó Péter" not in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_role_filter(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek?role=leader")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "Nagy Éva" not in resp.text
        assert "Szabó Péter" not in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_filter_options_from_full_set(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek?city=Budapest")
        # Debrecen must still appear as a filter option even when filtered out
        assert "Debrecen" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_page_search_structure(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek")
        assert "people-search" in resp.text
        assert "data-name=" in resp.text
        assert "data-city-section" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_role_displayed_in_hungarian(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek")
        assert "vezető" in resp.text      # "leader" → "vezető"
        assert "szervező" in resp.text    # "organizer" → "szervező"
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_venues_page_has_name_search_structure(tmp_path):
    db = _db(tmp_path)
    upsert_venues(db, [
        VenueRecord(name="Müpa Budapest", city="Budapest", locale="hu",
                    source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
                    welcomed_topics=["music"]).model_dump(),
    ])
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/helyszinek")
        assert resp.status_code == 200
        assert "venue-search" in resp.text
        assert "data-name=" in resp.text
        assert "data-city-section" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
