from pathlib import Path
from scraper.db import init_db, upsert_venues
from scraper.models import VenueRecord
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_suggest_edit_venue_wrong_info(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).post("/suggest-edit", data={
            "entity_type": "venue",
            "entity_id": "abc123",
            "entity_name": "Müpa Budapest",
            "entity_city": "Budapest",
            "entity_topic": "",
            "record_key": "",
            "change_type": "wrong_info",
            "new_value": "",
            "notes": "Rossz telefonszám van megadva",
            "email": "test@example.com",
        })
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
    finally:
        app_state.db_path = old_db


def test_suggest_edit_venue_name_correction_requires_new_value(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).post("/suggest-edit", data={
            "entity_type": "venue",
            "entity_id": "abc123",
            "entity_name": "Müpa Budapest",
            "entity_city": "Budapest",
            "entity_topic": "",
            "record_key": "",
            "change_type": "name_correction",
            "new_value": "",
            "notes": "Helytelen a név",
            "email": "test@example.com",
        })
        assert resp.json()["ok"] is False
        assert resp.json()["error"] == "missing_new_value"
    finally:
        app_state.db_path = old_db


def test_suggest_edit_venue_name_correction_with_value(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).post("/suggest-edit", data={
            "entity_type": "venue",
            "entity_id": "abc123",
            "entity_name": "Müpa Budapest",
            "entity_city": "Budapest",
            "entity_topic": "",
            "record_key": "",
            "change_type": "name_correction",
            "new_value": "Müpa Nemzeti Hangverseny- és Kongresszusi Központ",
            "notes": "Teljes oficial név",
            "email": "test@example.com",
        })
        assert resp.json()["ok"] is True
    finally:
        app_state.db_path = old_db


def test_suggest_edit_venue_rejects_community_change_type(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).post("/suggest-edit", data={
            "entity_type": "venue",
            "entity_id": "abc123",
            "entity_name": "Test Venue",
            "entity_city": "Budapest",
            "entity_topic": "",
            "record_key": "",
            "change_type": "wrong_city",  # community-only type
            "new_value": "",
            "notes": "test",
            "email": "test@example.com",
        })
        assert resp.json()["ok"] is False
        assert resp.json()["error"] == "invalid_change_type"
    finally:
        app_state.db_path = old_db
