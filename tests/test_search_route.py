from pathlib import Path
from scraper.db import init_db
from scraper.store import save_results
from scraper.models import CommunityRecord
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_search_route_empty_query(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/kereses")
        assert resp.status_code == 200
        assert 'action="/kereses"' in resp.text
    finally:
        app_state.db_path = old_db


def test_search_route_returns_community(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/kereses?q=Fut%C3%B3k")
        assert resp.status_code == 200
        assert "Budapest Futók" in resp.text
    finally:
        app_state.db_path = old_db
