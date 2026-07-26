"""Community pages offer a copyable backlink badge (encourages inbound links)."""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.identity import public_slug as _slugify
from scraper.models import CommunityRecord
from scraper.pipeline import CityConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state

KOZ = {"host": "kozossegek.com"}


@pytest.fixture()
def client(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [CommunityRecord(
        name="Zenei Kör", topic="music", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
        description="Aktív zenei közösség Budapesten.",
    )], db)
    old_db, old_cities = app_state.db_path, app_state.cities
    app_state.db_path = db
    app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
    try:
        yield TestClient(web_app.app)
    finally:
        app_state.db_path, app_state.cities = old_db, old_cities


def test_community_page_has_backlink_badge(client):
    html = client.get("/budapest/zenei-kor", headers=KOZ).text
    assert 'id="badge-code"' in html
    # the snippet is built client-side from data-* attrs pointing at the canonical URL
    assert 'data-url="https://kozossegek.com/budapest/zenei-kor"' in html
    assert 'data-name="Zenei Kör"' in html


def test_badge_escapes_malicious_community_name(tmp_path: Path):
    # A hostile name must never appear unescaped in the page (Jinja escapes the
    # data-* attribute; the JS builder re-escapes into the copied snippet).
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [CommunityRecord(
        name="Bad</a><script>alert(1)</script>", topic="music", city="Budapest",
        locale="hu", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00", description="x")], db)
    old_db, old_cities = app_state.db_path, app_state.cities
    app_state.db_path = db
    app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
    try:
        html = TestClient(web_app.app).get(
            f"/budapest/{_slugify('Bad</a><script>alert(1)</script>')}", headers=KOZ).text
        assert "<script>alert(1)</script>" not in html
    finally:
        app_state.db_path, app_state.cities = old_db, old_cities
