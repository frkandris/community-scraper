"""Breadcrumbs: visible nav + BreadcrumbList JSON-LD (SEO hierarchy signal)."""
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.models import CommunityRecord
from scraper.pipeline import CityConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.schema import breadcrumb_jsonld
from scraper.web.state import app_state

KOZ = {"host": "kozossegek.com"}


def test_breadcrumb_jsonld_shape():
    out = breadcrumb_jsonld([
        {"name": "Home", "url": "https://x/"},
        {"name": "Budapest", "url": "https://x/budapest"},
        {"name": "Zenei Kör", "url": "https://x/budapest/zenei-kor"},
    ])
    data = json.loads(out)
    assert data["@type"] == "BreadcrumbList"
    assert [e["position"] for e in data["itemListElement"]] == [1, 2, 3]
    assert data["itemListElement"][1]["name"] == "Budapest"


def test_breadcrumb_jsonld_needs_two_items():
    assert breadcrumb_jsonld([{"name": "Home", "url": "https://x/"}]) == ""
    assert breadcrumb_jsonld([]) == ""


@pytest.fixture()
def client(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [CommunityRecord(
        name="Zenei Kör", topic="music", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
        description="Aktív zenei közösség Budapesten, heti próbákkal.",
    )], db)
    old_db, old_cities = app_state.db_path, app_state.cities
    app_state.db_path = db
    app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
    try:
        yield TestClient(web_app.app)
    finally:
        app_state.db_path, app_state.cities = old_db, old_cities


def test_community_page_emits_breadcrumb_jsonld(client):
    # The base template emits only the BreadcrumbList JSON-LD (no visible base nav —
    # each page keeps its own visible bar). This is the SEO deliverable.
    r = client.get("/budapest/zenei-kor", headers=KOZ)
    assert r.status_code == 200
    assert "BreadcrumbList" in r.text
    # hierarchy: Home → Budapest → (current) Zenei Kör
    assert '"position": 1' in r.text and '"position": 3' in r.text
    assert '"name": "Budapest"' in r.text
