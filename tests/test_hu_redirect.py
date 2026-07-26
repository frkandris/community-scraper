"""meetapedia.com 301-redirects Hungarian-city pages to kozossegek.com.

GSC (2026-07) showed the rel=canonical hint was ignored: meetapedia kept the HU
duplicates indexed and won 551 HU impressions to kozossegek's 33, while kozossegek
was being deindexed. A hard 301 removes the duplicate so Google consolidates HU to
its intended home. Non-Hungarian cities (meetapedia's own market) are untouched, and
kozossegek never redirects its own HU pages.
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.models import CommunityRecord
from scraper.pipeline import CityConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state

MEET = {"host": "meetapedia.com"}
KOZ = {"host": "kozossegek.com"}


@pytest.fixture()
def client(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [
        CommunityRecord(
            name="Zenei Kör", topic="music", city="Budapest", locale="hu",
            source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
            description="Aktív zenei közösség Budapesten, heti próbákkal.",
        ),
    ], db)
    save_results("Stockholm", "running", [
        CommunityRecord(
            name="Stockholm Runners", topic="running", city="Stockholm", locale="sv",
            source_url="https://c.test", extracted_at="2026-01-01T00:00:00+00:00",
            description="Weekly running group in Stockholm.",
        ),
    ], db)
    old_db, old_cities = app_state.db_path, app_state.cities
    app_state.db_path = db
    app_state.cities = [
        CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=["Budapest"]),
        CityConfig(name="Stockholm", country="Sweden", locale="sv", search_variants=["Stockholm"]),
    ]
    try:
        yield TestClient(web_app.app)
    finally:
        app_state.db_path, app_state.cities = old_db, old_cities


def test_meetapedia_hu_community_301s_to_kozossegek(client):
    r = client.get("/budapest/zenei-kor", headers=MEET, follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == "https://kozossegek.com/budapest/zenei-kor"


def test_meetapedia_hu_city_page_301s(client):
    r = client.get("/budapest", headers=MEET, follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == "https://kozossegek.com/budapest"


def test_meetapedia_hu_redirect_preserves_query(client):
    r = client.get("/budapest/music?subscribed=1", headers=MEET, follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == "https://kozossegek.com/budapest/music?subscribed=1"


def test_meetapedia_non_hu_city_is_not_redirected(client):
    # Stockholm is meetapedia's own market — must render, not redirect.
    r = client.get("/stockholm/stockholm-runners", headers=MEET, follow_redirects=False)
    assert r.status_code == 200


def test_kozossegek_hu_page_is_not_redirected(client):
    r = client.get("/budapest/zenei-kor", headers=KOZ, follow_redirects=False)
    assert r.status_code == 200
