"""Home-page city search: the mobile-broken <datalist> is gone for good.

Regression guard for the 2026-08-16 report that the kozossegek.com home search
"did not work on mobile and could not find Szentendre". Two independent causes:
iOS Safari ignores <datalist> entirely, and the submit handler hard-blocked any
value that was not a byte-exact (case-folded) city name.
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state

KOZ = {"host": "kozossegek.com"}


@pytest.fixture()
def client(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    app_state.db_path = db
    app_state.cities = [
        CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[]),
        CityConfig(name="Szentendre", country="Hungary", locale="hu", search_variants=[]),
        CityConfig(name="Győr", country="Hungary", locale="hu", search_variants=[]),
    ]
    web_app._home_stats_cache.clear()
    try:
        yield TestClient(web_app.app)
    finally:
        app_state.db_path, app_state.cities = old_db, old_cities
        web_app._home_stats_cache.clear()


def test_home_has_no_datalist(client):
    # iOS Safari never implemented <datalist>; its presence means the mobile
    # regression is back.
    html = client.get("/", headers=KOZ).text
    assert "<datalist" not in html
    assert 'list="cities-list"' not in html


def test_home_loads_shared_widget_script(client):
    html = client.get("/", headers=KOZ).text
    assert "/static/js/listing.js" in html
    assert "MpAutocomplete.attach" in html


def test_city_data_carries_every_site_city(client):
    html = client.get("/", headers=KOZ).text
    # One compact list feeds both the search box and the nearby-cities panel;
    # they used to ship as two blobs of the same cities under different keys.
    assert "const _CITY_DATA = [" in html
    assert "const cityOptions = _CITY_DATA.map" in html
    assert '"n":"Szentendre"' in html      # the city the report named
    assert "Győr" in html                  # non-ASCII survives ensure_ascii=False
    assert "cities_coords_json" not in html and "city_options_json" not in html


def test_submit_handler_resolves_instead_of_blocking(client):
    html = client.get("/", headers=KOZ).text
    # The old handler compared lowercased strings and preventDefault()ed on any
    # mismatch. The new one asks the widget to resolve accents/partials first.
    assert "cityAc.resolve(val)" in html
    assert "cityOptions.some(" not in html


def test_form_posts_to_locale_aware_explore_url(client):
    html = client.get("/", headers=KOZ).text
    assert '<form action="/felfedezes" method="GET" id="main-form"' in html
    html_en = client.get("/", headers={"host": "meetapedia.com"}).text
    assert '<form action="/explore" method="GET" id="main-form"' in html_en


def test_static_js_is_served(client):
    resp = client.get("/static/js/listing.js")
    assert resp.status_code == 200
    assert "MpAutocomplete" in resp.text
    assert "MpListFilter" in resp.text
