"""Cross-domain canonical, thin-page noindex, and sitemap scoping.

HU-city pages are served with identical paths on both domains; kozossegek.com
is their canonical home (see 2026-06 deindexing post-mortem in docs/wiki).
"""
import re
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


def _canonical(html: str) -> str | None:
    m = re.search(r'<link rel="canonical" href="([^"]+)"', html)
    return m.group(1) if m else None


@pytest.fixture()
def seo_client(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [
        CommunityRecord(
            name="Zenei Kör", topic="music", city="Budapest", locale="hu",
            source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
            description="Aktív zenei közösség Budapesten, heti próbákkal.",
        ),
        CommunityRecord(
            name="Üres Klub", topic="music", city="Budapest", locale="hu",
            source_url="https://b.test", extracted_at="2026-01-01T00:00:00+00:00",
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


def test_hu_city_page_on_meetapedia_canonicalizes_to_kozossegek(seo_client):
    r = seo_client.get("/budapest", headers=MEET)
    assert _canonical(r.text) == "https://kozossegek.com/budapest"


def test_hu_city_page_on_kozossegek_self_canonical(seo_client):
    r = seo_client.get("/budapest", headers=KOZ)
    assert _canonical(r.text) == "https://kozossegek.com/budapest"


def test_non_hu_city_page_on_meetapedia_self_canonical(seo_client):
    r = seo_client.get("/stockholm", headers=MEET)
    assert _canonical(r.text) == "https://meetapedia.com/stockholm"


def test_hu_community_page_on_meetapedia_canonicalizes_to_kozossegek(seo_client):
    r = seo_client.get("/budapest/zenei-kor", headers=MEET)
    assert _canonical(r.text) == "https://kozossegek.com/budapest/zenei-kor"


def test_described_community_page_is_indexable(seo_client):
    r = seo_client.get("/budapest/zenei-kor", headers=KOZ)
    assert 'content="noindex"' not in r.text


def test_thin_community_page_is_noindexed(seo_client):
    for headers in (KOZ, MEET):
        r = seo_client.get("/budapest/ures-klub", headers=headers)
        assert 'content="noindex"' in r.text


def test_homepage_unaffected(seo_client):
    r = seo_client.get("/", headers=MEET)
    assert _canonical(r.text) == "https://meetapedia.com/"
    assert 'content="noindex"' not in r.text


def test_kozossegek_sitemap_lists_only_indexable_pages(seo_client):
    xml = seo_client.get("/sitemap.xml", headers=KOZ).text
    assert "kozossegek.com/budapest<" in xml
    assert "/budapest/zenei-kor" in xml
    assert "/budapest/ures-klub" not in xml


def test_meetapedia_sitemap_omits_hu_cities(seo_client):
    xml = seo_client.get("/sitemap.xml", headers=MEET).text
    assert "budapest" not in xml
    assert "meetapedia.com/stockholm" in xml


def test_meetapedia_home_title_is_english(seo_client):
    """SEO regression: the intl site's <title> was hardcoded Hungarian once."""
    r = seo_client.get("/", headers=MEET)
    assert "Find your community" in r.text
    assert "Találd meg a közösséged" not in r.text


def test_submit_page_english_on_meetapedia(seo_client):
    """Regression: the submit page was fully hardcoded Hungarian on both sites."""
    r = seo_client.get("/submit-community", headers=MEET)
    assert r.status_code == 200
    assert "Submit a community" in r.text
    assert "Küldd be" not in r.text
    assert "Válassz várost" not in r.text


def test_submit_page_hungarian_on_kozossegek(seo_client):
    r = seo_client.get("/kozosseg-bekuldes", headers=KOZ)
    assert r.status_code == 200
    assert "Közösség beküldése" in r.text
    assert "Válassz várost" in r.text


def test_country_page_path_based(seo_client):
    """Home country headings link to the SEO path form /cities/<slug>."""
    r = seo_client.get("/cities/sweden", headers=MEET)
    assert r.status_code == 200
    assert "Stockholm" in r.text
    assert "Budapest" not in r.text
    assert "magyar város" not in r.text  # count line must be i18n'd
    assert _canonical(r.text) == "https://meetapedia.com/cities/sweden"


def test_country_query_param_redirects_to_path(seo_client):
    r = seo_client.get("/cities?country=Sweden", headers=MEET, follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == "/cities/sweden"


def test_unknown_country_slug_redirects_to_cities(seo_client):
    r = seo_client.get("/cities/narnia", headers=MEET, follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/cities"


def test_meetapedia_sitemap_lists_country_pages(seo_client):
    xml = seo_client.get("/sitemap.xml", headers=MEET).text
    assert "meetapedia.com/cities/sweden" in xml
    assert "/cities/hungary" not in xml  # HU content is kozossegek-canonical
