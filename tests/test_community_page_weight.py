"""The community page must stay mostly about the community.

Measured on the live page on 2026-08-21: 231 KB, 3,922 `<option>` elements,
176 KB of them — 76% of the document — and 5,089 words of text of which 510
were the community. The rest was a city dropdown, byte-identical across all
42,091 community pages, nested inside two `hidden` divs where no visitor ever
saw it. That is what a near-duplicate looks like to a crawler, and 23,461 of
these pages are sitting in Google's "Crawled – currently not indexed".

CLAUDE.md already forbids this ("Never server-render large lists") for the
admin templates. It was never enforced on the page that has 42,091 instances.
"""
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.models import CommunityRecord
from scraper.pipeline import CityConfig, TopicConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state

KOZ = {"host": "kozossegek.com"}
MEET = {"host": "meetapedia.com"}

#: Enough cities that a regression is unmistakable; production carries ~3,900.
_MANY = 400


@pytest.fixture()
def weighty_client(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [
        CommunityRecord(
            name="Zenei Kör", topic="music", city="Budapest", locale="hu",
            source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
            description="Aktív zenei közösség Budapesten.",
        ),
    ], db)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    app_state.db_path = db
    app_state.cities = [
        CityConfig(name="Budapest", country="Hungary", locale="hu",
                   search_variants=["Budapest"]),
    ] + [
        CityConfig(name=f"Filler{i:04d}", country="Hungary", locale="hu",
                   search_variants=[f"Filler{i:04d}"])
        for i in range(_MANY)
    ] + [
        CityConfig(name="Stockholm", country="Sweden", locale="sv",
                   search_variants=["Stockholm"]),
    ]
    app_state.topics = [TopicConfig(name="music", search_terms={})]
    try:
        yield TestClient(web_app.app)
    finally:
        app_state.db_path = old_db
        app_state.cities, app_state.topics = old_cities, old_topics


def _options(html: str) -> int:
    return len(re.findall(r"<option", html))


def test_the_city_list_is_not_inlined_into_the_community_page(weighty_client):
    r = weighty_client.get("/budapest/zenei-kor", headers=KOZ)
    assert r.status_code == 200
    # 400 filler cities are configured. If any of them reach the document, the
    # list is being server-rendered again.
    assert "Filler0100" not in r.text
    # What is left: the problem-type picker and the topic picker. Well under a
    # hundred either way; a city list would blow past it immediately.
    assert _options(r.text) < 60


def test_the_page_is_mostly_the_community(weighty_client):
    """A page whose text is 90% boilerplate shared with 42,090 others."""
    html = weighty_client.get("/budapest/zenei-kor", headers=KOZ).text
    selects = re.findall(r"<select.*?</select>", html, re.S)
    select_bytes = sum(len(s) for s in selects)
    assert select_bytes < len(html) * 0.10


def test_the_picker_still_has_a_source_of_cities(weighty_client):
    """Removing the options must not remove the feature."""
    html = weighty_client.get("/budapest/zenei-kor", headers=KOZ).text
    assert 'data-cities-url="/api/cities.json"' in html
    assert "loadCities()" in html


def test_the_cities_endpoint_serves_the_list(weighty_client):
    r = weighty_client.get("/api/cities.json", headers=KOZ)
    assert r.status_code == 200
    names = r.json()["cities"]
    assert "Budapest" in names
    assert "Filler0100" in names
    assert "max-age" in r.headers.get("cache-control", "")


def test_the_cities_endpoint_is_scoped_to_the_site(weighty_client):
    """It replaced a server-rendered list that was already site-scoped."""
    assert "Stockholm" not in weighty_client.get("/api/cities.json", headers=KOZ).json()["cities"]
    assert "Stockholm" in weighty_client.get("/api/cities.json", headers=MEET).json()["cities"]


def test_the_cities_endpoint_is_not_counted_as_a_pageview(weighty_client):
    """It fires per visitor interaction; counting it would inflate traffic."""
    from scraper.db import get_funnel_counts

    weighty_client.get("/api/cities.json", headers=KOZ)
    assert get_funnel_counts(app_state.db_path, days=365)["pageviews"] == 0
