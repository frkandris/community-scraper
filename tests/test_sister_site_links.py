"""Cross-domain twin links (Wikipedia's "also available in …") and footer links.

kozossegek.com is the Hungarian edition of meetapedia.com. Every kozossegek page
has a twin; only Hungarian-city content has one in the other direction, because
kozossegek.com bounces foreign cities to its home page.
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


def test_community_page_links_its_twin_both_ways(client):
    """The twin link lives on the community record card, and nowhere else."""
    koz = client.get("/budapest/zenei-kor", headers=KOZ)
    assert "https://meetapedia.com/budapest/zenei-kor" in koz.text
    assert "ph-translate" in koz.text

    meet = client.get("/budapest/zenei-kor", headers=MEET)
    assert "https://kozossegek.com/budapest/zenei-kor" in meet.text
    assert "ph-translate" in meet.text


def test_foreign_community_page_has_no_twin(client):
    """kozossegek.com redirects /stockholm home — never link a visitor there."""
    r = client.get("/stockholm/stockholm-runners", headers=MEET)
    assert r.status_code == 200
    assert "https://kozossegek.com/stockholm" not in r.text
    assert "ph-translate" not in r.text


def test_no_twin_link_outside_community_pages(client):
    """Home, city, explore, about: no language notice — it was too prominent."""
    for path, headers in (("/", KOZ), ("/", MEET), ("/budapest", KOZ),
                          ("/budapest", MEET), ("/felfedezes", KOZ), ("/rolunk", KOZ)):
        r = client.get(path, headers=headers)
        assert "ph-translate" not in r.text, f"{path} still renders the language icon"
        assert "angolul is elérhető" not in r.text, path
        assert "available in Hungarian on" not in r.text, path


def test_footer_links_the_sister_home_page_both_ways(client):
    koz = client.get("/stockholm", headers=MEET)  # no page twin, footer still links
    assert "https://kozossegek.com" in koz.text
    assert "Hungarian edition" in koz.text

    meet = client.get("/", headers=KOZ)
    assert "https://meetapedia.com" in meet.text
    assert "projekt része" in meet.text


# ── About page: author, open source, sister project ─────────────────────────

def test_about_page_credits_the_author_and_repo(client):
    for headers in (KOZ, MEET):
        r = client.get("/rolunk", headers=headers)
        assert r.status_code == 200
        assert "P. Tóth András" in r.text
        assert "https://www.linkedin.com/in/ptothandras/" in r.text
        assert "https://github.com/frkandris/meetapedia" in r.text


def test_about_page_names_the_sister_edition(client):
    koz = client.get("/rolunk", headers=KOZ)
    assert "meetapedia.com magyar kiadása" in koz.text

    meet = client.get("/rolunk", headers=MEET)
    assert "Hungarian edition of this project" in meet.text
