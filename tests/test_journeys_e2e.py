"""End-to-end journeys through the public site.

Deliberately few. "Due to their high maintenance cost you should aim to reduce
the number of end-to-end tests to a bare minimum" — these cover the journeys
that carry the product's value, and everything else is tested a level down
(martinfowler.com/articles/practical-test-pyramid.html).

Every assertion goes through a page object rather than touching markup, so a
redesign changes one class and not twenty tests
(martinfowler.com/bliki/PageObject.html).
"""
import re

import pytest
from fastapi.testclient import TestClient

from scraper.db import init_db
from scraper.models import CommunityRecord
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state


# ── page objects ─────────────────────────────────────────────────────────────

class Page:
    """A fetched page, asked questions in the site's own terms."""

    def __init__(self, response):
        self.status = response.status_code
        self.html = response.text

    def _text(self) -> str:
        body = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", self.html,
                      flags=re.S | re.I)
        return " ".join(re.sub(r"<[^>]+>", " ", body).split())

    @property
    def title(self) -> str:
        m = re.search(r"<title>(.*?)</title>", self.html, re.S)
        return " ".join(m.group(1).split()) if m else ""

    @property
    def canonical(self) -> str:
        m = re.search(r'<link rel="canonical" href="(.*?)"', self.html)
        return m.group(1) if m else ""

    @property
    def is_indexable(self) -> bool:
        return 'name="robots" content="noindex"' not in self.html

    def links_to(self, path: str) -> bool:
        return f'href="{path}"' in self.html

    def mentions(self, text: str) -> bool:
        return text in self._text()

    def outbound_paths(self) -> set[str]:
        return set(re.findall(r'href="(/[^"#?]*)"', self.html))


class Site:
    """The public site, navigated the way a visitor would."""

    def __init__(self, client: TestClient, host: str = "kozossegek.com"):
        self._client, self._host = client, host

    def visit(self, path: str) -> Page:
        return Page(self._client.get(path, headers={"host": self._host},
                                     follow_redirects=True))


# ── fixture ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def site(tmp_path, monkeypatch):
    db = tmp_path / "scraper.db"
    init_db(db)

    def _rec(name, topic, city="Szentendre", desc="Egy valódi közösség leírása."):
        return CommunityRecord(
            name=name, topic=topic, city=city, locale="hu",
            source_url=f"https://example.test/{name}", description=desc,
            extracted_at="2026-08-01T00:00:00+00:00")

    save_results("Szentendre", "running",
                 [_rec("Szentendrei Futóklub", "running"),
                  _rec("Duna-parti Futók", "running")], db)
    save_results("Szentendre", "chess", [_rec("Szentendrei Sakk Kör", "chess")], db)

    # The public routes read the configured city and topic lists, not the
    # database — a city with records but no entry is not a city.
    from scraper.pipeline import CityConfig, TopicConfig

    old = (app_state.db_path, app_state.cities, app_state.topics)
    app_state.db_path = db
    app_state.cities = [CityConfig(name="Szentendre", locale="hu",
                                   search_variants=["Szentendre"], country="Hungary")]
    app_state.topics = [TopicConfig(name=n, search_terms={"hu": [n]})
                        for n in ("running", "chess")]
    try:
        yield Site(TestClient(web_app.app))
    finally:
        app_state.db_path, app_state.cities, app_state.topics = old


# ── the journeys ─────────────────────────────────────────────────────────────

def test_a_visitor_can_reach_a_community_from_its_city(site):
    """The journey the whole site exists for: city → topic → community."""
    city = site.visit("/szentendre")
    assert city.status == 200
    assert city.mentions("Szentendre")

    community = site.visit("/szentendre/szentendrei-futoklub")
    assert community.status == 200
    assert community.mentions("Szentendrei Futóklub")
    assert community.canonical.endswith("/szentendre/szentendrei-futoklub")


def test_a_community_page_leads_on_to_its_neighbours(site):
    """The fix for 23,461 pages that were crawled and not indexed.

    A page that links nowhere is a dead end for a reader and for a crawler.
    """
    page = site.visit("/szentendre/szentendrei-futoklub")

    assert page.links_to("/szentendre/duna-parti-futok"), "no sibling in the same topic"
    assert page.links_to("/szentendre/szentendrei-sakk-kor"), "no other topic in the city"
    assert page.links_to("/szentendre"), "no way back to the city"
    assert not page.links_to("/szentendre/szentendrei-futoklub"), "links to itself"


def test_a_described_community_is_offered_to_search_engines(site):
    page = site.visit("/szentendre/szentendrei-futoklub")
    assert page.is_indexable
    assert "Szentendrei Futóklub" in page.title


def test_every_link_a_community_page_offers_resolves(site):
    """A crawl path is only a path if the links work."""
    page = site.visit("/szentendre/szentendrei-futoklub")
    internal = [p for p in page.outbound_paths()
                if not p.startswith(("/static", "/admin", "/api", "/v1"))]
    assert internal, "the page offers no internal links at all"

    broken = [p for p in sorted(internal) if site.visit(p).status >= 400]
    assert not broken, f"dead links: {broken}"


def test_a_community_without_a_description_is_still_offered_to_search_engines(site, tmp_path):
    """68% of the corpus has no long description and used to exclude itself.

    The `noindex` made sense when such a page was a name and nothing else. It
    now lists its neighbours, which is content and a crawl path — and hiding
    23,461 pages from search was never going to get them indexed.
    """
    from scraper.models import CommunityRecord
    from scraper.store import save_results
    from scraper.web.state import app_state

    save_results("Szentendre", "running", [CommunityRecord(
        name="Leírás Nélküli Klub", topic="running", city="Szentendre", locale="hu",
        source_url="https://example.test/x", description="",
        extracted_at="2026-08-01T00:00:00+00:00")], app_state.db_path)

    page = site.visit("/szentendre/leiras-nelkuli-klub")
    assert page.status == 200
    assert page.is_indexable
    # And it is not a dead end: it reaches the rest of the city.
    assert page.links_to("/szentendre/szentendrei-sakk-kor")
