"""The acquisition funnel: that it is recorded, and that it is readable.

Two things were true before these tests. A claim — the strongest signal the
public site produces — was emailed and never stored, so with no mail key set it
vanished while the visitor was told "ok". And every other stage of the funnel
was in the database but behind the admin password, so nobody driving the
project from a terminal could see whether anything converted at all.
"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from scraper.db import get_funnel_counts, init_db, record_pageview, save_subscription
from scraper.models import CommunityRecord
from scraper.pipeline import CityConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state

KOZ = {"host": "kozossegek.com"}


@pytest.fixture()
def funnel_db(tmp_path: Path, monkeypatch):
    db = tmp_path / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [
        CommunityRecord(
            name="Zenei Kör", topic="music", city="Budapest", locale="hu",
            source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
            description="Aktív zenei közösség.", email="kor@example.test",
            website="https://kor.example.test",
        ),
        CommunityRecord(
            name="Néma Klub", topic="music", city="Budapest", locale="hu",
            source_url="https://b.test", extracted_at="2026-01-01T00:00:00+00:00",
        ),
    ], db)
    old_db, old_cities = app_state.db_path, app_state.cities
    app_state.db_path = db
    app_state.cities = [
        CityConfig(name="Budapest", country="Hungary", locale="hu",
                   search_variants=["Budapest"]),
    ]
    monkeypatch.setenv("ROUTER_API_KEY", "funnel-key")
    monkeypatch.setattr(web_app, "_RESEND_API_KEY", "", raising=False)
    try:
        yield db
    finally:
        app_state.db_path, app_state.cities = old_db, old_cities


def test_an_empty_database_reports_zeroes_not_an_error(tmp_path: Path):
    """A funnel that raises on a fresh install is a funnel nobody wires up."""
    counts = get_funnel_counts(tmp_path / "missing.db")
    assert counts["subscriptions_total"] == 0
    assert counts["records"] == 0


def test_the_funnel_counts_each_stage(funnel_db):
    record_pageview(funnel_db, "2026-08-21", "kozossegek", "visitor-a")
    record_pageview(funnel_db, "2026-08-21", "kozossegek", "visitor-a")
    record_pageview(funnel_db, "2026-08-21", "kozossegek", "visitor-b")
    save_subscription(funnel_db, "reader@example.test", "Budapest", "music")
    save_subscription(funnel_db, "reader@example.test", "Budapest", "sport")
    save_subscription(funnel_db, "other@example.test", "Budapest", "music")

    counts = get_funnel_counts(funnel_db, days=365)
    assert counts["pageviews"] == 3
    assert counts["visitors"] == 2
    # Three rows, two people. A mail goes to a person, so both are reported.
    assert counts["subscriptions_total"] == 3
    assert counts["subscribers_total"] == 2
    assert counts["records"] == 2
    assert counts["records_with_email"] == 1
    assert counts["records_with_website"] == 1


def test_a_claim_survives_without_a_mail_provider(funnel_db):
    """The failure this test exists for: no RESEND_API_KEY, claim silently lost."""
    client = TestClient(web_app.app)
    r = client.post("/claim-community", data={
        "community_id": "abc123",
        "community_name": "Zenei Kör",
        "city": "Budapest",
        "page_url": "https://kozossegek.com/budapest/zenei-kor",
        "claimant_email": "leader@example.test",
    }, headers=KOZ)
    assert r.json()["ok"] is True

    counts = get_funnel_counts(funnel_db, days=365)
    assert counts["claims_total"] == 1
    # A claim is not a correction; counting them together hides both.
    assert counts["edit_requests_total"] == 0


def test_a_claim_without_an_email_is_rejected_and_not_stored(funnel_db):
    client = TestClient(web_app.app)
    r = client.post("/claim-community", data={
        "community_name": "Zenei Kör", "claimant_email": "",
    }, headers=KOZ)
    assert r.json()["ok"] is False
    assert get_funnel_counts(funnel_db, days=365)["claims_total"] == 0


def test_the_funnel_endpoint_needs_a_key(funnel_db):
    client = TestClient(web_app.app)
    assert client.get("/v1/funnel").status_code == 401
    r = client.get("/v1/funnel", headers={"Authorization": "Bearer funnel-key"})
    assert r.status_code == 200
    assert r.json()["object"] == "funnel"


def test_the_window_is_bounded(funnel_db):
    """`days` reaches a date() call; an unbounded one is worth refusing early."""
    client = TestClient(web_app.app)
    r = client.get("/v1/funnel?days=99999", headers={"Authorization": "Bearer funnel-key"})
    assert r.status_code == 200
    assert r.json()["days"] == 365


def test_the_report_carries_the_funnel(funnel_db):
    """The report is the only thing read every day; a metric outside it is unread."""
    from scraper.report import build_report_html

    counts = get_funnel_counts(funnel_db, days=30)
    counts["claims_total"] = counts["claims"] = 7
    summary = {
        "hu": {k: 0 for k in ("new_communities", "changed_communities", "change_rows",
                              "new_venues", "new_persons", "pages_scraped",
                              "pages_extracted", "searches")},
        "intl": {k: 0 for k in ("new_communities", "changed_communities", "change_rows",
                                "new_venues", "new_persons", "pages_scraped",
                                "pages_extracted", "searches")},
        "totals": {"hu": 0, "intl": 0, "covered_pairs_hu": 0, "covered_pairs_intl": 0},
        "runs": [], "providers": [],
    }
    _, html = build_report_html("2026-08-21", summary, {}, None, counts)
    assert "Vevőszerzés" in html
    assert "Közösség igénylés" in html
    assert ">7<" in html


def test_the_report_survives_a_missing_funnel(funnel_db):
    from scraper.report import build_report_html

    summary = {
        "hu": {k: 0 for k in ("new_communities", "changed_communities", "change_rows",
                              "new_venues", "new_persons", "pages_scraped",
                              "pages_extracted", "searches")},
        "intl": {k: 0 for k in ("new_communities", "changed_communities", "change_rows",
                                "new_venues", "new_persons", "pages_scraped",
                                "pages_extracted", "searches")},
        "totals": {"hu": 0, "intl": 0, "covered_pairs_hu": 0, "covered_pairs_intl": 0},
        "runs": [], "providers": [],
    }
    _, html = build_report_html("2026-08-21", summary, {}, None, None)
    assert "Vevőszerzés" not in html
    assert "Változások" in html
