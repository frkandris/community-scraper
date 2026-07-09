"""Daily report email: traffic counter, summary scopes, HTML builder, middleware."""
from pathlib import Path

from scraper.db import (
    get_daily_summary,
    get_traffic_for_day,
    init_db,
    record_pageview,
)
from scraper.models import CommunityRecord
from scraper.report import build_report_html
from scraper.store import save_results


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_pageview_counter_and_uniques(tmp_path):
    db = _db(tmp_path)
    record_pageview(db, "2026-07-09", "kozossegek", "visitor-a")
    record_pageview(db, "2026-07-09", "kozossegek", "visitor-a")  # same visitor
    record_pageview(db, "2026-07-09", "kozossegek", "visitor-b")
    record_pageview(db, "2026-07-09", "meetapedia", "visitor-a")
    t = get_traffic_for_day(db, "2026-07-09")
    assert t["kozossegek"] == {"pageviews": 3, "visitors": 2}
    assert t["meetapedia"] == {"pageviews": 1, "visitors": 1}
    assert get_traffic_for_day(db, "2026-07-08") == {}


def test_daily_summary_scopes(tmp_path):
    db = _db(tmp_path)
    save_results("Budapest", "running", [CommunityRecord(
        name="Futó Kör", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00")], db)
    save_results("Stockholm", "running", [CommunityRecord(
        name="Sthlm Runners", topic="running", city="Stockholm", locale="sv",
        source_url="https://b.test", extracted_at="2026-01-01T00:00:00+00:00")], db)

    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=1)).isoformat()
    end = (now + timedelta(hours=1)).isoformat()
    s = get_daily_summary(db, start, end, hu_cities={"Budapest"})
    assert s["hu"]["new_communities"] == 1
    assert s["intl"]["new_communities"] == 1
    assert s["totals"]["hu"] == 1 and s["totals"]["intl"] == 1


def test_report_html_contains_sections_and_numbers():
    summary = {
        "hu": {"new_communities": 5, "changed_communities": 2, "change_rows": 7,
               "new_venues": 1, "new_persons": 0, "pages_scraped": 40,
               "pages_extracted": 30, "searches": 12},
        "intl": {"new_communities": 3, "changed_communities": 0, "change_rows": 0,
                 "new_venues": 0, "new_persons": 2, "pages_scraped": 10,
                 "pages_extracted": 8, "searches": 4},
        "runs": [{"id": 1, "mode": "search_only", "started_at": "2026-07-09T01:00:00",
                  "finished_at": "2026-07-09T16:20:00", "success": True,
                  "pairs": 100, "records": 0, "search_failed": 2, "extract_failed": 0}],
        "totals": {"hu": 11000, "intl": 2000,
                   "covered_pairs_hu": 12000, "covered_pairs_intl": 900},
    }
    traffic = {"kozossegek": {"pageviews": 120, "visitors": 45},
               "meetapedia": {"pageviews": 30, "visitors": 12}}
    subject, html = build_report_html("2026-07-09", summary, traffic)
    assert "8 új közösség" in subject and "57 látogató" in subject
    for frag in ("Napi összefoglaló — 2026-07-09", "kozossegek.com", "meetapedia.com",
                 "Új közösség", ">5<", ">3<", ">8<", "search_only", "hibák: 2 keresés",
                 "11000", "13000"):
        assert frag in html, f"hiányzik: {frag}"


def test_middleware_counts_public_html_only(tmp_path):
    from fastapi.testclient import TestClient
    from scraper.pipeline import CityConfig, TopicConfig
    from scraper.web import app as web_app
    from scraper.web.state import app_state

    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    app_state.db_path = db
    app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
    app_state.topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]
    try:
        c = TestClient(web_app.app)
        human = {"user-agent": "Mozilla/5.0 (Macintosh) Safari/605.1", "host": "kozossegek.com"}
        bot = {"user-agent": "Googlebot/2.1", "host": "kozossegek.com"}
        c.get("/", headers=human)
        c.get("/", headers=bot)                    # bot: skipped
        c.get("/robots.txt", headers=human)        # utility: skipped
        t = get_traffic_for_day(db, __import__("datetime").datetime.utcnow().strftime("%Y-%m-%d"))
        assert t.get("kozossegek", {}).get("pageviews", 0) == 1
    finally:
        app_state.db_path, app_state.cities, app_state.topics = old_db, old_cities, old_topics


def test_report_html_ga4_mode():
    summary = {
        "hu": {k: 0 for k in ("new_communities", "changed_communities", "change_rows",
                              "new_venues", "new_persons", "pages_scraped",
                              "pages_extracted", "searches")},
        "intl": {k: 0 for k in ("new_communities", "changed_communities", "change_rows",
                                "new_venues", "new_persons", "pages_scraped",
                                "pages_extracted", "searches")},
        "runs": [], "totals": {"hu": 0, "intl": 0,
                               "covered_pairs_hu": 0, "covered_pairs_intl": 0},
    }
    traffic = {"kozossegek": {"pageviews": 3, "visitors": 2}}
    ga4 = {"kozossegek": {"visitors": 87, "sessions": 110, "pageviews": 240},
           "meetapedia": {"visitors": 13, "sessions": 15, "pageviews": 30}}
    subject, html = build_report_html("2026-07-09", summary, traffic, ga4)
    assert "100 látogató" in subject          # GA4 wins over the server counter
    assert "Látogatók (GA4)" in html and "Munkamenet" in html
    assert ">87<" in html and ">125<" in html  # per-site + total sessions
    # without GA4: falls back to the server counter
    subject2, html2 = build_report_html("2026-07-09", summary, traffic, None)
    assert "2 látogató" in subject2 and "(GA4)" not in html2
