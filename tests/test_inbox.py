"""Admin Inbox: pending user-interaction counts + nav badge rendering."""
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from scraper.db import (
    count_pending_interactions,
    init_db,
    resolve_community_submission,
    save_community_submission,
    save_edit_request,
    save_not_community_report,
)
from scraper.web import app as web_app
from scraper.web.state import app_state

_AUTH = {
    "Authorization": "Basic YWRtaW46dGVzdHBhc3M=",
    "Host": "testserver",
    "Origin": "http://testserver",
}


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _seed(db: Path) -> None:
    save_edit_request(db, "community", "abc123", "Futó Kör", "Budapest", "running",
                      "rk", "correction", "new desc", "notes", "a@b.hu")
    save_not_community_report(db, "abc123", "Nem Klub", "Budapest", "running",
                              "https://a.test", "https://kozossegek.com/x")
    save_community_submission(db, "Új Kör", "Szeged", "chess", "https://b.test", None)
    save_community_submission(db, "Már Kezelt", "Szeged", "chess", "https://c.test", None)


def test_count_pending_interactions(tmp_path):
    db = _db(tmp_path)
    assert count_pending_interactions(db) == {
        "edit_requests": 0, "reports": 0, "submissions": 0, "total": 0}

    _seed(db)
    sub_rows = count_pending_interactions(db)
    assert sub_rows == {"edit_requests": 1, "reports": 1, "submissions": 2, "total": 4}

    # handled items leave the pending count
    resolve_community_submission(db, 2, "rejected")
    assert count_pending_interactions(db)["submissions"] == 1


def test_count_pending_interactions_missing_db(tmp_path):
    assert count_pending_interactions(tmp_path / "nope.db")["total"] == 0


def test_admin_nav_shows_inbox_badge(tmp_path):
    db = _db(tmp_path)
    _seed(db)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        with patch("scraper.web.app._ADMIN_PASSWORD", "testpass"):
            r = TestClient(web_app.app).get("/admin/edit-requests", headers=_AUTH)
        assert r.status_code == 200
        assert "Inbox" in r.text
        # total badge (1 edit + 1 report + 2 submissions)
        assert ">4</span>" in r.text
    finally:
        app_state.db_path = old_db
