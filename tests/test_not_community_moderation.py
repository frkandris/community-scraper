from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from scraper.db import (
    get_communities,
    get_false_positives,
    get_not_community_reports,
    init_db,
    save_not_community_report,
)
from scraper.models import CommunityRecord
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state


_ADMIN_HEADERS = {
    "Authorization": "Basic YWRtaW46dGVzdHBhc3M=",
    "Host": "testserver",
    "Origin": "http://testserver",
}


def _setup_report(tmp_path: Path) -> tuple[Path, int]:
    db = tmp_path / "scraper.db"
    init_db(db)
    record = CommunityRecord(
        name="Budapest Futók",
        topic="running",
        city="Budapest",
        locale="hu",
        source_url="https://example.com/futok",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [record], db)
    report_id = save_not_community_report(
        db,
        record.community_id,
        record.name,
        record.city,
        record.topic,
        record.source_url,
        "/budapest/budapest-futok",
    )
    return db, report_id


def _post_admin(db: Path, path: str):
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        with patch("scraper.web.app._ADMIN_PASSWORD", "testpass"):
            return TestClient(web_app.app).post(path, headers=_ADMIN_HEADERS)
    finally:
        app_state.db_path = old_db


def test_pending_report_does_not_hide_community_on_init(tmp_path):
    db, _ = _setup_report(tmp_path)

    init_db(db)

    assert len(get_communities(db, "Budapest", "running")) == 1


def test_approving_report_hides_community_and_adds_false_positive(tmp_path):
    db, report_id = _setup_report(tmp_path)

    response = _post_admin(db, f"/admin/not-community/{report_id}/approve")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert get_communities(db, "Budapest", "running") == []
    assert get_not_community_reports(db) == []
    assert [fp["name"] for fp in get_false_positives(db)] == ["Budapest Futók"]


def test_dismissing_report_keeps_community_visible(tmp_path):
    db, report_id = _setup_report(tmp_path)

    response = _post_admin(db, f"/admin/not-community/{report_id}/dismiss")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert len(get_communities(db, "Budapest", "running")) == 1
    assert get_not_community_reports(db) == []
