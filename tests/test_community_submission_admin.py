from pathlib import Path
from unittest.mock import AsyncMock, patch
from scraper.db import init_db, save_community_submission, get_community_submissions
from scraper.pipeline import PipelineConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient

# Base64 of "admin:testpass"
_AUTH = "Basic YWRtaW46dGVzdHBhc3M="
_HOST = "testserver"
_ADMIN_HEADERS = {
    "Authorization": _AUTH,
    "Host": _HOST,
    "Origin": f"http://{_HOST}",
}


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _cfg(db: Path) -> PipelineConfig:
    return PipelineConfig(
        search_results_per_query=5,
        search_max_pages=2,
        search_rate_limit=1.0,
        fetch_timeout=15,
        fetch_min_text_length=100,
        fetch_max_concurrent=3,
        fetch_blocked_domains=[],
        db_path=db,
    )


def test_reject_submission(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Club A", "Budapest", "running",
                                       "https://a.example.com", None)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        with patch("scraper.web.app._ADMIN_PASSWORD", "testpass"):
            resp = TestClient(web_app.app).post(
                f"/admin/submissions/{row_id}/reject",
                headers=_ADMIN_HEADERS,
            )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        rows = get_community_submissions(db, status="rejected")
        assert len(rows) == 1
        assert rows[0]["name"] == "Club A"
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg


def test_approve_submission_enqueues_scrape(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Club B", "Budapest", "yoga",
                                       "https://b.example.com", None)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        with patch("scraper.web.app._ADMIN_PASSWORD", "testpass"), \
             patch("scraper.web.app.scrape_submitted_url", new_callable=AsyncMock) as mock_scrape:
            resp = TestClient(web_app.app).post(
                f"/admin/submissions/{row_id}/approve",
                headers=_ADMIN_HEADERS,
            )
        assert mock_scrape.called
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        rows = get_community_submissions(db, status="approved")
        assert len(rows) == 1
        assert rows[0]["name"] == "Club B"
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg
