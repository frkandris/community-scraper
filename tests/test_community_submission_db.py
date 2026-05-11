from pathlib import Path
from scraper.db import (
    init_db,
    save_community_submission,
    get_community_submissions,
    resolve_community_submission,
)


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_save_community_submission_returns_id(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(
        db, "Budapest Futók", "Budapest", "running",
        "https://example.com/futok", "user@example.com"
    )
    assert row_id > 0


def test_get_community_submissions_pending(tmp_path):
    db = _db(tmp_path)
    save_community_submission(db, "Budapest Futók", "Budapest", "running",
                              "https://example.com/futok", None)
    rows = get_community_submissions(db, status="pending")
    assert len(rows) == 1
    r = rows[0]
    assert r["name"] == "Budapest Futók"
    assert r["city"] == "Budapest"
    assert r["topic"] == "running"
    assert r["source_url"] == "https://example.com/futok"
    assert r["submitter_email"] is None
    assert r["status"] == "pending"
    assert r["submitted_at"]


def test_get_community_submissions_filters_by_status(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Klub", "Debrecen", "chess",
                                       "https://example.com/klub", None)
    resolve_community_submission(db, row_id, "approved")
    assert len(get_community_submissions(db, status="pending")) == 0
    approved = get_community_submissions(db, status="approved")
    assert len(approved) == 1
    assert approved[0]["name"] == "Klub"


def test_resolve_community_submission(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Test", "Győr", "yoga",
                                       "https://example.com/test", "a@b.com")
    resolve_community_submission(db, row_id, "rejected")
    rows = get_community_submissions(db, status="rejected")
    assert len(rows) == 1
    assert rows[0]["id"] == row_id
