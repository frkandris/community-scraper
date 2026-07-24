from pathlib import Path
from scraper.db import (
    init_db,
    save_edit_request,
    get_edit_requests,
    resolve_edit_request,
    apply_community_edit,
    _community_record_key,
    get_community_by_record_key,
    get_all_communities,
)
from scraper.store import save_results
from scraper.models import CommunityRecord


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _community(tmp_path, name="Budapest Futók", topic="running", city="Budapest") -> tuple[Path, str]:
    db = _db(tmp_path)
    r = CommunityRecord(
        name=name, topic=topic, city=city, locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00"
    )
    save_results(city, topic, [r], db)
    return db, _community_record_key(name, city, topic)


def test_save_and_get_edit_request(tmp_path):
    db = _db(tmp_path)
    req_id = save_edit_request(
        db, "community", "abc123", "Budapest Futók", "Budapest", "running",
        "budapest_futok|budapest|running", "wrong_city", "Debrecen",
        "Ez Debrecenben van, nem Budapesten", "test@example.com",
    )
    assert req_id > 0
    rows = get_edit_requests(db, status="pending")
    assert len(rows) == 1
    r = rows[0]
    assert r["entity_name"] == "Budapest Futók"
    assert r["change_type"] == "wrong_city"
    assert r["new_value"] == "Debrecen"
    assert r["email"] == "test@example.com"
    assert r["status"] == "pending"
    assert r["reviewed_at"] is None


def test_resolve_edit_request(tmp_path):
    db = _db(tmp_path)
    req_id = save_edit_request(
        db, "community", "abc", "Test", "Budapest", "running",
        "test|budapest|running", "archive", None,
        "Megszűnt 2024-ben", "u@example.com",
    )
    resolve_edit_request(db, req_id, "approved")
    assert get_edit_requests(db, status="pending") == []
    approved = get_edit_requests(db, status="approved")
    assert approved[0]["status"] == "approved"
    assert approved[0]["reviewed_at"] is not None


def test_apply_community_edit_wrong_city(tmp_path):
    db, key = _community(tmp_path)
    assert apply_community_edit(db, key, "wrong_city", "Debrecen") == "ok"
    # record_key derives from (name, city, topic) — the fixed edit moves the row
    # to its new key (the stale key used to cause duplicate rows on next scrape)
    assert get_community_by_record_key(db, key) is None
    new_key = _community_record_key("Budapest Futók", "Debrecen", "running")
    data = get_community_by_record_key(db, new_key)
    assert data and data["city"] == "Debrecen"


def test_apply_community_edit_archive(tmp_path):
    db, key = _community(tmp_path)
    apply_community_edit(db, key, "archive", None)
    visible = get_all_communities(db)
    assert not any(c["name"] == "Budapest Futók" for c in visible)


def test_apply_community_edit_name_correction(tmp_path):
    db, key = _community(tmp_path, name="Budpaest Futók")
    apply_community_edit(db, key, "name_correction", "Budapest Futók")
    assert get_community_by_record_key(db, key) is None
    new_key = _community_record_key("Budapest Futók", "Budapest", "running")
    data = get_community_by_record_key(db, new_key)
    assert data and data["name"] == "Budapest Futók"


def test_apply_community_edit_wrong_topic(tmp_path):
    db, key = _community(tmp_path, topic="running")
    result = apply_community_edit(db, key, "wrong_topic", "fitness")
    assert result == "ok"
    assert get_community_by_record_key(db, key) is None
    new_key = _community_record_key("Budapest Futók", "Budapest", "fitness")
    data = get_community_by_record_key(db, new_key)
    assert data and data["topic"] == "fitness"


def test_apply_community_edit_not_found(tmp_path):
    db = _db(tmp_path)
    assert apply_community_edit(db, "c2:missing", "wrong_city", "Debrecen") == "not_found"


def test_apply_community_edit_unsupported(tmp_path):
    db, key = _community(tmp_path)
    assert apply_community_edit(db, key, "nonsense", "x") == "unsupported"


def test_apply_community_edit_wrong_city_merges_on_conflict(tmp_path):
    # The same club exists under both the wrong city (Szentendre) and its real
    # city (Szentgotthárd). Approving the wrong_city edit must merge, not fail.
    db = _db(tmp_path)
    wrong = CommunityRecord(
        name="Nappali Idősek Klubja", topic="seniors", city="Szentendre", locale="hu",
        source_url="https://wrong.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    right = CommunityRecord(
        name="Nappali Idősek Klubja", topic="seniors", city="Szentgotthárd", locale="hu",
        source_url="https://right.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Szentendre", "seniors", [wrong], db)
    save_results("Szentgotthárd", "seniors", [right], db)
    wrong_key = _community_record_key("Nappali Idősek Klubja", "Szentendre", "seniors")
    right_key = _community_record_key("Nappali Idősek Klubja", "Szentgotthárd", "seniors")

    assert apply_community_edit(db, wrong_key, "wrong_city", "Szentgotthárd") == "merged"

    # Target keeps its identity and gains the source's URLs; source is hidden.
    merged = get_community_by_record_key(db, right_key)
    assert merged and merged["city"] == "Szentgotthárd"
    assert "https://wrong.test" in (merged.get("source_urls") or [])
    assert "https://right.test" in (merged.get("source_urls") or [])
    visible = get_all_communities(db)
    assert sum(1 for c in visible if c["name"] == "Nappali Idősek Klubja") == 1
