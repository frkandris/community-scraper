"""Regression tests for the 2026-07 bug-hunt batch A (severe findings)."""
from pathlib import Path

from scraper.db import (
    _community_record_key,
    apply_recategorize_suggestion,
    get_communities,
    get_persons_for_community,
    init_db,
    set_community_hidden,
    upsert_persons,
)
from scraper.fetch import _is_blocked, host_matches_domain
from scraper.models import CommunityRecord, PersonRecord
from scraper.store import save_results


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _rec(name, topic="running", city="Budapest"):
    return CommunityRecord(
        name=name, topic=topic, city=city, locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )


def test_hidden_survives_rescrape(tmp_path):
    """A merged/reported (hidden) community must NOT resurface on re-scrape."""
    db = _db(tmp_path)
    save_results("Budapest", "running", [_rec("Duplikált Klub")], db)
    rk = _community_record_key("Duplikált Klub", "Budapest", "running")
    set_community_hidden(db, rk, True)
    assert get_communities(db, "Budapest", "running") == []

    # next pipeline run re-extracts the same community from the cached page
    save_results("Budapest", "running", [_rec("Duplikált Klub")], db)
    assert get_communities(db, "Budapest", "running") == [], \
        "hidden flag was lost across replace_communities_for_topic"


def test_revalidate_fingerprint_survives_rescrape(tmp_path):
    from scraper.db import set_community_revalidate_fingerprint, _connect
    db = _db(tmp_path)
    save_results("Budapest", "running", [_rec("Futó Kör")], db)
    rk = _community_record_key("Futó Kör", "Budapest", "running")
    set_community_revalidate_fingerprint(db, rk, "fp123")
    save_results("Budapest", "running", [_rec("Futó Kör")], db)
    with _connect(db) as conn:
        row = conn.execute(
            "SELECT revalidate_fingerprint FROM communities WHERE record_key=?", (rk,)
        ).fetchone()
    assert row and row[0] == "fp123"


def test_blocked_domain_no_substring_false_positive():
    blocked = ["x.com", "facebook.com"]
    assert not _is_blocked("https://www.linux.com/groups", blocked)
    assert not _is_blocked("https://forum.maxx.com/", blocked)
    assert _is_blocked("https://x.com/foo", blocked)
    assert _is_blocked("https://mobile.x.com/foo", blocked)      # subdomain
    assert _is_blocked("https://www.facebook.com/groups/1", blocked)
    assert not host_matches_domain("notfacebook.com", "facebook.com")


def test_persons_found_despite_case_difference(tmp_path):
    db = _db(tmp_path)
    p = PersonRecord(
        name="Kiss Anna", role="leader", city="Budapest", topic="running",
        community_name="FUTÓ kör",  # LLM-variant casing
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])
    found = get_persons_for_community(db, "Futó Kör", "Budapest")
    assert len(found) == 1 and found[0]["name"] == "Kiss Anna"


def test_recategorize_updates_topic_column(tmp_path):
    db = _db(tmp_path)
    save_results("Budapest", "other", [_rec("Sakk Kör", topic="other")], db)
    rk = _community_record_key("Sakk Kör", "Budapest", "other")
    apply_recategorize_suggestion(db, rk, "chess")
    assert get_communities(db, "Budapest", "other") == []
    chess = get_communities(db, "Budapest", "chess")
    assert len(chess) == 1 and chess[0]["name"] == "Sakk Kör"


def test_recategorize_key_collision_no_crash(tmp_path):
    db = _db(tmp_path)
    save_results("Budapest", "chess", [_rec("Sakk Kör", topic="chess")], db)
    save_results("Budapest", "other", [_rec("Sakk Kör", topic="other")], db)
    rk_other = _community_record_key("Sakk Kör", "Budapest", "other")
    apply_recategorize_suggestion(db, rk_other, "chess")  # must not raise
    assert get_communities(db, "Budapest", "other") == []
    assert len(get_communities(db, "Budapest", "chess")) == 1


def test_timeline_new_communities_dedups_recreate(tmp_path):
    """delete+reinsert re-logs __created__ — the timeline must count each id once."""
    from scraper.db import get_activity_timeline, _connect
    db = _db(tmp_path)
    with _connect(db) as conn:
        for ts in ("2026-07-09T01:00:00+00:00", "2026-07-09T05:00:00+00:00"):
            conn.execute(
                "INSERT INTO community_history (community_id, changed_at, changed_by, field, old_value, new_value)"
                " VALUES ('abc123', ?, 'scraper', '__created__', NULL, 'X')", (ts,))
        conn.commit()
    rows = get_activity_timeline(db, "24h")
    total_new = sum(r.get("new_communities", 0) for r in rows.values()) if isinstance(rows, dict) else sum(r.get("new_communities", 0) for r in rows)
    assert total_new <= 1, f"same community_id counted {total_new}x"
