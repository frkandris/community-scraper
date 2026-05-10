from pathlib import Path
from scraper.db import (
    init_db,
    insert_duplicate_candidate,
    get_duplicate_candidates,
    resolve_duplicate_candidate,
    merge_community_into,
    get_all_communities,
    get_communities,
    _community_record_key,
)
from scraper.store import save_results
from scraper.models import CommunityRecord


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_insert_and_get_candidates(tmp_path):
    db = _db(tmp_path)
    insert_duplicate_candidate(db, "community", "id_a", "id_b", "key_a", "key_b", 0.95, "fuzzy_name")
    rows = get_duplicate_candidates(db)
    assert len(rows) == 1
    r = rows[0]
    assert r["entity_type"] == "community"
    assert r["winner_id"] == "id_a"
    assert r["loser_id"] == "id_b"
    assert r["similarity"] == 0.95
    assert r["signal"] == "fuzzy_name"
    assert r["resolution"] is None


def test_resolve_candidate(tmp_path):
    db = _db(tmp_path)
    insert_duplicate_candidate(db, "community", "id_a", "id_b", "key_a", "key_b", 0.95, "fuzzy_name")
    cid = get_duplicate_candidates(db)[0]["id"]
    resolve_duplicate_candidate(db, cid, "dismissed")
    rows = get_duplicate_candidates(db, resolved=False)
    assert len(rows) == 0
    rows_all = get_duplicate_candidates(db, resolved=True)
    assert rows_all[0]["resolution"] == "dismissed"


def test_no_duplicate_pair_inserted_twice(tmp_path):
    db = _db(tmp_path)
    insert_duplicate_candidate(db, "community", "id_a", "id_b", "key_a", "key_b", 0.95, "fuzzy_name")
    insert_duplicate_candidate(db, "community", "id_a", "id_b", "key_a", "key_b", 0.95, "fuzzy_name")
    rows = get_duplicate_candidates(db)
    assert len(rows) == 1  # idempotent


def test_merge_community_into_hides_loser_and_merges_urls(tmp_path):
    db = _db(tmp_path)
    r1 = CommunityRecord(name="Budapest Futók", topic="running", city="Budapest",
                         locale="hu", source_url="https://a.test",
                         extracted_at="2026-01-01T00:00:00+00:00")
    r2 = CommunityRecord(name="Budapest Futó Kör", topic="fitness", city="Budapest",
                         locale="hu", source_url="https://b.test",
                         extracted_at="2026-01-01T00:00:00+00:00")
    save_results("Budapest", "running", [r1], db)
    save_results("Budapest", "fitness", [r2], db)

    winner_key = _community_record_key(r1.name, r1.city, r1.topic)
    loser_key = _community_record_key(r2.name, r2.city, r2.topic)

    merge_community_into(db, winner_key, loser_key)

    # get_all_communities returns every row (no hidden filter); get_communities uses hidden=0
    all_records = get_all_communities(db)
    assert len(all_records) == 2  # both rows exist in DB

    # Only the winner is visible (hidden=0)
    visible = get_communities(db, "Budapest", "running")
    assert len(visible) == 1
    assert visible[0]["name"] == "Budapest Futók"
    assert "https://b.test" in (visible[0].get("source_urls") or [])

    # The loser is hidden
    hidden_check = get_communities(db, "Budapest", "fitness")
    assert len(hidden_check) == 0
