import json
import sqlite3
from pathlib import Path

from scraper.db import (
    _UNICODE_RECORD_KEYS_MIGRATION,
    _community_record_key,
    get_communities,
    get_community_by_record_key,
    init_db,
)
from scraper.identity import person_record_key, public_slug, venue_record_key
from scraper.models import CommunityRecord
from scraper.store import save_results


def _record(name: str) -> CommunityRecord:
    return CommunityRecord(
        name=name,
        topic="running",
        city="Tokyo",
        locale="ja",
        source_url="https://example.com/community",
        extracted_at="2026-01-01T00:00:00+00:00",
    )


def test_non_latin_entities_get_distinct_record_keys():
    assert _community_record_key("東京ランニングクラブ", "Tokyo", "running") != (
        _community_record_key("東京走友会", "Tokyo", "running")
    )
    assert venue_record_key("東京体育館", "Tokyo") != venue_record_key("東京武道館", "Tokyo")
    assert person_record_key("山田太郎", "Tokyo", "leader", "走友会") != (
        person_record_key("鈴木花子", "Tokyo", "leader", "走友会")
    )


def test_non_latin_public_slugs_are_nonempty_and_distinct():
    first = public_slug("東京ランニングクラブ")
    second = public_slug("東京走友会")

    assert first.startswith("u-")
    assert second.startswith("u-")
    assert first != second
    assert public_slug("Kovács János") == "kovacs-janos"


def test_store_keeps_two_non_latin_communities_in_the_same_pair(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)

    save_results(
        "Tokyo",
        "running",
        [_record("東京ランニングクラブ"), _record("東京走友会")],
        db,
    )

    assert {r["name"] for r in get_communities(db, "Tokyo", "running")} == {
        "東京ランニングクラブ",
        "東京走友会",
    }


def test_init_db_migrates_legacy_keys_and_references(tmp_path: Path):
    db = tmp_path / "scraper.db"
    init_db(db)
    record = _record("東京ランニングクラブ")
    legacy_key = "|tokyo|running"
    new_key = _community_record_key(record.name, record.city, record.topic)

    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO communities"
            " (record_key, community_id, city, topic, data, updated_at, hidden)"
            " VALUES (?, ?, ?, ?, ?, ?, 0)",
            (
                legacy_key,
                record.community_id,
                record.city,
                record.topic,
                json.dumps(record.model_dump(), ensure_ascii=False),
                record.extracted_at,
            ),
        )
        conn.execute(
            "INSERT INTO edit_requests"
            " (entity_type, entity_id, entity_name, entity_city, entity_topic, record_key,"
            " change_type, notes, email, submitted_at)"
            " VALUES ('community', ?, ?, ?, ?, ?, 'description', '', '', ?)",
            (
                record.community_id,
                record.name,
                record.city,
                record.topic,
                legacy_key,
                record.extracted_at,
            ),
        )
        conn.execute(
            "INSERT INTO recategorize_suggestions"
            " (record_key, community_name, city, status) VALUES (?, ?, ?, 'pending')",
            (legacy_key, record.name, record.city),
        )
        conn.execute(
            "DELETE FROM schema_migrations WHERE name=?",
            (_UNICODE_RECORD_KEYS_MIGRATION,),
        )
        conn.commit()

    init_db(db)

    assert get_community_by_record_key(db, legacy_key) is None
    assert get_community_by_record_key(db, new_key)["name"] == record.name
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT record_key FROM edit_requests").fetchone()[0] == new_key
        assert conn.execute("SELECT record_key FROM recategorize_suggestions").fetchone()[0] == new_key
