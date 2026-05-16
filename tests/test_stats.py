from pathlib import Path

from scraper.db import init_db, bulk_upsert_communities, get_data_quality_stats


def test_get_data_quality_stats_empty_db(tmp_path: Path):
    db_path = tmp_path / "test.db"
    stats = get_data_quality_stats(db_path)
    assert stats["total"] == 0
    assert stats["visible"] == 0
    assert stats["city_rows"] == []
    assert stats["topic_counts"] == {}


def test_get_data_quality_stats(tmp_path: Path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    bulk_upsert_communities(db_path, [
        {
            "name": "Alpha Runners", "city": "Budapest", "topic": "futás",
            "website": "https://alpha.com", "contact": "",
            "description": "x" * 60,
        },
        {
            "name": "Beta Yoga", "city": "Budapest", "topic": "jóga",
            "website": "", "contact": "info@beta.com",
            "description": "rövid",
        },
        {
            "name": "Gamma Club", "city": "Debrecen", "topic": "futás",
            "website": "", "contact": "",
            "description": "",
        },
    ])

    stats = get_data_quality_stats(db_path)

    assert stats["total"] == 3
    assert stats["visible"] == 3
    assert stats["hidden"] == 0
    assert stats["cities"] == 2
    assert stats["topics"] == 2
    assert stats["has_website"] == 1
    assert stats["has_contact"] == 1
    assert stats["has_description"] == 1  # only Alpha (>50 chars)
    assert stats["has_any"] == 2          # Alpha (website) + Beta (contact)

    assert len(stats["city_rows"]) == 2
    assert stats["city_rows"][0]["city"] == "Budapest"
    assert stats["city_rows"][0]["cnt"] == 2
    assert stats["city_rows"][0]["w"] == 1
    assert stats["city_rows"][0]["c"] == 1
    assert stats["city_rows"][1]["city"] == "Debrecen"
    assert stats["city_rows"][1]["cnt"] == 1

    assert stats["topic_counts"]["futás"] == 2
    assert stats["topic_counts"]["jóga"] == 1


def test_get_data_quality_stats_hidden(tmp_path: Path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    bulk_upsert_communities(db_path, [
        {"name": "Visible", "city": "Budapest", "topic": "futás",
         "website": "https://v.com", "contact": "", "description": ""},
    ])
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE communities SET hidden=1 WHERE city='Budapest'")
        conn.commit()

    stats = get_data_quality_stats(db_path)
    assert stats["total"] == 1
    assert stats["visible"] == 0
    assert stats["hidden"] == 1
    assert stats["has_website"] == 0
    assert stats["city_rows"] == []
