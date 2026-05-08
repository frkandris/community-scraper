from pathlib import Path

from scraper.db import get_communities, init_db
from scraper.models import CommunityRecord
from scraper.store import save_results


def make_record(name: str, website: str, source_url: str) -> CommunityRecord:
    return CommunityRecord(
        name=name,
        topic="running",
        city="Budapest",
        locale="hu",
        website=website,
        source_url=source_url,
        extracted_at="2026-01-01T00:00:00+00:00",
    )


def test_save_results_deduplicates_by_website(tmp_path: Path):
    db_path = tmp_path / "scraper.db"
    init_db(db_path)

    save_results(
        "Budapest",
        "running",
        [
            make_record("Budapest Runners", "https://example.com", "https://source-a.test"),
            make_record("Budapest Running Club", "https://example.com/", "https://source-b.test"),
        ],
        db_path,
    )

    records = get_communities(db_path, "Budapest", "running")
    assert len(records) == 1
