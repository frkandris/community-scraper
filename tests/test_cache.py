from pathlib import Path

from scraper.cache import CacheManager, _url_hash
from scraper.db import init_db
from scraper.models import CommunityRecord


def make_record() -> CommunityRecord:
    return CommunityRecord(
        name="Budapest Runners",
        topic="running",
        city="Budapest",
        locale="hu",
        source_url="https://example.com/source",
        extracted_at="2026-01-01T00:00:00+00:00",
    )


def test_extract_cache_uses_fingerprint(tmp_path: Path):
    db_path = tmp_path / "scraper.db"
    init_db(db_path)
    cache = CacheManager(db_path)

    cache.save_extracted(
        "https://example.com/page",
        [make_record()],
        fingerprint="model-a",
        model="example-model",
    )

    assert cache.get_extracted("https://example.com/page", fingerprint="model-a")
    assert cache.get_extracted("https://example.com/page", fingerprint="model-b") is None


def test_delete_extracted_clears_fingerprint_and_model(tmp_path: Path):
    db_path = tmp_path / "scraper.db"
    init_db(db_path)
    cache = CacheManager(db_path)
    url = "https://example.com/page"

    cache.save_extracted(url, [make_record()], fingerprint="model-a", model="example-model")
    assert cache.delete_extracted(_url_hash(url))

    entry = cache.get_entry(_url_hash(url))
    assert "extract_fingerprint" not in entry
    assert "extract_model" not in entry
    assert "records" not in entry
