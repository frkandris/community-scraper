from pathlib import Path

from scraper.cache import CacheManager, _url_hash
from scraper.db import init_db, save_search_cache
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


def test_pair_scrape_read_does_not_materialize_other_pairs(tmp_path: Path):
    db_path = tmp_path / "scraper.db"
    init_db(db_path)
    cache = CacheManager(db_path)
    budapest_url = "https://example.com/budapest"
    stockholm_url = "https://example.com/stockholm"
    save_search_cache(db_path, "Budapest", "running", [budapest_url], ["q1"])
    save_search_cache(db_path, "Stockholm", "running", [stockholm_url], ["q2"])
    cache.save_scraped(budapest_url, "budapest text", "Budapest", "running")
    cache.save_scraped(stockholm_url, "stockholm text", "Stockholm", "running")

    assert cache.get_scraped_for_pair("Stockholm", "running") == [
        (stockholm_url, "stockholm text")
    ]


def test_pair_scrape_read_uses_search_cache_for_shared_url(tmp_path: Path):
    db_path = tmp_path / "scraper.db"
    init_db(db_path)
    cache = CacheManager(db_path)
    url = "https://example.com/shared"
    save_search_cache(db_path, "Budapest", "running", [url], ["q1"])
    save_search_cache(db_path, "Stockholm", "running", [url], ["q2"])
    # Denormalized metadata is last-write-wins, but both authoritative search
    # pairs must still see the page.
    cache.save_scraped(url, "shared text", "Stockholm", "running")

    assert cache.get_scraped_for_pair("Budapest", "running") == [(url, "shared text")]
    assert cache.get_scraped_for_pair("Stockholm", "running") == [(url, "shared text")]
