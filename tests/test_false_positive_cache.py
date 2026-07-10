import asyncio
from pathlib import Path

from scraper.cache import CacheManager, _url_hash
from scraper.db import init_db, save_search_cache
from scraper.false_positives import add, remove
from scraper.pipeline import CityConfig, PipelineConfig, TopicConfig, _run_ai_only


def _config(db_path: Path) -> PipelineConfig:
    return PipelineConfig(
        search_results_per_query=1,
        search_max_pages=1,
        search_rate_limit=0,
        fetch_timeout=1,
        fetch_min_text_length=1,
        fetch_max_concurrent=1,
        fetch_blocked_domains=[],
        db_path=db_path,
    )


def _seed_extracted(cache: CacheManager, url: str, city: str, topic: str) -> None:
    cache.save_scraped(url, "raw page text", city, topic)
    cache.save_extracted(url, [], fingerprint="community-v1", model="test-model")


def test_pair_false_positive_add_and_remove_invalidate_only_affected_extraction(tmp_path):
    db = tmp_path / "scraper.db"
    init_db(db)
    cache = CacheManager(db)
    budapest_url = "https://example.com/budapest"
    szeged_url = "https://example.com/szeged"
    save_search_cache(db, "Budapest", "running", [budapest_url], ["query"])
    save_search_cache(db, "Szeged", "cycling", [szeged_url], ["query"])
    _seed_extracted(cache, budapest_url, "Budapest", "running")
    _seed_extracted(cache, szeged_url, "Szeged", "cycling")

    add(
        db,
        name="Not a club",
        city="Budapest",
        topic="running",
        reason="commercial event",
        source_url=budapest_url,
    )

    affected = cache.get_entry(_url_hash(budapest_url))
    untouched = cache.get_entry(_url_hash(szeged_url))
    assert affected["raw_text"] == "raw page text"
    assert "records" not in affected
    assert "extracted_at" not in affected
    assert "extract_fingerprint" not in affected
    assert untouched["records"] == []
    assert untouched["extract_fingerprint"] == "community-v1"

    cache.save_extracted(budapest_url, [], fingerprint="community-v1", model="test-model")
    remove(db, "Not a club", "Budapest", "running")

    affected_after_remove = cache.get_entry(_url_hash(budapest_url))
    assert affected_after_remove["raw_text"] == "raw page text"
    assert "records" not in affected_after_remove
    assert cache.get_entry(_url_hash(szeged_url))["extract_fingerprint"] == "community-v1"


def test_global_extraction_rule_invalidates_all_community_extractions(tmp_path):
    db = tmp_path / "scraper.db"
    init_db(db)
    cache = CacheManager(db)
    urls = ["https://example.com/one", "https://example.com/two"]
    _seed_extracted(cache, urls[0], "Budapest", "running")
    _seed_extracted(cache, urls[1], "Szeged", "cycling")

    add(
        db,
        name="[AI rule] Events are not communities",
        city="",
        topic="",
        reason="Do not extract one-off events.",
        source_url="",
        fp_type="extraction_rule",
    )

    for url in urls:
        entry = cache.get_entry(_url_hash(url))
        assert entry["raw_text"] == "raw page text"
        assert "records" not in entry
        assert "extract_fingerprint" not in entry


def test_ai_only_passes_pair_scoped_false_positive_examples(tmp_path):
    db = tmp_path / "scraper.db"
    init_db(db)
    cache = CacheManager(db)
    url = "https://example.com/budapest"
    save_search_cache(db, "Budapest", "running", [url], ["query"])
    # Simulate a shared URL whose denormalized metadata was overwritten by
    # another pair. search_cache remains the authoritative attribution.
    cache.save_scraped(url, "raw page text", "Szeged", "running")
    add(db, "Not a club", "Budapest", "running", "commercial event", url)
    add(db, "Other city", "Szeged", "running", "wrong city", "")
    add(
        db,
        "[AI rule] No events",
        "",
        "",
        "Do not extract one-off events.",
        "",
        fp_type="extraction_rule",
    )

    class Extractor:
        canonical_fingerprint = "community-v1"
        canonical_venue_fingerprint = "venue-v1"
        canonical_person_fingerprint = "person-v1"
        exhausted = False
        model = "test-model"

        def __init__(self):
            self.calls = []

        async def extract(self, **kwargs):
            self.calls.append(kwargs)
            return []

    extractor = Extractor()
    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]

    asyncio.run(_run_ai_only(
        cities,
        topics,
        _config(db),
        extractor,
        cache,
        skip_extracted=True,
        run_stats={},
        on_progress=None,
        run_venues=False,
        run_persons=False,
    ))

    examples = extractor.calls[0]["false_positive_examples"]
    assert '"Not a club": commercial event' in examples
    assert "Do not extract one-off events." in examples
    assert "Other city" not in examples
