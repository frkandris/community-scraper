"""Cost-saver schedule: search_only mode, stop_at windows, transient-search safety."""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from scraper.extract import FallbackExtractor
from scraper.main import _next_window_end
from scraper.models import SearchResult
from scraper.pipeline import (
    CityConfig,
    PipelineConfig,
    TopicConfig,
    _new_pair_log,
    run_pipeline,
    _run_full,
)
from scraper.search import SearchUnavailableError


def _fixtures(tmp_path):
    from scraper.db import init_db
    db = tmp_path / "scraper.db"
    init_db(db)
    cfg = PipelineConfig(
        search_results_per_query=5, search_max_pages=2, search_rate_limit=1.0,
        fetch_timeout=15, fetch_min_text_length=10, fetch_max_concurrent=3,
        fetch_blocked_domains=[], db_path=db,
        deepseek_api_key="test-key",  # so an extractor primary exists
    )
    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]
    return db, cfg, cities, topics


class CountingExtractor(FallbackExtractor):
    """Fails the test if any LLM method is ever invoked."""
    def __init__(self):
        super().__init__(primaries=[])
        self.llm_calls = 0

    async def _call(self, *a, **k):  # noqa: D401
        self.llm_calls += 1
        raise AssertionError("LLM must not be called in search_only mode")


class OkSearch:
    exhausted = False
    def __init__(self, primaries): ...
    async def search_all(self, *a, **k):
        return [SearchResult(url="https://klub.test/a", title="t")]


def test_search_only_collects_without_llm(tmp_path):
    from scraper.cache import CacheManager
    from scraper.db import get_search_cache
    db, cfg, cities, topics = _fixtures(tmp_path)
    cache = CacheManager(db)

    async def fake_fetch(url, *a, **k):
        return "Elég hosszú oldalszöveg a gyűjtögetéshez."

    counting = CountingExtractor()
    with patch("scraper.pipeline.FallbackSearchClient", OkSearch), \
         patch("scraper.pipeline.fetch_and_clean", fake_fetch), \
         patch("scraper.pipeline.FallbackExtractor", lambda primaries: counting):
        logs, _ = asyncio.run(run_pipeline(
            cities, topics, cfg, cache=cache, run_mode="search_only",
        ))

    assert counting.llm_calls == 0
    assert get_search_cache(db, "Budapest", "running", ttl_days=3650) == ["https://klub.test/a"]
    assert cache.get_scraped("https://klub.test/a")  # page text collected
    assert cache.get_extracted("https://klub.test/a", fingerprint="") is None


def test_stop_at_in_past_processes_zero_pairs(tmp_path):
    db, cfg, cities, topics = _fixtures(tmp_path)
    past = datetime.now(timezone.utc) - timedelta(minutes=1)

    class MustNotSearch:
        exhausted = False
        def __init__(self, primaries): ...
        async def search_all(self, *a, **k):
            raise AssertionError("window closed — search must not run")

    with patch("scraper.pipeline.FallbackSearchClient", MustNotSearch):
        total, logs = asyncio.run(_run_full(
            cities, topics, cfg, FallbackExtractor(primaries=[]), None,
            True, True, {}, None, stop_at=past,
        ))
    assert total == 0 and logs == []


def test_next_window_end_same_day_and_midnight_cross():
    start = datetime(2026, 7, 9, 1, 5, tzinfo=timezone.utc)
    assert _next_window_end(start, "16:20") == datetime(2026, 7, 9, 16, 20, tzinfo=timezone.utc)
    # extract window: starts 16:35, "00:20" must be the NEXT day
    start2 = datetime(2026, 7, 9, 16, 35, tzinfo=timezone.utc)
    assert _next_window_end(start2, "00:20") == datetime(2026, 7, 10, 0, 20, tzinfo=timezone.utc)
    assert _next_window_end(start2, "garbage") is None


def test_transient_search_failure_not_cached(tmp_path):
    from scraper.db import get_search_cache
    db, cfg, cities, topics = _fixtures(tmp_path)

    class TransientFail:
        exhausted = False
        def __init__(self, primaries): ...
        async def search_all(self, *a, **k):
            raise SearchUnavailableError("DataForSEO HTTP 500")

    with patch("scraper.pipeline.FallbackSearchClient", TransientFail):
        _, logs = asyncio.run(_run_full(
            cities, topics, cfg, FallbackExtractor(primaries=[]), None,
            True, True, {}, None,
        ))

    assert get_search_cache(db, "Budapest", "running", ttl_days=3650) is None, \
        "transient search failure must not be recorded as searched"
    assert any(p.get("search_failed") for p in logs)


def test_failure_pair_logs_carry_all_template_keys():
    """run_detail.html sums/iterates these keys with strict Undefined."""
    log = _new_pair_log("Budapest", "running", ["q"])
    for key in ("records_extracted", "urls_found", "fetched_urls",
                "cache_hits_scrape", "cache_hits_extract", "fetch_failed",
                "search_cache_hit", "search_failed", "extract_failed"):
        assert key in log, f"missing pair-log key: {key}"
