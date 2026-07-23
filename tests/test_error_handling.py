"""Provider-failure handling: typed errors, no empty-result caching, retries."""
import asyncio
from unittest.mock import patch

import pytest

from scraper.extract import (
    ExtractorQuotaError,
    ExtractorRateLimitError,
    ExtractorUnavailableError,
    FallbackExtractor,
)
from scraper.models import SearchResult
from scraper.pipeline import CityConfig, PipelineConfig, TopicConfig, _run_full
from scraper.search import (
    FallbackSearchClient,
    SearchQuotaError,
    SearchUnavailableError,
)


class StubPrimary:
    """Scripted extractor primary raising per-call errors then succeeding."""

    model = "stub-model"
    model_fingerprint = "fp-stub"
    venue_fingerprint = "fp-venue"
    person_fingerprint = "fp-person"
    enrich_fingerprint = "fp-enrich"

    def __init__(self, script):
        self.script = list(script)  # each item: Exception or return value
        self.calls = 0

    async def extract(self, *a, **k):
        self.calls += 1
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


def test_quota_error_raises_unavailable_and_exhausts():
    fe = FallbackExtractor(primaries=[StubPrimary([ExtractorQuotaError("402")])])
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert fe.exhausted
    # subsequent calls fail fast without touching the provider again
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))


def test_transient_error_is_retried_once():
    p = StubPrimary([ExtractorUnavailableError("HTTP 500"), ["ok"]])
    fe = FallbackExtractor(primaries=[p])
    assert asyncio.run(fe.extract("t", "c", "top", "hu", "http://x")) == ["ok"]
    assert p.calls == 2
    assert not fe.exhausted


def test_persistent_transient_error_raises():
    p = StubPrimary([ExtractorUnavailableError("boom"), ExtractorUnavailableError("boom")])
    fe = FallbackExtractor(primaries=[p])
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert not fe.exhausted  # transient ≠ exhausted; next page will try again


def test_rate_limit_waits_out_short_window_and_succeeds():
    p = StubPrimary([ExtractorRateLimitError(0.05), ["ok"]])
    fe = FallbackExtractor(primaries=[p])
    assert asyncio.run(fe.extract("t", "c", "top", "hu", "http://x")) == ["ok"]
    assert p.calls == 2


class QuotaSearchProvider:
    async def search(self, query, locale="en", num_results=10):
        raise SearchQuotaError("credits gone")


def test_search_all_raises_when_quota_kills_everything():
    c = FallbackSearchClient(primaries=[QuotaSearchProvider()])
    with pytest.raises(SearchQuotaError):
        asyncio.run(c.search_all(["q1"]))
    assert c.exhausted
    with pytest.raises(SearchQuotaError):  # fail-fast on later pairs
        asyncio.run(c.search_all(["q2"]))


def test_search_all_no_providers_raises():
    c = FallbackSearchClient(primaries=[])
    with pytest.raises(SearchQuotaError):
        asyncio.run(c.search_all(["q1"]))


def test_persistent_search_failure_disables_provider_after_three_attempts():
    class UnavailableSearchProvider:
        def __init__(self):
            self.calls = 0

        async def search(self, query, locale="en", num_results=10):
            self.calls += 1
            raise SearchUnavailableError("queue timed out")

    provider = UnavailableSearchProvider()
    client = FallbackSearchClient(primaries=[provider])
    for query in ("q1", "q2", "q3"):
        with pytest.raises(SearchUnavailableError):
            asyncio.run(client.search_all([query]))
    assert client.exhausted
    with pytest.raises(SearchUnavailableError):
        asyncio.run(client.search_all(["q4"]))
    assert provider.calls == 3


def test_success_resets_search_unavailability_counter():
    class FlakySearchProvider:
        def __init__(self):
            self.script = [
                SearchUnavailableError("temporary"),
                [SearchResult(url="https://ok.test", title="ok")],
                SearchUnavailableError("temporary"),
                SearchUnavailableError("temporary"),
            ]

        async def search(self, query, locale="en", num_results=10):
            result = self.script.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

    client = FallbackSearchClient(primaries=[FlakySearchProvider()])
    with pytest.raises(SearchUnavailableError):
        asyncio.run(client.search_all(["q1"]))
    assert asyncio.run(client.search_all(["q2"]))
    for query in ("q3", "q4"):
        with pytest.raises(SearchUnavailableError):
            asyncio.run(client.search_all([query]))
    assert not client.exhausted


def test_single_search_propagates_transient_failure_instead_of_empty_result():
    class UnavailableSearchProvider:
        async def search(self, query, locale="en", num_results=10):
            raise SearchUnavailableError("temporary")

    client = FallbackSearchClient(primaries=[UnavailableSearchProvider()])
    with pytest.raises(SearchUnavailableError):
        asyncio.run(client.search("q1"))
    assert not client.exhausted


def test_single_search_fails_fast_after_provider_is_disabled():
    class UnavailableSearchProvider:
        def __init__(self):
            self.calls = 0

        async def search(self, query, locale="en", num_results=10):
            self.calls += 1
            raise SearchUnavailableError("temporary")

    provider = UnavailableSearchProvider()
    client = FallbackSearchClient(primaries=[provider])
    for query in ("q1", "q2", "q3", "q4"):
        with pytest.raises(SearchUnavailableError):
            asyncio.run(client.search(query))
    assert client.exhausted
    assert provider.calls == 3


def test_search_all_partial_results_survive_quota():
    class OneThenQuota:
        async def search(self, query, locale="en", num_results=10):
            if query == "q1":
                return [SearchResult(url="https://a.test", title="a")]
            raise SearchQuotaError("gone")
    c = FallbackSearchClient(primaries=[OneThenQuota()])
    results = asyncio.run(c.search_all(["q1", "q2"]))
    assert [r.url for r in results] == ["https://a.test"]  # partial kept, no raise


# ── pipeline-level: failures must not be cached ─────────────────────────────

def _pipeline_fixtures(tmp_path):
    from scraper.db import init_db
    db = tmp_path / "scraper.db"
    init_db(db)
    cfg = PipelineConfig(
        search_results_per_query=5, search_max_pages=2, search_rate_limit=1.0,
        fetch_timeout=15, fetch_min_text_length=10, fetch_max_concurrent=3,
        fetch_blocked_domains=[], db_path=db,
    )
    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]
    return db, cfg, cities, topics


def test_search_quota_pair_not_cached(tmp_path):
    from scraper.db import get_search_cache
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)

    class QuotaFallback:
        exhausted = False
        def __init__(self, primaries): ...
        async def search_all(self, *a, **k):
            raise SearchQuotaError("credits gone")

    with patch("scraper.pipeline.FallbackSearchClient", QuotaFallback):
        _, logs = asyncio.run(_run_full(
            cities, topics, cfg, FallbackExtractor(primaries=[]), None,
            True, True, {}, None,
        ))

    assert get_search_cache(db, "Budapest", "running", ttl_days=3650) is None, \
        "a quota-failed search must NOT be recorded as searched"
    assert any(p.get("search_failed") for p in logs)


def test_provider_death_aborts_run_instead_of_per_pair_failures(tmp_path):
    """3 real errors must not become thousands of per-pair failures (2026-07-22)."""
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)
    cities = [CityConfig(name=n, locale="hu", search_variants=[])
              for n in ("Budapest", "Szeged")]
    topics = [TopicConfig(name=t, search_terms={"hu": ["kifejezés"]})
              for t in ("running", "chess")]

    def quota_client(primaries):
        return FallbackSearchClient(primaries=[QuotaSearchProvider()])

    with patch("scraper.pipeline.FallbackSearchClient", quota_client):
        _, logs = asyncio.run(_run_full(
            cities, topics, cfg, FallbackExtractor(primaries=[]), None,
            True, True, {}, None,
        ))

    # pair 1: real quota error; pair 2: abort marker; pairs 3-4: never walked
    assert len(logs) == 2
    assert all(p["search_failed"] for p in logs)
    assert "credits gone" in (logs[0]["search_error"] or "")
    assert "credits gone" in (logs[1]["search_error"] or "")


def test_missing_credentials_abort_carries_reason(tmp_path):
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)
    topics = [TopicConfig(name=t, search_terms={"hu": ["kifejezés"]})
              for t in ("running", "chess")]

    _, logs = asyncio.run(_run_full(
        cities, topics, cfg, FallbackExtractor(primaries=[]), None,
        True, True, {}, None,
    ))

    assert len(logs) == 1  # single abort marker, not one failure per pair
    assert logs[0]["search_failed"]
    assert "no search provider configured" in (logs[0]["search_error"] or "")


def test_run_pipeline_skips_catchup_when_provider_dead(tmp_path):
    from scraper.pipeline import run_pipeline
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)

    def quota_client(primaries):
        return FallbackSearchClient(primaries=[QuotaSearchProvider()])

    with patch("scraper.pipeline.FallbackSearchClient", quota_client):
        logs, _ = asyncio.run(run_pipeline(
            cities, topics, cfg, cache=None, run_mode="search_only",
        ))

    # main pass logs the one real failure; the catch-up pass must not replay it
    assert len(logs) == 1
    assert logs[0]["search_failed"]


def test_extract_failure_not_cached_as_empty(tmp_path):
    from scraper.cache import CacheManager
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)
    cache = CacheManager(db)

    class OkSearch:
        exhausted = False
        def __init__(self, primaries): ...
        async def search_all(self, *a, **k):
            return [SearchResult(url="https://klub.test/x", title="t")]

    failing = FallbackExtractor(primaries=[StubPrimary([ExtractorUnavailableError("500"),
                                                        ExtractorUnavailableError("500")])])

    async def fake_fetch(url, *a, **k):
        return "Elég hosszú oldalszöveg a teszthez."

    with patch("scraper.pipeline.FallbackSearchClient", OkSearch), \
         patch("scraper.pipeline.fetch_and_clean", fake_fetch):
        _, logs = asyncio.run(_run_full(
            cities, topics, cfg, failing, cache,
            True, True, {}, None, run_venues=False, run_persons=False,
        ))

    # page scraped (raw text cached) but NOT marked extracted — retried next run
    assert cache.get_scraped("https://klub.test/x")
    assert cache.get_extracted("https://klub.test/x", fingerprint="fp-stub") is None, \
        "a failed extraction must not be cached as an empty result"
    assert sum(p.get("extract_failed", 0) for p in logs) == 1
