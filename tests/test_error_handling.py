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


def test_unexpected_error_becomes_unavailable_not_a_run_abort():
    """2026-07-30: a parser AttributeError escaped the chain and killed the whole
    ai_only window (0 pairs). Untyped bugs must surface as a skipped page."""
    p = StubPrimary([AttributeError("'list' object has no attribute 'get'"),
                     AttributeError("'list' object has no attribute 'get'")])
    fe = FallbackExtractor(primaries=[p])
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert p.calls == 2  # retried like any transient error
    assert not fe.exhausted


def test_unexpected_errors_still_open_the_circuit_breaker():
    """A systematic bug must not be retried for a whole night either."""
    p = StubPrimary([TypeError("boom")] * 100)
    fe = FallbackExtractor(primaries=[p], failure_threshold=3)
    for _ in range(3):
        with pytest.raises(ExtractorUnavailableError):
            asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert fe.providers_down


def test_rate_limit_waits_out_short_window_and_succeeds():
    p = StubPrimary([ExtractorRateLimitError(0.05), ["ok"]])
    fe = FallbackExtractor(primaries=[p])
    assert asyncio.run(fe.extract("t", "c", "top", "hu", "http://x")) == ["ok"]
    assert p.calls == 2


class QuotaSearchProvider:
    async def search(self, query, locale="en", num_results=10):
        raise SearchQuotaError("credits gone")


class MalformedResponseProvider:
    """DataForSEO parsing assumes the documented object shape; a bare array in
    the response raises AttributeError deep inside search()."""

    def __init__(self):
        self.calls = 0

    async def search(self, query, locale="en", num_results=10):
        self.calls += 1
        raise AttributeError("'list' object has no attribute 'get'")


def test_untyped_search_error_does_not_escape_the_chain():
    """Search-side twin of the 2026-07-30 extraction abort: an unexpected error
    must become a typed, uncached failure — not a dead collector window."""
    p = MalformedResponseProvider()
    c = FallbackSearchClient(primaries=[p])
    with pytest.raises(SearchUnavailableError):
        asyncio.run(c.search_all(["q1"]))
    with pytest.raises(SearchUnavailableError):
        asyncio.run(c.search("q1"))
    assert p.calls  # the provider was actually exercised


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


# ── extractor circuit breaker + preflight ───────────────────────────────────

def test_circuit_breaker_opens_after_consecutive_failures():
    """A dead provider must stop the chain instead of failing page after page.

    2026-07-24: a retired model name 400'd 2736 times across a whole off-peak
    window because nothing counted the repetition.
    """
    p = StubPrimary([ExtractorUnavailableError("HTTP 400")] * 20)
    fe = FallbackExtractor(primaries=[p], failure_threshold=3)
    for _ in range(3):
        with pytest.raises(ExtractorUnavailableError):
            asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert fe.providers_down
    assert "HTTP 400" in (fe.failure_reason or "")
    # breaker open → no further provider calls (3 attempts × 2 retry rounds)
    calls_when_open = p.calls
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert p.calls == calls_when_open


def test_success_resets_circuit_breaker_counter():
    """Scattered transient errors must never trip the breaker."""
    p = StubPrimary([
        ExtractorUnavailableError("500"), ExtractorUnavailableError("500"),
        ["ok"],
        ExtractorUnavailableError("500"), ExtractorUnavailableError("500"),
    ])
    fe = FallbackExtractor(primaries=[p], failure_threshold=2)
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert asyncio.run(fe.extract("t", "c", "top", "hu", "http://x")) == ["ok"]
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.extract("t", "c", "top", "hu", "http://x"))
    assert not fe.providers_down


def test_no_provider_configured_is_not_providers_down():
    """An unset API key is a deliberate no-LLM run, not an outage — it must not
    abort search/fetch work."""
    fe = FallbackExtractor(primaries=[])
    assert fe.exhausted
    assert not fe.providers_down


def test_preflight_noop_without_providers():
    assert asyncio.run(FallbackExtractor(primaries=[]).preflight()) is None


def test_preflight_raises_on_dead_provider():
    fe = FallbackExtractor(primaries=[StubPrimary([ExtractorUnavailableError("HTTP 400")] * 2)])
    with pytest.raises(ExtractorUnavailableError):
        asyncio.run(fe.preflight())


def test_run_pipeline_aborts_before_any_work_when_preflight_fails(tmp_path):
    """A broken model name must fail the run immediately, not one page at a time."""
    from scraper.pipeline import run_pipeline
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)
    searched = []

    class OkSearch:
        exhausted = False
        def __init__(self, primaries): ...
        async def search_all(self, *a, **k):
            searched.append(a)
            return [SearchResult(url="https://klub.test/x", title="t")]

    dead = FallbackExtractor(primaries=[StubPrimary([ExtractorUnavailableError("HTTP 400")] * 4)])

    with patch("scraper.pipeline.FallbackSearchClient", OkSearch), \
         patch("scraper.pipeline.FallbackExtractor", lambda primaries: dead):
        with pytest.raises(ExtractorUnavailableError) as exc:
            asyncio.run(run_pipeline(cities, topics, cfg, cache=None, run_mode="full"))

    assert "preflight" in str(exc.value)
    assert not searched, "no paid search may run once the extractor is known dead"


def test_ai_only_aborts_when_extractor_dies_midrun(tmp_path):
    """The breaker must end the run, not log one failure per cached page."""
    from scraper.cache import CacheManager
    from scraper.pipeline import _run_ai_only
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)
    cache = CacheManager(db)
    cities = [CityConfig(name=n, locale="hu", search_variants=[])
              for n in ("Budapest", "Szeged")]
    from scraper.db import save_search_cache
    for city in ("Budapest", "Szeged"):
        for url in (f"https://{city}.test/a", f"https://{city}.test/b"):
            cache.save_scraped(url, "Elég hosszú oldalszöveg a teszthez.", city, "running")
        save_search_cache(db, city, "running",
                          [f"https://{city}.test/a", f"https://{city}.test/b"], ["q"])

    dead = FallbackExtractor(
        primaries=[StubPrimary([ExtractorUnavailableError("HTTP 400")] * 10)],
        failure_threshold=1)

    _, logs = asyncio.run(_run_ai_only(
        cities, topics, cfg, dead, cache, True, {}, None,
        run_venues=False, run_persons=False))

    assert len(logs) == 1, "the run must abort at the first pair, not walk both cities"
    assert logs[0]["extract_error"] and "HTTP 400" in logs[0]["extract_error"]


def test_a_provider_that_recovers_on_retry_is_not_retired():
    """Failing an attempt and succeeding on the retry is a working provider.

    `_call` retries transient errors once, and the failures seen during a call
    are applied at the end — so the provider that eventually answered has to be
    excluded, or twenty self-recovering calls retire a healthy endpoint.
    """
    p = StubPrimary([ExtractorUnavailableError("500"), ["ok"]] * 30)
    fe = FallbackExtractor(primaries=[p], failure_threshold=2)
    for _ in range(30):
        assert asyncio.run(fe.extract("t", "c", "top", "hu", "http://x")) == ["ok"]
    assert not fe.providers_down


def test_completed_extractions_are_cached_even_when_the_pair_stops(tmp_path):
    """Work the fleet was already charged for must reach the cache.

    With extraction concurrent, every page of a pair can finish before the
    consumer loop notices the fleet stopped. Breaking out of the loop there
    threw those results away and made the next pass pay for them again.
    """
    from scraper.cache import CacheManager
    from scraper.pipeline import _run_ai_only
    db, cfg, cities, topics = _pipeline_fixtures(tmp_path)
    cfg.extract_concurrency = 4
    cache = CacheManager(db)
    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    urls = [f"https://budapest.test/{i}" for i in range(4)]
    for url in urls:
        cache.save_scraped(url, "Elég hosszú oldalszöveg a teszthez.", "Budapest", "running")

    class _Primary:
        provider, model, quality = "p", "m", 50
        model_fingerprint = "fp"

        async def extract(self, text, city, topic, locale, source_url,
                          false_positive_examples=""):
            return []

    chain = FallbackExtractor(primaries=[_Primary()])
    asyncio.run(_run_ai_only(cities, topics, cfg, chain, cache, True, {}, None,
                             run_venues=False, run_persons=False))

    # Every page that was extracted is cached, so the next pass skips it.
    assert all(cache.get_extracted(u, fingerprint=chain.canonical_fingerprint) is not None
               for u in urls)
