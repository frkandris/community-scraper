"""The extraction quarantine: pages that stop being paid to re-fail.

A failed extraction is deliberately never cached — caching it would record "0
communities" permanently under the current fingerprint. Without a bound, that
correct rule meant a page failing deterministically was re-attempted by every
run forever, walking the whole provider fleet each time. Free until paid
providers went on; after that it was most of the day's bill. See
docs/wiki/pages/concepts/extraction-quarantine.md.
"""
from pathlib import Path

import pytest

from scraper.db import (count_quarantined_pages, get_extract_failure_counts,
                        init_db)
from scraper.extract import ExtractorContentError, ExtractorUnavailableError


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


class _FakeCache:
    """Just the quarantine surface of CacheManager, against a real database."""

    def __init__(self, db_path):
        self.db_path = db_path

    @staticmethod
    def url_hash(url):
        from scraper.cache import _url_hash
        return _url_hash(url)

    def extract_failure_counts(self, fingerprint):
        return get_extract_failure_counts(self.db_path, fingerprint)

    def note_extract_failure(self, url, fingerprint, error=None):
        from scraper.db import bump_extract_failure
        return bump_extract_failure(self.db_path, self.url_hash(url), fingerprint,
                                    url=url, error=error)

    def clear_extract_failure(self, url, fingerprint=None):
        from scraper.db import clear_extract_failure
        clear_extract_failure(self.db_path, self.url_hash(url), fingerprint)


@pytest.mark.asyncio
async def test_a_page_that_keeps_failing_the_same_way_stops_being_paid_for(tmp_path):
    from scraper.pipeline import _Quarantine

    db = _db(tmp_path)
    cache = _FakeCache(db)
    url = "https://example.test/stubborn"
    error = ExtractorContentError("answer truncated at max_output_tokens=1500")

    q = _Quarantine(cache, "fp1", threshold=3)
    assert q.holds(url) is False
    for expected_held in (False, False, True):
        await q.note(url, error)
        assert q.holds(url) is expected_held

    # Persisted, so the next run starts already knowing.
    assert count_quarantined_pages(db, "fp1", 3) == 1
    assert _Quarantine(cache, "fp1", threshold=3).holds(url) is True

    # And released by the one change that could produce a different answer.
    assert _Quarantine(cache, "fp2", threshold=3).holds(url) is False


@pytest.mark.asyncio
async def test_only_the_answer_being_unusable_counts(tmp_path):
    """An outage, a 429 or a spent quota says nothing about the page. Counting
    those would quarantine the corpus over one bad afternoon."""
    from scraper.pipeline import _Quarantine

    cache = _FakeCache(_db(tmp_path))
    q = _Quarantine(cache, "fp1", threshold=2)
    url = "https://example.test/unlucky"

    for _ in range(5):
        await q.note(url, ExtractorUnavailableError("extract unavailable: timeout"))
    assert q.holds(url) is False


@pytest.mark.asyncio
async def test_a_page_that_succeeds_is_forgiven(tmp_path):
    from scraper.pipeline import _Quarantine

    db = _db(tmp_path)
    cache = _FakeCache(db)
    q = _Quarantine(cache, "fp1", threshold=3)
    url = "https://example.test/flaky"

    await q.note(url, ExtractorContentError("invalid JSON"))
    await q.note(url, ExtractorContentError("invalid JSON"))
    await q.forgive(url)
    assert get_extract_failure_counts(db, "fp1") == {}

    # Two more failures must not tip it over on the strength of the old ones.
    await q.note(url, ExtractorContentError("invalid JSON"))
    assert q.holds(url) is False


@pytest.mark.asyncio
async def test_a_run_without_a_cache_has_no_quarantine(tmp_path):
    """`_run_full` may be given no cache at all. There is nowhere to read the
    counts from and nowhere to write them, which is the same state as switched
    off — never an AttributeError on the first page."""
    from scraper.pipeline import _Quarantine

    q = _Quarantine(None, "fp1", threshold=3)
    assert q.holds("https://example.test/x") is False
    assert q.size == 0
    await q.note("https://example.test/x", ExtractorContentError("invalid JSON"))
    await q.forgive("https://example.test/x")


@pytest.mark.asyncio
async def test_a_zero_threshold_disables_the_quarantine(tmp_path):
    """The escape hatch, if it is ever found to be hiding work that would now
    succeed."""
    from scraper.pipeline import _Quarantine

    db = _db(tmp_path)
    q = _Quarantine(_FakeCache(db), "fp1", threshold=0)
    await q.note("https://example.test/x", ExtractorContentError("invalid JSON"))
    assert q.holds("https://example.test/x") is False
    assert get_extract_failure_counts(db, "fp1") == {}


class _Answers:
    """A provider that always fails the same way."""

    def __init__(self, exc, model="m"):
        self.exc = exc
        self.model = model
        self.provider = model
        self.quality = 50
        self.calls = 0

    async def extract(self, *args, **kwargs):
        self.calls += 1
        raise self.exc


@pytest.mark.asyncio
async def test_the_chain_blames_the_page_only_when_every_answer_agreed():
    """One flaky network error anywhere makes the attempt uncountable.

    The counter rising slowly costs a few retries; rising wrongly quarantines
    the corpus, so the chain is deliberately conservative.
    """
    from scraper.extract import FallbackExtractor

    both_content = FallbackExtractor([
        _Answers(ExtractorContentError("invalid JSON"), "a"),
        _Answers(ExtractorContentError("truncated"), "b")])
    with pytest.raises(ExtractorContentError):
        await both_content.extract("t", "Szeged", "running", "hu", "https://x.test/1")

    mixed = FallbackExtractor([
        _Answers(ExtractorContentError("invalid JSON"), "a"),
        _Answers(ExtractorUnavailableError("connection reset"), "b")])
    with pytest.raises(ExtractorUnavailableError) as caught:
        await mixed.extract("t", "Szeged", "running", "hu", "https://x.test/2")
    assert not isinstance(caught.value, ExtractorContentError)


@pytest.mark.asyncio
async def test_a_content_failure_does_not_wait_for_a_paced_provider():
    """The chain will sit out a rate-limit window for up to fifteen minutes to
    give a paced provider its turn. Worth it when the alternative is losing the
    page; not worth it when every provider that answered has said the answer
    does not fit, since three separate runs will each offer this page a
    differently-paced fleet before the quarantine takes it."""
    import time

    from scraper.extract import FallbackExtractor

    chain = FallbackExtractor([_Answers(ExtractorContentError("truncated"), "a")])

    class _AlwaysPaced:
        def has_capacity(self, _scope=None):
            return True

        def can_use(self, _extractor):
            return True

        def spec_for(self, _extractor):
            return None

        def reserve(self, _extractor):
            return False

        def note(self, *a, **kw):
            pass

        def shortest_pace_wait(self):
            return 600.0

    chain.router = _AlwaysPaced()
    started = time.monotonic()
    with pytest.raises(ExtractorContentError):
        await chain.extract("t", "Szeged", "running", "hu", "https://x.test/5")
    assert time.monotonic() - started < 1.0


@pytest.mark.asyncio
async def test_a_content_failure_is_not_retried_within_the_call():
    """The second round sends the identical prompt to the same fleet, and an
    answer that did not fit the cap will not fit it now either. Every one of
    those retries is charged in full."""
    from scraper.extract import FallbackExtractor

    provider = _Answers(ExtractorContentError("truncated"), "a")
    with pytest.raises(ExtractorContentError):
        await FallbackExtractor([provider]).extract(
            "t", "Szeged", "running", "hu", "https://x.test/3")
    assert provider.calls == 1

    transient = _Answers(ExtractorUnavailableError("timeout"), "a")
    with pytest.raises(ExtractorUnavailableError):
        await FallbackExtractor([transient]).extract(
            "t", "Szeged", "running", "hu", "https://x.test/4")
    assert transient.calls == 2, "a transient error still gets its one retry"


def test_a_quarantined_page_leaves_its_pair_alone(tmp_path):
    """Otherwise the pair returns to the loop on every run of the day to skip
    the same page again — the noise the done-pair filter exists to remove."""

    from scraper.db import (bump_extract_failure, get_fully_processed_pairs,
                            save_cache_page, save_search_cache)
    from scraper.cache import _url_hash

    db = _db(tmp_path)
    url = "https://example.test/stubborn"
    save_search_cache(db, "Szeged", "running", [url], [])
    save_cache_page(db, {
        "url_hash": _url_hash(url), "url": url, "city": "Szeged",
        "topic": "running", "domain": "example.test",
        "scraped_at": "2026-08-27T00:00:00", "data": {"raw_text": "x"},
    })

    assert get_fully_processed_pairs(db, "fp1") == set()
    bump_extract_failure(db, _url_hash(url), "fp1", url=url, error="truncated")
    assert get_fully_processed_pairs(db, "fp1", quarantine_threshold=3) == set()
    for _ in range(2):
        bump_extract_failure(db, _url_hash(url), "fp1", url=url, error="truncated")
    assert get_fully_processed_pairs(db, "fp1", quarantine_threshold=3) == {
        ("Szeged", "running")}
    # …and a threshold of 0 means the pair is still outstanding.
    assert get_fully_processed_pairs(db, "fp1", quarantine_threshold=0) == set()


def _summary(providers, **extra):
    zero = ("new_communities", "changed_communities", "change_rows", "new_venues",
            "new_persons", "pages_scraped", "pages_extracted", "searches")
    return {
        "hu": {k: 0 for k in zero},
        "intl": {k: 0 for k in zero},
        "totals": {"hu": 0, "intl": 0, "covered_pairs_hu": 0, "covered_pairs_intl": 0},
        "runs": [],
        "providers": providers,
        **extra,
    }


def test_the_report_says_how_much_is_quarantined():
    """Not an error — the guard working — but a number that must not grow
    quietly: a rising count means the cap or the prompt is wrong for a whole
    class of pages, and nothing else in the report would say so."""
    from scraper.report import build_report_html

    _, html = build_report_html("2026-08-27", _summary(
        [{"name": "groq", "used": 10, "budget": 100, "observed_limit": None,
          "rate_limits": 0, "failures": 0}],
        quarantined_pages=21), {}, None)

    assert "Karanténban 21 oldal" in html


def test_a_quarantined_page_is_not_outstanding_work():
    """The worker decides whether to run extraction from what the last pass had
    left. A page that fails caches nothing, which is how one permanently failing
    page drove ~100 runs on 2026-08-18; a quarantined page caches nothing
    either, so it has to leave this count the same way — the same empty loop,
    one guard later."""
    from scraper.pipeline import pages_worked

    assert pages_worked([{"urls_found": 5, "cache_hits_extract": 2}]) == 3
    assert pages_worked([
        {"urls_found": 5, "cache_hits_extract": 2, "extract_failed": 1}]) == 2
    assert pages_worked([
        {"urls_found": 5, "cache_hits_extract": 2, "extract_quarantined": 3}]) == 0
