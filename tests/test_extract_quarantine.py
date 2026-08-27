"""Storage for the extraction quarantine.

A failed extraction is deliberately never cached — caching it would record "0
communities" permanently under the current fingerprint. The bound that rule was
missing is a memory of *having tried*: `extract_failures`, keyed by page and
fingerprint. This file covers the storage and what reads it; the pipeline
behaviour that writes it lands with the rest of the guard.
"""
from pathlib import Path

from scraper.db import init_db


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


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
