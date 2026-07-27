"""Short + long description enrichment: selection, write, durability (LLM mocked)."""
import asyncio
from pathlib import Path

from scraper.cache import CacheManager
from scraper.db import get_communities, get_enrichment_candidates, init_db
from scraper.enrich import enrich_batch, validate
from scraper.models import CommunityRecord
from scraper.store import save_results

HU = {"Budapest"}
LONG = "Bővített, hasznos, valódi tartalommal teli leírás a közösségről a városban. " * 8
SHORT = "Zenei kör Budapesten"


def _rec(description="Rövid.", **kw):
    return CommunityRecord(
        name="Zenei Kör", topic="music", city="Budapest", locale="hu",
        source_url="https://klub.test/a", extracted_at="2026-01-01T00:00:00+00:00",
        description=description, **kw)


def _setup(base: Path, description="Rövid.", raw_text="Forrásszöveg. " * 60):
    base = Path(base)
    base.mkdir(parents=True, exist_ok=True)
    db = base / "scraper.db"
    init_db(db)
    save_results("Budapest", "music", [_rec(description)], db)
    if raw_text is not None:
        CacheManager(db).save_scraped("https://klub.test/a", raw_text, "Budapest", "music")
    return db


class FakeExtractor:
    exhausted = False

    def __init__(self, short=SHORT, long=LONG):
        self.short, self.long, self.calls = short, long, 0

    async def write_descriptions(self, name, city, topic, locale, page_text):
        self.calls += 1
        return {"short_description": self.short, "long_description": self.long}


def test_selects_unenriched_with_source(tmp_path):
    db = _setup(tmp_path)
    cands = get_enrichment_candidates(db, HU, limit=10)
    assert len(cands) == 1 and cands[0]["name"] == "Zenei Kör"
    assert cands[0]["source_urls"] == ["https://klub.test/a"]
    # other city excluded
    assert get_enrichment_candidates(db, {"Debrecen"}, limit=10) == []


def test_batch_writes_short_and_long(tmp_path):
    db = _setup(tmp_path)
    ex = FakeExtractor()
    stats = asyncio.run(enrich_batch(db, ex, HU, limit=10, fetch_missing=False))
    assert stats["enriched"] == 1 and ex.calls == 1
    rec = get_communities(db, "Budapest", "music")[0]
    assert rec["short_description"] == SHORT
    assert rec["long_description"] == LONG.strip()
    assert rec["description"] == "Rövid."  # base extraction field untouched


def test_dry_run_does_not_write(tmp_path):
    db = _setup(tmp_path)
    stats = asyncio.run(enrich_batch(db, FakeExtractor(), HU, limit=10, dry_run=True, fetch_missing=False))
    assert stats["enriched"] == 1
    assert not get_communities(db, "Budapest", "music")[0].get("long_description")


def test_enriched_row_not_reselected(tmp_path):
    db = _setup(tmp_path)
    asyncio.run(enrich_batch(db, FakeExtractor(), HU, limit=10, fetch_missing=False))
    # long_description now set → no longer a candidate
    assert get_enrichment_candidates(db, HU, limit=10) == []
    ex2 = FakeExtractor()
    stats = asyncio.run(enrich_batch(db, ex2, HU, limit=10, fetch_missing=False))
    assert stats["enriched"] == 0 and ex2.calls == 0


def test_enrichment_durable_across_reextraction(tmp_path):
    """A later save_results (re-extraction) with a fresh record lacking the enriched
    fields must NOT drop them — _merge_source_urls carries them forward."""
    db = _setup(tmp_path)
    asyncio.run(enrich_batch(db, FakeExtractor(), HU, limit=10, fetch_missing=False))
    # simulate a re-extraction producing the base record again (no short/long)
    save_results("Budapest", "music", [_rec("Frissen kinyert rövid leírás.")], db)
    rec = get_communities(db, "Budapest", "music")[0]
    assert rec["long_description"] == LONG.strip()   # preserved
    assert rec["short_description"] == SHORT          # preserved
    assert rec["description"] == "Frissen kinyert rövid leírás."  # base updated


def test_skips_refusal_or_thin_long(tmp_path):
    db = _setup(tmp_path)
    ex = FakeExtractor(short="x", long="Sajnos nem tudok segíteni.")
    stats = asyncio.run(enrich_batch(db, ex, HU, limit=10, fetch_missing=False))
    assert stats["enriched"] == 0 and stats["skipped"] == 1
    assert not get_communities(db, "Budapest", "music")[0].get("long_description")


def test_fetch_fallback_when_no_raw_text(tmp_path, monkeypatch):
    db = _setup(tmp_path, raw_text=None)  # no cached raw_text
    assert get_enrichment_candidates(db, HU, limit=10)[0]["raw_text"] is None

    async def fake_fetch(url, blocked, **kw):
        return "Frissen letöltött forrásszöveg a klubról. " * 30

    monkeypatch.setattr("scraper.enrich.fetch_and_clean", fake_fetch)
    stats = asyncio.run(enrich_batch(db, FakeExtractor(), HU, limit=10, fetch_missing=True))
    assert stats["enriched"] == 1
    assert get_communities(db, "Budapest", "music")[0]["long_description"] == LONG.strip()


def test_enriched_only_page_is_indexable_and_in_sitemap(tmp_path):
    """A community with an empty original description but a long_description must be
    indexable (no noindex) and present in the sitemap (codex P1)."""
    from fastapi.testclient import TestClient
    from scraper.pipeline import CityConfig
    from scraper.web import app as web_app
    from scraper.web.state import app_state
    db = _setup(tmp_path, description="")            # empty base description
    asyncio.run(enrich_batch(db, FakeExtractor(), HU, limit=5, fetch_missing=False))
    old = (app_state.db_path, app_state.cities)
    app_state.db_path = db
    app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
    try:
        c = TestClient(web_app.app)
        page = c.get("/budapest/zenei-kor", headers={"host": "kozossegek.com"}).text
        assert 'name="robots" content="noindex"' not in page
        assert LONG.strip()[:40] in page
        assert "/budapest/zenei-kor" in c.get("/sitemap.xml", headers={"host": "kozossegek.com"}).text
    finally:
        app_state.db_path, app_state.cities = old


def test_failed_attempt_marked_and_not_reselected(tmp_path):
    """A skipped (junk/refusal) candidate is marked so it doesn't block later
    communities every batch, but is retryable once the window passes (codex P1)."""
    db = _setup(tmp_path)
    s1 = asyncio.run(enrich_batch(
        db, FakeExtractor(long="Sajnos nem tudok segíteni."), HU, limit=10, fetch_missing=False))
    assert s1["skipped"] == 1
    # within the retry window → not re-selected
    assert get_enrichment_candidates(db, HU, limit=10) == []
    # window elapsed → retryable again
    assert len(get_enrichment_candidates(db, HU, limit=10, retry_after_days=0)) == 1


def test_validate():
    assert validate(SHORT, LONG) == (SHORT, LONG.strip())
    assert validate("x", "too short") is None                    # long too short
    assert validate("x", "As an AI I cannot " + "w " * 80) is None  # refusal
    # missing short → derived from long's first sentence
    out = validate("", "Első mondat. " + "szó " * 80)
    assert out and out[0].startswith("Első mondat")


def test_validate_accepts_cjk_by_char_count():
    # Japanese: ~few spaces, so word count is tiny but char count is high — must pass.
    jp_long = "東京" * 130  # 260 chars, ~0 spaces
    out = validate("東京の走るクラブ", jp_long)
    assert out is not None and out[1] == jp_long


def test_new_null_fields_do_not_change_fingerprint():
    from scraper.db import _community_content_fingerprint as fp
    base = {"name": "X", "description": "leírás", "extracted_at": "2026-01-01"}
    with_nulls = {**base, "short_description": None, "long_description": None,
                  "enrich_attempted_at": "2026-07-27T00:00:00+00:00",
                  "extracted_at": "2026-07-27"}  # volatile fields differ too
    assert fp(base) == fp(with_nulls)  # adding null/volatile fields ≠ content change
