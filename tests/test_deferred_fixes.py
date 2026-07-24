"""Regressions for the 2026-07-24 deferred-findings batch (codex full review)."""
import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from scraper.db import get_daily_summary, get_duplicate_candidates, init_db
from scraper.extract import ExtractorUnavailableError, _parse_communities
from scraper.models import CommunityRecord
from scraper.store import save_results


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _rec(name, topic, city="Budapest", **kw):
    return CommunityRecord(
        name=name, topic=topic, city=city, locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00", **kw,
    )


def test_daily_summary_multi_topic_community_counted_once(tmp_path):
    """A community indexed under two topics shares community_id — the history
    join must not double the new/changed counts (inflated the daily email)."""
    db = _db(tmp_path)
    save_results("Budapest", "running", [_rec("Kör", "running")], db)
    save_results("Budapest", "chess", [_rec("Kör", "chess")], db)

    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    s = get_daily_summary(
        db, (now - timedelta(hours=1)).isoformat(), (now + timedelta(hours=1)).isoformat(),
        hu_cities={"Budapest"})
    assert s["hu"]["new_communities"] == 1


def test_recent_communities_multi_topic_deduped(tmp_path):
    from scraper.db import get_recently_added_communities
    db = _db(tmp_path)
    save_results("Budapest", "running", [_rec("Kör", "running")], db)
    save_results("Budapest", "chess", [_rec("Kör", "chess")], db)
    recents = get_recently_added_communities(db)
    assert len(recents) == 1


def test_duplicate_winner_is_richer_not_lexicographic(tmp_path):
    """winner_key drives the merge — it must be the richer record even when
    its key sorts after the poorer one, and re-scans must stay idempotent."""
    from scraper.duplicates import detect_all
    db = _db(tmp_path)
    # "AAA Klub" sorts first but is poor; "ZZZ AAA Klub"... need fuzzy match:
    # same name, two topics, one much richer.
    save_results("Budapest", "running", [_rec("Futó Kör", "running")], db)
    save_results("Budapest", "fitness", [_rec(
        "Futó Kör", "fitness", description="Gazdag leírás", website="https://futo.hu",
        contact="x@y.hu", meeting_schedule="kedd", fee="ingyenes")], db)

    detect_all(db)
    cands = [c for c in get_duplicate_candidates(db) if c["entity_type"] == "community"]
    assert len(cands) == 1
    from scraper.db import _community_record_key
    rich_key = _community_record_key("Futó Kör", "Budapest", "fitness")
    assert cands[0]["winner_key"] == rich_key, \
        "winner must be the richer (fitness) record regardless of key order"

    detect_all(db)  # idempotent even though key order is no longer canonical
    assert len([c for c in get_duplicate_candidates(db) if c["entity_type"] == "community"]) == 1


def test_stale_pending_candidate_orientation_corrected(tmp_path):
    """Pending rows created before the richer-wins change get their winner
    flipped in place on re-scan, so a merge keeps the right record."""
    from scraper.db import _community_record_key, insert_duplicate_candidate
    from scraper.duplicates import detect_all
    db = _db(tmp_path)
    save_results("Budapest", "running", [_rec("Futó Kör", "running")], db)
    save_results("Budapest", "fitness", [_rec(
        "Futó Kör", "fitness", description="Gazdag leírás", website="https://futo.hu",
        contact="x@y.hu", meeting_schedule="kedd", fee="ingyenes")], db)
    poor_key = _community_record_key("Futó Kör", "Budapest", "running")
    rich_key = _community_record_key("Futó Kör", "Budapest", "fitness")
    # legacy row: lexicographic orientation happened to keep the poor record
    insert_duplicate_candidate(db, "community", "", "", poor_key, rich_key, 1.0, "manual")

    detect_all(db)
    cands = [c for c in get_duplicate_candidates(db) if c["entity_type"] == "community"]
    assert len(cands) == 1
    assert cands[0]["winner_key"] == rich_key


def test_leader_cleanup_spares_ai_extracted_persons(tmp_path):
    from scraper.db import (delete_leader_persons_for_community,
                            get_persons_for_community, upsert_persons)
    from scraper.models import PersonRecord
    db = _db(tmp_path)
    ai_person = PersonRecord(
        name="Kiss Anna", role="leader", city="Budapest", topic="running",
        community_name="Futó Kör", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00")
    synth = PersonRecord(
        name="Nagy Béla", role="leader", city="Budapest", topic="running",
        community_name="Futó Kör", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00")
    upsert_persons(db, [ai_person.model_dump()])
    upsert_persons(db, [{**synth.model_dump(), "origin": "leader_field"}])

    # stale-cleanup mode: only the synthesized row goes
    delete_leader_persons_for_community(db, "Futó Kör", "Budapest", only_synthesized=True)
    names = [p["name"] for p in get_persons_for_community(db, "Futó Kör", "Budapest")]
    assert names == ["Kiss Anna"], "AI-extracted leader must survive stale cleanup"


def test_malformed_llm_json_raises_instead_of_empty(tmp_path):
    with pytest.raises(ExtractorUnavailableError):
        _parse_communities("Sorry, I cannot help with that.", "Budapest", "running",
                           "hu", "https://a.test")
    with pytest.raises(ExtractorUnavailableError):
        _parse_communities('{"communities": "not-a-list"}', "Budapest", "running",
                           "hu", "https://a.test")
    # a well-formed empty result is still a legitimate empty extraction
    assert _parse_communities('{"communities": []}', "Budapest", "running",
                              "hu", "https://a.test") == []


def test_scrape_submitted_url_filters_non_joinable(tmp_path):
    from scraper.db import get_communities
    from scraper.pipeline import PipelineConfig, scrape_submitted_url
    db = _db(tmp_path)
    cfg = PipelineConfig(
        search_results_per_query=5, search_max_pages=2, search_rate_limit=1.0,
        fetch_timeout=15, fetch_min_text_length=10, fetch_max_concurrent=3,
        fetch_blocked_domains=[], db_path=db,
    )

    class StubExtractor:
        def __init__(self, primaries): ...
        async def extract(self, **kwargs):
            return [_rec("Nyitott Kör", "running", joinable=True),
                    _rec("Zárt Профи Klub", "running", joinable=False)]

    async def fake_fetch(url, *a, **k):
        return "Elég hosszú oldalszöveg a teszthez."

    with patch("scraper.pipeline.FallbackExtractor", StubExtractor), \
         patch("scraper.pipeline.fetch_and_clean", fake_fetch):
        ok = asyncio.run(scrape_submitted_url(db, cfg, "Budapest", "running",
                                              "https://klub.test/x"))
    assert ok
    names = [c["name"] for c in get_communities(db, "Budapest", "running")]
    assert "Nyitott Kör" in names and len(names) == 1, \
        "non-joinable records must be filtered like in the main pipeline"
