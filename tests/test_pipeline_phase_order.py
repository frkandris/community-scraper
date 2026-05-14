import asyncio
from unittest.mock import patch
from pathlib import Path

from scraper.pipeline import (
    CityConfig,
    PipelineConfig,
    TopicConfig,
    run_pipeline,
)


def _cfg(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        searxng_url="http://localhost:8888",
        ollama_url="http://localhost:11434",
        ollama_model="llama3",
        ollama_temperature=0.1,
        ollama_timeout=30,
        ollama_max_text_chars=6000,
        search_results_per_query=5,
        search_max_pages=2,
        search_rate_limit=1.0,
        fetch_timeout=15,
        fetch_min_text_length=100,
        fetch_max_concurrent=3,
        fetch_blocked_domains=[],
        db_path=tmp_path / "scraper.db",
    )


def test_full_mode_runs_reai_before_search(tmp_path):
    """Smart mode must run re-ai phase first, then full search phase."""
    from scraper.db import init_db
    db = tmp_path / "scraper.db"
    init_db(db)

    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]
    cfg = _cfg(tmp_path)

    call_order = []

    async def fake_ai_only(*args, **kwargs):
        call_order.append("ai_only")
        return 0, []

    async def fake_full(*args, **kwargs):
        call_order.append("full")
        return 0, []

    with patch("scraper.pipeline._run_ai_only", side_effect=fake_ai_only), \
         patch("scraper.pipeline._run_full", side_effect=fake_full), \
         patch("scraper.pipeline.get_covered_pairs", return_value=set()), \
         patch("scraper.pipeline.OllamaExtractor"):
        asyncio.run(run_pipeline(cities, topics, cfg, cache=None, run_mode="full"))

    assert call_order[0] == "ai_only", f"Expected ai_only first, got: {call_order}"
    assert "full" in call_order, f"Expected full to be called, got: {call_order}"


def test_ai_only_mode_does_not_run_full(tmp_path):
    """ai_only mode must not trigger the full search phase."""
    from scraper.db import init_db
    db = tmp_path / "scraper.db"
    init_db(db)

    cities = [CityConfig(name="Budapest", locale="hu", search_variants=[])]
    topics = [TopicConfig(name="running", search_terms={"hu": ["futás"]})]
    cfg = _cfg(tmp_path)

    call_order = []

    async def fake_ai_only(*args, **kwargs):
        call_order.append("ai_only")
        return 0, []

    async def fake_full(*args, **kwargs):
        call_order.append("full")
        return 0, []

    with patch("scraper.pipeline._run_ai_only", side_effect=fake_ai_only), \
         patch("scraper.pipeline._run_full", side_effect=fake_full), \
         patch("scraper.pipeline.OllamaExtractor"):
        asyncio.run(run_pipeline(cities, topics, cfg, cache=None, run_mode="ai_only"))

    assert call_order == ["ai_only"], f"Expected only ai_only, got: {call_order}"
