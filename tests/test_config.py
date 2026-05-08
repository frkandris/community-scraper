from pathlib import Path

import pytest

from scraper.config import load_config_from_docs


def minimal_settings() -> dict:
    return {
        "ollama": {
            "model": "gemma3:4b",
            "temperature": 0.1,
            "timeout_seconds": 180,
            "max_text_chars": 6000,
        },
        "search": {
            "results_per_query": 10,
            "max_pages_per_topic": 5,
            "rate_limit_seconds": 1.5,
        },
        "fetch": {
            "timeout_seconds": 15,
            "min_text_length": 100,
            "max_concurrent": 3,
            "blocked_domains": [],
        },
        "pipeline": {
            "test_mode": False,
            "test_cities": [],
            "enrich_communities": True,
        },
        "cache": {
            "skip_scraped": True,
            "skip_extracted": True,
            "search_ttl_days": 7,
        },
    }


def test_load_config_from_docs_builds_runtime_config(tmp_path: Path):
    cities, topics, cfg = load_config_from_docs(
        tmp_path / "scraper.db",
        {
            "cities": [
                {
                    "name": "Budapest",
                    "country": "Hungary",
                    "locale": "hu",
                    "search_variants": ["Budapest"],
                }
            ]
        },
        {"topics": [{"name": "running", "search_terms": {"hu": ["futás"]}}]},
        minimal_settings(),
    )

    assert cities[0].name == "Budapest"
    assert topics[0].name == "running"
    assert cfg.ollama_model == "gemma3:4b"


def test_load_config_from_docs_rejects_invalid_shapes(tmp_path: Path):
    with pytest.raises(ValueError, match="cities must be a list"):
        load_config_from_docs(
            tmp_path / "scraper.db",
            {"cities": {"name": "Budapest"}},
            {"topics": []},
            minimal_settings(),
        )
