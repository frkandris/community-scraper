import os
from pathlib import Path

import yaml

from .pipeline import CityConfig, PipelineConfig, TopicConfig

BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"


def _mapping(value: object, label: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    return value


def _list(value: object, label: str) -> list:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    return value


def load_config_from_docs(
    db_path: Path,
    cities_raw: object,
    topics_raw: object,
    settings_raw: object,
) -> tuple[list[CityConfig], list[TopicConfig], PipelineConfig]:
    cities_doc = _mapping(cities_raw, "cities.yaml")
    topics_doc = _mapping(topics_raw, "topics.yaml")
    settings = _mapping(settings_raw, "settings.yaml")
    cities_items = _list(cities_doc.get("cities"), "cities")
    topic_items = _list(topics_doc.get("topics"), "topics")

    pipeline_settings = settings.get("pipeline", {})
    test_mode = pipeline_settings.get("test_mode", False)
    test_cities = set(pipeline_settings.get("test_cities", []))

    all_cities = [
        CityConfig(
            name=c["name"],
            country=c.get("country", ""),
            locale=str(c["locale"]),  # str() guards against PyYAML parsing "no" as bool
            search_variants=c.get("search_variants", [c["name"]]),
        )
        for c in cities_items
    ]
    cities = [c for c in all_cities if not test_mode or c.name in test_cities]

    topics = [
        TopicConfig(name=t["name"], search_terms={str(k): v for k, v in t["search_terms"].items()})
        for t in topic_items
    ]
    cache_cfg = settings.get("cache", {})
    deepseek_cfg = settings.get("deepseek", {})
    groq_cfg = settings.get("groq", {})
    pipeline_cfg = PipelineConfig(
        searxng_url=os.environ.get("SEARXNG_URL", "http://localhost:8080"),
        ollama_url=os.environ.get("OLLAMA_URL", "http://localhost:11434"),
        ollama_model=settings["ollama"]["model"],
        ollama_temperature=settings["ollama"]["temperature"],
        ollama_timeout=settings["ollama"]["timeout_seconds"],
        ollama_max_text_chars=settings["ollama"].get("max_text_chars", 3000),
        search_results_per_query=settings["search"]["results_per_query"],
        search_max_pages=settings["search"]["max_pages_per_topic"],
        search_rate_limit=settings["search"]["rate_limit_seconds"],
        fetch_timeout=settings["fetch"]["timeout_seconds"],
        fetch_min_text_length=settings["fetch"]["min_text_length"],
        fetch_max_concurrent=settings["fetch"]["max_concurrent"],
        fetch_blocked_domains=settings["fetch"].get("blocked_domains", []),
        db_path=db_path,
        cache_skip_scraped=cache_cfg.get("skip_scraped", True),
        cache_skip_extracted=cache_cfg.get("skip_extracted", True),
        search_cache_ttl_days=cache_cfg.get("search_ttl_days", 7),
        enrich_communities=pipeline_settings.get("enrich_communities", True),
        brave_api_key=os.environ.get("BRAVE_API_KEY", ""),
        serper_api_key=os.environ.get("SERPER_DEV_API_KEY", ""),
        dataforseo_login=os.environ.get("DATAFORSEO_LOGIN", ""),
        dataforseo_password=os.environ.get("DATAFORSEO_PASSWORD", ""),
        deepseek_api_key=os.environ.get("DEEPSEEK_API_KEY", ""),
        deepseek_model=deepseek_cfg.get("model", "deepseek-chat"),
        deepseek_temperature=deepseek_cfg.get("temperature", 0.1),
        deepseek_timeout=deepseek_cfg.get("timeout_seconds", 60),
        deepseek_max_text_chars=deepseek_cfg.get("max_text_chars", 8000),
        deepseek_rate_limit_seconds=deepseek_cfg.get("rate_limit_seconds", 1.0),
        groq_api_key=os.environ.get("GROQ_API_KEY", ""),
        groq_model=groq_cfg.get("model", "llama-3.3-70b-versatile"),
        groq_temperature=groq_cfg.get("temperature", 0.1),
        groq_timeout=groq_cfg.get("timeout_seconds", 60),
        groq_max_text_chars=groq_cfg.get("max_text_chars", 4000),
        groq_rate_limit_seconds=groq_cfg.get("rate_limit_seconds", 4.0),
    )
    return cities, topics, pipeline_cfg


def load_config(db_path: Path) -> tuple[list[CityConfig], list[TopicConfig], PipelineConfig]:
    with open(CONFIG_DIR / "cities.yaml", encoding="utf-8") as f:
        cities_raw = yaml.safe_load(f)
    with open(CONFIG_DIR / "topics.yaml", encoding="utf-8") as f:
        topics_raw = yaml.safe_load(f)
    with open(CONFIG_DIR / "settings.yaml", encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    return load_config_from_docs(db_path, cities_raw, topics_raw, settings)
