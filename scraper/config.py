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
            topic_tier=str(c.get("topic_tier", "full")),
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
    pipeline_cfg = PipelineConfig(
        search_results_per_query=settings["search"]["results_per_query"],
        search_max_pages=settings["search"]["max_pages_per_topic"],
        search_rate_limit=settings["search"]["rate_limit_seconds"],
        dataforseo_mode=settings["search"].get("dataforseo_mode", "live"),
        core_topics=pipeline_settings.get("core_topics", []) or [],
        fetch_timeout=settings["fetch"]["timeout_seconds"],
        fetch_min_text_length=settings["fetch"]["min_text_length"],
        fetch_max_concurrent=settings["fetch"]["max_concurrent"],
        fetch_blocked_domains=settings["fetch"].get("blocked_domains", []),
        fetch_playwright_domains=settings["fetch"].get("playwright_domains", []),
        db_path=db_path,
        cache_skip_scraped=cache_cfg.get("skip_scraped", True),
        cache_skip_extracted=cache_cfg.get("skip_extracted", True),
        search_cache_ttl_days=cache_cfg.get("search_ttl_days", 7),
        enrich_communities=pipeline_settings.get("enrich_communities", True),
        dataforseo_login=os.environ.get("DATAFORSEO_LOGIN", ""),
        dataforseo_password=os.environ.get("DATAFORSEO_PASSWORD", ""),
        deepseek_api_key=os.environ.get("DEEPSEEK_API_KEY", ""),
        deepseek_model=deepseek_cfg.get("model", "deepseek-chat"),
        deepseek_temperature=deepseek_cfg.get("temperature", 0.1),
        deepseek_timeout=deepseek_cfg.get("timeout_seconds", 60),
        deepseek_max_text_chars=deepseek_cfg.get("max_text_chars", 8000),
        deepseek_rate_limit_seconds=deepseek_cfg.get("rate_limit_seconds", 1.0),
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
