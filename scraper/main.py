import argparse
import asyncio
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import structlog
import uvicorn
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .cache import CacheManager
from .db import get_last_run, init_db, record_run
from .pipeline import CityConfig, PipelineConfig, TopicConfig, run_pipeline
from .vcs import ensure_repo
from .web.app import app as web_app, templates
from .web.log_stream import broadcaster
from .web.state import app_state

BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"


def broadcast_processor(logger, method, event_dict):
    broadcaster.add_line({k: str(v) for k, v in event_dict.items()})
    return event_dict


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            broadcast_processor,
            structlog.dev.ConsoleRenderer(),
        ]
    )


def _build_version() -> str:
    version_file = BASE_DIR / "VERSION"
    if version_file.exists():
        ts = version_file.read_text().strip()
        if ts:
            return "v." + ts
    return "v.unknown"


def load_config() -> tuple[list[CityConfig], list[TopicConfig], PipelineConfig]:
    with open(CONFIG_DIR / "cities.yaml", encoding="utf-8") as f:
        cities_raw = yaml.safe_load(f)
    with open(CONFIG_DIR / "topics.yaml", encoding="utf-8") as f:
        topics_raw = yaml.safe_load(f)
    with open(CONFIG_DIR / "settings.yaml", encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    cities = [
        CityConfig(
            name=c["name"],
            locale=c["locale"],
            search_variants=c.get("search_variants", [c["name"]]),
        )
        for c in cities_raw["cities"]
    ]
    topics = [
        TopicConfig(name=t["name"], search_terms=t["search_terms"])
        for t in topics_raw["topics"]
    ]
    cache_cfg = settings.get("cache", {})
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
        commit_after_run=settings["pipeline"]["commit_after_run"],
        data_dir=DATA_DIR,
        repo_dir=BASE_DIR,
        cache_skip_scraped=cache_cfg.get("skip_scraped", True),
        cache_skip_extracted=cache_cfg.get("skip_extracted", True),
    )
    return cities, topics, pipeline_cfg


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-once", action="store_true")
    args = parser.parse_args()

    configure_logging()
    log = structlog.get_logger()

    cities, topics, pipeline_cfg = load_config()
    ensure_repo(pipeline_cfg.repo_dir)

    cache = CacheManager(DATA_DIR / "cache")

    db_path = DATA_DIR / "scraper.db"
    init_db(db_path)

    app_state.cities = cities
    app_state.topics = topics
    app_state.pipeline_cfg = pipeline_cfg
    app_state.cache_manager = cache
    app_state.db_path = db_path
    app_state.version = _build_version()
    templates.env.globals["app_version"] = app_state.version

    # Restore last_run_at from DB so it survives container restarts
    persisted = get_last_run(db_path)
    if persisted:
        app_state.last_run_at = persisted
        log.info("restored_last_run_at", last_run_at=persisted.isoformat())

    if args.run_once:
        await run_pipeline(cities, topics, pipeline_cfg, cache=cache)
        return

    cron_expr = os.environ.get("SCHEDULE_CRON", "0 3 * * *")
    minute, hour, day, month, day_of_week = cron_expr.split()

    async def _scheduled_run() -> None:
        started = datetime.now(timezone.utc)
        success = False
        pair_logs: list = []
        try:
            pair_logs = await run_pipeline(cities, topics, pipeline_cfg, cache=cache)
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
        except Exception as exc:
            log.error("scheduled_run_failed", error=str(exc))
        finally:
            record_run(db_path, started, datetime.now(timezone.utc), "full", success,
                       json.dumps(pair_logs) if pair_logs else None)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _scheduled_run,
        CronTrigger(minute=minute, hour=hour, day=day, month=month,
                    day_of_week=day_of_week, timezone="UTC"),
        misfire_grace_time=3600,
    )
    scheduler.start()
    app_state.scheduler = scheduler
    log.info("scheduler_started", cron=cron_expr, version=app_state.version)

    config = uvicorn.Config(
        web_app,
        host="0.0.0.0",
        port=8000,
        log_level="warning",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
