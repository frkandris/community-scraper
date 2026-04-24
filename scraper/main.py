import argparse
import asyncio
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import structlog
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .pipeline import CityConfig, PipelineConfig, TopicConfig, run_pipeline
from .vcs import ensure_repo

log = structlog.get_logger()

BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"


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
        TopicConfig(
            name=t["name"],
            search_terms=t["search_terms"],
        )
        for t in topics_raw["topics"]
    ]

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
    )
    return cities, topics, pipeline_cfg


def start_health_server(port: int = 8000) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, *args):
            pass

    server = HTTPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("health_server_started", port=port)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-once", action="store_true", help="Run pipeline once and exit")
    args = parser.parse_args()

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ]
    )

    cities, topics, pipeline_cfg = load_config()
    ensure_repo(pipeline_cfg.repo_dir)

    if args.run_once:
        await run_pipeline(cities, topics, pipeline_cfg)
        return

    start_health_server()

    cron_expr = os.environ.get("SCHEDULE_CRON", "0 3 * * *")
    minute, hour, day, month, day_of_week = cron_expr.split()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_pipeline,
        CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone="UTC",
        ),
        args=[cities, topics, pipeline_cfg],
        misfire_grace_time=3600,
    )
    scheduler.start()
    log.info("scheduler_started", cron=cron_expr)

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
