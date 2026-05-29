import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import structlog
import uvicorn
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .cache import CacheManager
from .config import CONFIG_DIR, load_config
from .db import finish_run, get_last_run, get_last_run_row, init_db, start_run
from .pipeline import run_pipeline
from .web.app import app as web_app, templates
from .web.log_stream import broadcaster
from .web.state import app_state

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_SCHEDULE_CRON = "*/15 * * * *"


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


def _cron_fields(cron_expr: str, fallback: str = DEFAULT_SCHEDULE_CRON) -> tuple[str, str, str, str, str]:
    fields = cron_expr.split()
    if len(fields) == 5:
        return fields[0], fields[1], fields[2], fields[3], fields[4]

    log = structlog.get_logger()
    log.warning("invalid_cron_expression", cron=cron_expr, fallback=fallback)
    fallback_fields = fallback.split()
    if len(fallback_fields) != 5:
        raise ValueError(f"Fallback cron must have 5 fields: {fallback}")
    return (
        fallback_fields[0],
        fallback_fields[1],
        fallback_fields[2],
        fallback_fields[3],
        fallback_fields[4],
    )


def _settings_cron() -> str:
    try:
        settings = yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8")) or {}
        schedule = settings.get("schedule", {})
        if isinstance(schedule, dict):
            return str(schedule.get("cron") or DEFAULT_SCHEDULE_CRON)
    except Exception:
        return DEFAULT_SCHEDULE_CRON
    return DEFAULT_SCHEDULE_CRON


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-once", action="store_true")
    args = parser.parse_args()

    configure_logging()
    log = structlog.get_logger()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db_path = DATA_DIR / "scraper.db"
    init_db(db_path)

    cities, topics, pipeline_cfg = load_config(db_path)
    cache = CacheManager(db_path)

    app_state.cities = cities
    app_state.topics = topics
    app_state.pipeline_cfg = pipeline_cfg
    app_state.cache_manager = cache
    app_state.db_path = db_path
    app_state.version = _build_version()
    templates.env.globals["app_version"] = app_state.version

    persisted = get_last_run(db_path)
    if persisted:
        app_state.last_run_at = persisted
        log.info("restored_last_run_at", last_run_at=persisted.isoformat())

    if args.run_once:
        await run_pipeline(cities, topics, pipeline_cfg, cache=cache)
        return

    cron_expr = os.environ.get("SCHEDULE_CRON") or _settings_cron()
    minute, hour, day, month, day_of_week = _cron_fields(cron_expr)

    def _on_progress(phase: str | None, url: str | None) -> None:
        app_state.current_phase = phase
        app_state.current_url = url

    async def _scheduled_run() -> None:
        if app_state.is_running:
            log.info("scheduled_run_skipped", reason="already_running")
            return
        app_state.is_running = True
        app_state.current_run_mode = "smart"
        app_state._run_task = asyncio.current_task()
        started = datetime.now(timezone.utc)
        run_id = start_run(db_path, started, "full")
        success = False
        pair_logs: list = []
        hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
        intl_cities = [c for c in (app_state.cities or []) if c.country != "Hungary"]
        try:
            pair_logs, _ = await run_pipeline(
                hu_cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                on_progress=_on_progress,
            )
            if intl_cities:
                intl_logs, _ = await run_pipeline(
                    intl_cities,
                    app_state.topics,
                    app_state.pipeline_cfg,
                    cache=app_state.cache_manager,
                    on_progress=_on_progress,
                )
                pair_logs += intl_logs
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
        except Exception as exc:
            log.error("scheduled_run_failed", error=str(exc))
        finally:
            app_state.is_running = False
            app_state.current_phase = None
            app_state.current_url = None
            app_state.current_run_mode = None
            finish_run(db_path, run_id, datetime.now(timezone.utc), success,
                       json.dumps(pair_logs) if pair_logs else None)

    scheduler = AsyncIOScheduler()
    scheduler.start()
    app_state.scheduler = scheduler
    log.info("scheduler_started_paused", cron=cron_expr, version=app_state.version)

    async def _startup_run() -> None:
        await asyncio.sleep(5)

        last_row = get_last_run_row(db_path)
        if last_row and (last_row["finished_at"] is None or not last_row["success"]):
            # Interrupted (redeploy) or failed → re-run same mode until it succeeds
            # Revalidate can't run from here, fall back to ai_only
            prev_mode = last_row["run_mode"]
            startup_mode = prev_mode if prev_mode in ("full", "ai_only") else "ai_only"
            reason = "interrupted" if last_row["finished_at"] is None else "failed"
            log.info("startup_run_retry", reason=reason, prev_mode=prev_mode, startup_mode=startup_mode)
        else:
            last_mode = last_row["run_mode"] if last_row else None
            # Completed successfully → progress: revalidate → ai_only → full → full
            startup_mode = {"revalidate": "ai_only", "ai_only": "full"}.get(last_mode or "full", "full")
            log.info("startup_run_triggered", last_mode=last_mode, startup_mode=startup_mode)

        if app_state.is_running:
            log.info("startup_run_skipped", reason="already_running")
            return
        app_state.is_running = True
        app_state.current_run_mode = "re-ai" if startup_mode == "ai_only" else "smart"
        app_state._run_task = asyncio.current_task()
        started = datetime.now(timezone.utc)
        run_id = start_run(db_path, started, startup_mode)
        success = False
        pair_logs: list = []
        hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
        intl_cities = [c for c in (app_state.cities or []) if c.country != "Hungary"]
        try:
            pair_logs, _ = await run_pipeline(
                hu_cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                on_progress=_on_progress,
                run_mode=startup_mode,
            )
            if intl_cities:
                intl_logs, _ = await run_pipeline(
                    intl_cities,
                    app_state.topics,
                    app_state.pipeline_cfg,
                    cache=app_state.cache_manager,
                    on_progress=_on_progress,
                    run_mode=startup_mode,
                )
                pair_logs += intl_logs
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
        except Exception as exc:
            log.error("startup_run_failed", error=str(exc))
        finally:
            app_state.is_running = False
            app_state.current_phase = None
            app_state.current_url = None
            app_state.current_run_mode = None
            finish_run(db_path, run_id, datetime.now(timezone.utc), success,
                       json.dumps(pair_logs) if pair_logs else None)

    # asyncio.create_task(_startup_run())

    config = uvicorn.Config(
        web_app,
        host=os.environ.get("HOST", "127.0.0.1"),
        port=8000,
        log_level="warning",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
