import argparse
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
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


def _settings_auto_run_on_startup() -> bool:
    try:
        settings = yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8")) or {}
        schedule = settings.get("schedule", {})
        if isinstance(schedule, dict):
            return bool(schedule.get("auto_run_on_startup", False))
    except Exception:
        return False
    return False


def _settings_schedule() -> dict:
    """The full schedule: block from settings.yaml ({} on any error)."""
    try:
        settings = yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8")) or {}
        schedule = settings.get("schedule", {})
        return schedule if isinstance(schedule, dict) else {}
    except Exception:
        return {}


def _next_window_end(start: "datetime", hhmm: str) -> "datetime | None":
    """First occurrence of HH:MM (UTC) strictly after `start` — handles windows
    that cross midnight (e.g. extract 16:35 → 00:20 next day)."""
    try:
        hour, minute = (int(p) for p in hhmm.strip().split(":"))
        candidate = start.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= start:
            candidate += timedelta(days=1)
        return candidate
    except Exception:
        log = structlog.get_logger()
        log.warning("invalid_window_end", value=hhmm)
        return None


def _settings_cron_enabled() -> bool:
    """schedule.cron_enabled — off by default. When on, the cron in settings.yaml
    actually schedules runs. Pair it with an off-peak time: DeepSeek discounts
    ~50-75% between UTC 16:30 and 00:30, so a nightly run in that window halves
    the LLM bill."""
    try:
        settings = yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8")) or {}
        schedule = settings.get("schedule", {})
        if isinstance(schedule, dict):
            return bool(schedule.get("cron_enabled", False))
    except Exception:
        return False
    return False


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

    def _on_pair_start(city: str, topic: str) -> None:
        app_state.current_city = city
        app_state.current_topic = topic

    async def _cron_run(run_mode: str, mode_label: str, until_hhmm: str | None) -> None:
        """Shared scheduled runner: Hungary → Sweden → world, optionally boxed
        into a UTC time window (stop_at from until_hhmm)."""
        if app_state.is_running:
            log.info("scheduled_run_skipped", reason="already_running", mode=run_mode)
            return
        app_state.is_running = True
        app_state.current_run_mode = mode_label
        app_state._run_task = asyncio.current_task()
        started = datetime.now(timezone.utc)
        stop_at = _next_window_end(started, until_hhmm) if until_hhmm else None
        run_id = start_run(db_path, started, run_mode)
        success = False
        pair_logs: list = []
        hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
        se_cities = [c for c in (app_state.cities or []) if c.country == "Sweden"]
        intl_cities = [c for c in (app_state.cities or []) if c.country not in {"Hungary", "Sweden"}]
        try:
            for group in (hu_cities, se_cities, intl_cities):
                if not group:
                    continue
                if stop_at and datetime.now(timezone.utc) >= stop_at:
                    log.info("scheduled_run_window_closed", mode=run_mode)
                    break
                group_logs, _ = await run_pipeline(
                    group,
                    app_state.topics,
                    app_state.pipeline_cfg,
                    cache=app_state.cache_manager,
                    run_mode=run_mode,
                    on_progress=_on_progress,
                    on_pair_start=_on_pair_start,
                    stop_at=stop_at,
                )
                pair_logs += group_logs
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
        except Exception as exc:
            log.error("scheduled_run_failed", error=str(exc), mode=run_mode)
        finally:
            app_state.is_running = False
            app_state.current_phase = None
            app_state.current_url = None
            app_state.current_run_mode = None
            app_state.current_city = None
            app_state.current_topic = None
            finish_run(db_path, run_id, datetime.now(timezone.utc), success,
                       json.dumps(pair_logs) if pair_logs else None)

    async def _scheduled_run() -> None:
        await _cron_run("full", "smart", None)

    async def _search_collector_run() -> None:
        # DataForSEO gyűjtögetés: search + fetch, zero LLM (standard mode ajánlott)
        await _cron_run("search_only", "collect", _settings_schedule().get("search_until"))

    async def _offpeak_extract_run() -> None:
        # DeepSeek off-peak: extract the already-collected pages only
        await _cron_run("ai_only", "re-ai", _settings_schedule().get("extract_until"))

    scheduler = AsyncIOScheduler()
    scheduler.start()
    app_state.scheduler = scheduler
    if _settings_cron_enabled():
        minute, hour, day, month, day_of_week = _cron_fields(cron_expr)
        scheduler.add_job(
            _scheduled_run, CronTrigger(
                minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week,
            ),
            misfire_grace_time=900,
        )
        log.info("scheduler_cron_enabled", cron=cron_expr, version=app_state.version)

    schedule_cfg = _settings_schedule()
    if schedule_cfg.get("saver_enabled"):
        # Cost-saver twin jobs: search collects cheaply during the day; extraction
        # runs only in DeepSeek's off-peak window on the already-collected pages.
        # Complementary *_until windows keep the two from overlapping (single-run
        # guard would otherwise skip one).
        for job_fn, cron_key, default_cron in (
            (_search_collector_run, "search_cron", "0 1 * * *"),
            (_offpeak_extract_run, "extract_cron", "35 16 * * *"),
        ):
            m, h, d, mo, dow = _cron_fields(str(schedule_cfg.get(cron_key) or default_cron))
            scheduler.add_job(
                job_fn, CronTrigger(minute=m, hour=h, day=d, month=mo, day_of_week=dow),
                misfire_grace_time=3600,
            )
        log.info("scheduler_saver_enabled",
                 search_cron=schedule_cfg.get("search_cron"),
                 search_until=schedule_cfg.get("search_until"),
                 extract_cron=schedule_cfg.get("extract_cron"),
                 extract_until=schedule_cfg.get("extract_until"))
    if not _settings_cron_enabled() and not schedule_cfg.get("saver_enabled"):
        log.info("scheduler_started_paused", cron=cron_expr, version=app_state.version)

    async def _startup_run() -> None:
        await asyncio.sleep(5)

        last_row = get_last_run_row(db_path)
        if last_row and (last_row["finished_at"] is None or not last_row["success"]):
            # Interrupted (redeploy) or failed → re-run same mode until it succeeds
            # Revalidate can't run from here, fall back to ai_only
            prev_mode = last_row["run_mode"]
            startup_mode = prev_mode if prev_mode in ("full", "ai_only", "search_only") else "ai_only"
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
        se_cities = [c for c in (app_state.cities or []) if c.country == "Sweden"]
        intl_cities = [c for c in (app_state.cities or []) if c.country not in {"Hungary", "Sweden"}]
        try:
            pair_logs, _ = await run_pipeline(
                hu_cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                on_progress=_on_progress,
                on_pair_start=_on_pair_start,
                run_mode=startup_mode,
            )
            if se_cities:
                se_logs, _ = await run_pipeline(
                    se_cities,
                    app_state.topics,
                    app_state.pipeline_cfg,
                    cache=app_state.cache_manager,
                    on_progress=_on_progress,
                    on_pair_start=_on_pair_start,
                    run_mode=startup_mode,
                )
                pair_logs += se_logs
            if intl_cities:
                intl_logs, _ = await run_pipeline(
                    intl_cities,
                    app_state.topics,
                    app_state.pipeline_cfg,
                    cache=app_state.cache_manager,
                    on_progress=_on_progress,
                    on_pair_start=_on_pair_start,
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
            app_state.current_city = None
            app_state.current_topic = None
            finish_run(db_path, run_id, datetime.now(timezone.utc), success,
                       json.dumps(pair_logs) if pair_logs else None)

    if _settings_auto_run_on_startup():
        asyncio.create_task(_startup_run())

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
