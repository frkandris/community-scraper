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


def _startup_until(startup_mode: str, schedule_cfg: dict) -> str | None:
    """Window-end HH:MM to box a startup recovery run to, matching the saver twin
    windows so a resumed collector/extractor stops exactly where its cron twin
    would. `full` (the legacy escalated steady state) stays unbounded."""
    if startup_mode == "search_only":
        return schedule_cfg.get("search_until")
    if startup_mode == "ai_only":
        return schedule_cfg.get("extract_until")
    return None


def _startup_window(startup_mode: str, schedule_cfg: dict) -> tuple[str | None, str | None]:
    """(start_hhmm, end_hhmm) for a bounded saver mode, else (None, None). Start is
    derived from the mode's cron minute/hour; end from its `*_until`."""
    if startup_mode == "search_only":
        cron = str(schedule_cfg.get("search_cron") or "0 1 * * *")
        end = schedule_cfg.get("search_until")
    elif startup_mode == "ai_only":
        cron = str(schedule_cfg.get("extract_cron") or "35 16 * * *")
        end = schedule_cfg.get("extract_until")
    else:
        return None, None
    parts = cron.split()
    try:
        start = f"{int(parts[1]):02d}:{int(parts[0]):02d}"
    except (IndexError, ValueError):
        start = None
    return start, end


def _within_window(now: "datetime", start_hhmm: str | None, end_hhmm: str | None) -> bool:
    """Is `now` (UTC) inside the [start, end) daily window? Handles windows that
    cross midnight (extract 16:35 → 00:20). Permissive (True) if a bound is
    unparseable — recovery must not be silently skipped on a config typo."""
    try:
        sh, sm = (int(p) for p in start_hhmm.strip().split(":"))
        eh, em = (int(p) for p in end_hhmm.strip().split(":"))
    except (AttributeError, ValueError):
        return True
    now_m = now.hour * 60 + now.minute
    start_m, end_m = sh * 60 + sm, eh * 60 + em
    if start_m <= end_m:
        return start_m <= now_m < end_m
    return now_m >= start_m or now_m < end_m  # crosses midnight


def _startup_plan(
    last_row: dict | None, schedule_cfg: dict, now: "datetime"
) -> tuple[str | None, "datetime | None"]:
    """Decide what a startup recovery should do → (startup_mode, stop_at).

    A `None` startup_mode means *do nothing on startup*.

    Under the saver schedule, startup is only a crash-recovery net: a mid-window
    deploy/restart kills the in-flight bounded run (search_only/ai_only), so we
    resume that same mode boxed to its window. A clean boot (last run succeeded)
    does nothing — the twin crons drive the day, and we must never launch a
    `full` LLM run outside the off-peak split. When the saver schedule is off,
    the legacy escalation is preserved unchanged (and unbounded)."""
    saver = bool(schedule_cfg.get("saver_enabled"))
    if not last_row:
        return (None, None) if saver else ("full", None)

    interrupted = last_row["finished_at"] is None or not last_row["success"]
    prev_mode = last_row["run_mode"]

    if saver:
        if not interrupted or prev_mode not in ("search_only", "ai_only"):
            return None, None
        # Only resume while still inside the interrupted mode's own window. Outside
        # it, _next_window_end would wrap to tomorrow — a ~day-long run that holds
        # the coordinator slot and starves the complementary cron. The crons drive
        # the next cycle instead.
        start, until = _startup_window(prev_mode, schedule_cfg)
        if until and not _within_window(now, start, until):
            return None, None
        return prev_mode, (_next_window_end(now, until) if until else None)

    # Legacy (saver disabled): unchanged escalation, always unbounded.
    if interrupted:
        mode = prev_mode if prev_mode in ("full", "ai_only", "search_only") else "ai_only"
    else:
        mode = {"revalidate": "ai_only", "ai_only": "full"}.get(prev_mode or "full", "full")
    return mode, None


def _saver_city_groups(cities: list) -> tuple[list, list, list, list]:
    """Expansion-first order for the bounded collector/extractor windows: the
    active expansion market (Germany) leads, then Sweden (fully indexed — the
    done-pair pre-filter fast-skips it), then the rest of the world, then
    Hungary (also fully indexed)."""
    de = [city for city in cities if city.country == "Germany"]
    se = [city for city in cities if city.country == "Sweden"]
    intl = [city for city in cities if city.country not in {"Hungary", "Sweden", "Germany"}]
    hu = [city for city in cities if city.country == "Hungary"]
    return de, se, intl, hu


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
        """Shared scheduled runner: Sweden → world → Hungary, optionally boxed
        into a UTC time window (stop_at from until_hhmm)."""
        task = asyncio.current_task()
        if not app_state.run_coordinator.reserve(mode_label, task):
            log.info("scheduled_run_skipped", reason="already_running", mode=run_mode)
            return
        started = datetime.now(timezone.utc)
        stop_at = _next_window_end(started, until_hhmm) if until_hhmm else None
        run_id = start_run(db_path, started, run_mode)
        success = False
        run_error: str | None = None
        pair_logs: list = []
        groups = _saver_city_groups(app_state.cities or [])
        try:
            # Current expansion work gets the bounded saver window first. Hungary
            # remains available as a tail pass for genuinely unfinished work.
            for group in groups:
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
            search_failures = sum(1 for row in pair_logs if row.get("search_failed"))
            extract_failures = sum(row.get("extract_failed", 0) for row in pair_logs)
            success = not (search_failures or extract_failures)
        except asyncio.CancelledError:
            run_error = "run cancelled (deploy, restart, or manual stop)"
            log.warning("scheduled_run_cancelled", mode=run_mode)
            raise
        except Exception as exc:
            run_error = str(exc)
            log.error("scheduled_run_failed", error=str(exc), mode=run_mode)
        finally:
            try:
                finish_run(db_path, run_id, datetime.now(timezone.utc), success,
                           json.dumps(pair_logs) if pair_logs else None,
                           error=run_error)
            finally:
                app_state.run_coordinator.release(task)

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
    if _settings_schedule().get("report_enabled"):
        async def _daily_report_job() -> None:
            from .report import send_daily_report
            hu = {c.name for c in (app_state.cities or []) if c.country == "Hungary"}
            try:
                await send_daily_report(db_path, hu)
            except Exception as exc:
                log.error("daily_report_failed", error=str(exc))
        rm, rh, rd, rmo, rdow = _cron_fields(str(_settings_schedule().get("report_cron") or "30 4 * * *"))
        scheduler.add_job(_daily_report_job, CronTrigger(
            minute=rm, hour=rh, day=rd, month=rmo, day_of_week=rdow), misfire_grace_time=3600)
        log.info("scheduler_report_enabled", cron=_settings_schedule().get("report_cron"))

    if not _settings_cron_enabled() and not schedule_cfg.get("saver_enabled"):
        log.info("scheduler_started_paused", cron=cron_expr, version=app_state.version)

    async def _startup_run() -> None:
        await asyncio.sleep(5)

        now = datetime.now(timezone.utc)
        last_row = get_last_run_row(db_path)
        startup_mode, stop_at = _startup_plan(last_row, _settings_schedule(), now)
        if startup_mode is None:
            log.info("startup_run_skipped", reason="nothing_to_recover",
                     last_mode=last_row["run_mode"] if last_row else None)
            return
        log.info("startup_run_triggered", last_mode=last_row["run_mode"] if last_row else None,
                 startup_mode=startup_mode, stop_at=stop_at.isoformat() if stop_at else None)

        # Mirror the cron labels so /admin/api/status shows the true active mode.
        mode_label = {"ai_only": "re-ai", "search_only": "collect"}.get(startup_mode, "smart")
        task = asyncio.current_task()
        if not app_state.run_coordinator.reserve(mode_label, task):
            log.info("startup_run_skipped", reason="already_running")
            return
        started = datetime.now(timezone.utc)
        run_id = start_run(db_path, started, startup_mode)
        success = False
        run_error: str | None = None
        pair_logs: list = []
        groups = _saver_city_groups(app_state.cities or [])
        try:
            for group in groups:
                if not group:
                    continue
                if stop_at and datetime.now(timezone.utc) >= stop_at:
                    log.info("startup_run_window_closed", mode=startup_mode)
                    break
                group_logs, _ = await run_pipeline(
                    group,
                    app_state.topics,
                    app_state.pipeline_cfg,
                    cache=app_state.cache_manager,
                    on_progress=_on_progress,
                    on_pair_start=_on_pair_start,
                    run_mode=startup_mode,
                    stop_at=stop_at,
                )
                pair_logs += group_logs
            app_state.last_run_at = datetime.now(timezone.utc)
            search_failures = sum(1 for row in pair_logs if row.get("search_failed"))
            extract_failures = sum(row.get("extract_failed", 0) for row in pair_logs)
            success = not (search_failures or extract_failures)
        except asyncio.CancelledError:
            run_error = "run cancelled (deploy, restart, or manual stop)"
            log.warning("startup_run_cancelled", mode=startup_mode)
            raise
        except Exception as exc:
            run_error = str(exc)
            log.error("startup_run_failed", error=str(exc))
        finally:
            try:
                finish_run(db_path, run_id, datetime.now(timezone.utc), success,
                           json.dumps(pair_logs) if pair_logs else None,
                           error=run_error)
            finally:
                app_state.run_coordinator.release(task)

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
