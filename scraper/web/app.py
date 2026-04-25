import asyncio
import base64
import importlib.metadata
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import structlog
import yaml
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.types import ASGIApp, Receive, Scope, Send

from ..models import CommunityRecord
from ..pipeline import run_pipeline
from ..store import _normalize
from .log_stream import broadcaster
from .state import app_state

log = structlog.get_logger()

BASE_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"

_ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "almafa123")


class _BasicAuth:
    """Pure ASGI auth middleware — no response buffering, SSE works correctly."""

    def __init__(self, inner: ASGIApp) -> None:
        self._inner = inner

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self._inner(scope, receive, send)
            return

        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        auth = headers.get(b"authorization", b"").decode("latin-1")

        if auth.lower().startswith("basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode("utf-8")
                user, _, pwd = decoded.partition(":")
                if user == _ADMIN_USER and pwd == _ADMIN_PASSWORD:
                    await self._inner(scope, receive, send)
                    return
            except Exception:
                pass

        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [
                [b"www-authenticate", b'Basic realm="Community Scraper"'],
                [b"content-length", b"0"],
            ],
        })
        await send({"type": "http.response.body", "body": b""})


_fastapi = FastAPI(title="Community Scraper Admin")
app = _BasicAuth(_fastapi)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _lib_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except Exception:
        return "?"


async def _ollama_version(base_url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{base_url.rstrip('/')}/api/version")
            return resp.json().get("version", "?")
    except Exception:
        return "unreachable"


async def _searxng_status(base_url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{base_url.rstrip('/')}/search",
                                    params={"q": "test", "format": "json"})
            return "ok" if resp.status_code == 200 else f"HTTP {resp.status_code}"
    except Exception:
        return "unreachable"


async def _build_software_info() -> dict:
    cfg = app_state.pipeline_cfg
    ollama_url = cfg.ollama_url if cfg else "http://localhost:11434"
    ollama_model = cfg.ollama_model if cfg else "?"
    searxng_url = cfg.searxng_url if cfg else "http://localhost:8080"

    ollama_ver, searxng_st = await asyncio.gather(
        _ollama_version(ollama_url),
        _searxng_status(searxng_url),
    )
    return {
        "searxng": {"label": "SearXNG", "status": searxng_st},
        "ollama": {"label": "Ollama", "version": ollama_ver, "model": ollama_model},
        "python": {"label": "Python", "version": sys.version.split()[0]},
        "libs": {
            "httpx": _lib_version("httpx"),
            "trafilatura": _lib_version("trafilatura"),
            "pydantic": _lib_version("pydantic"),
            "fastapi": _lib_version("fastapi"),
        },
    }


# ── Dashboard ──────────────────────────────────────────────────────────────────

@_fastapi.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    metadata = {}
    meta_file = DATA_DIR / "metadata.json"
    if meta_file.exists():
        metadata = json.loads(meta_file.read_text(encoding="utf-8"))

    result = subprocess.run(
        ["git", "log", "--oneline", "-5"],
        cwd=str(BASE_DIR), capture_output=True, text=True,
    )
    commits = [l.strip() for l in result.stdout.strip().splitlines() if l]

    next_run = None
    if app_state.scheduler:
        jobs = app_state.scheduler.get_jobs()
        if jobs and jobs[0].next_run_time:
            next_run = jobs[0].next_run_time.strftime("%Y-%m-%d %H:%M UTC")

    cache_defaults = {}
    if app_state.pipeline_cfg:
        cache_defaults = {
            "skip_scraped": app_state.pipeline_cfg.cache_skip_scraped,
            "skip_extracted": app_state.pipeline_cfg.cache_skip_extracted,
        }

    cache_stats = {}
    if app_state.cache_manager:
        idx = app_state.cache_manager.get_index()
        cache_stats = {
            "total": len(idx),
            "with_text": sum(1 for e in idx if e["has_text"]),
            "with_extract": sum(1 for e in idx if e["extracted_at"]),
        }

    run_history = []
    if app_state.db_path:
        from ..db import get_run_history
        run_history = get_run_history(app_state.db_path, limit=10)

    return templates.TemplateResponse(request, "dashboard.html", {
        "metadata": metadata,
        "commits": commits,
        "is_running": app_state.is_running,
        "last_run_at": app_state.last_run_at,
        "next_run": next_run,
        "city_count": len(app_state.cities),
        "topic_count": len(app_state.topics),
        "cache_defaults": cache_defaults,
        "cache_stats": cache_stats,
        "run_history": run_history,
    })


# ── Results ────────────────────────────────────────────────────────────────────

@_fastapi.get("/results", response_class=HTMLResponse)
async def results(request: Request):
    metadata = {}
    meta_file = DATA_DIR / "metadata.json"
    if meta_file.exists():
        metadata = json.loads(meta_file.read_text(encoding="utf-8"))

    rows = []
    for city, topics in metadata.get("records_by_city_topic", {}).items():
        for topic, count in topics.items():
            rows.append({"city": city, "topic": topic, "count": count})

    return templates.TemplateResponse(request, "results.html", {"rows": rows})


@_fastapi.get("/results/{city}/{topic}", response_class=HTMLResponse)
async def result_detail(request: Request, city: str, topic: str):
    file = DATA_DIR / _normalize(city) / _normalize(topic) / "communities.json"
    records = []
    if file.exists():
        records = [CommunityRecord.model_validate(r) for r in json.loads(file.read_text(encoding="utf-8"))]

    return templates.TemplateResponse(request, "result_detail.html", {
        "city": city,
        "topic": topic,
        "records": records,
    })


# ── Config ─────────────────────────────────────────────────────────────────────

@_fastapi.get("/config", response_class=HTMLResponse)
async def config_page(request: Request, saved: Optional[str] = None, error: Optional[str] = None):
    software = await _build_software_info()
    return templates.TemplateResponse(request, "config.html", {
        "cities_yaml": (CONFIG_DIR / "cities.yaml").read_text(encoding="utf-8"),
        "topics_yaml": (CONFIG_DIR / "topics.yaml").read_text(encoding="utf-8"),
        "settings_yaml": (CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8"),
        "saved": saved,
        "error": error,
        "software": software,
    })


@_fastapi.post("/config/cities")
async def save_cities(request: Request, cities_yaml: str = Form(...)):
    try:
        parsed = yaml.safe_load(cities_yaml)
        assert isinstance(parsed, dict) and "cities" in parsed, "Missing 'cities' key"
        (CONFIG_DIR / "cities.yaml").write_text(cities_yaml, encoding="utf-8")
        return RedirectResponse("/config?saved=cities", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/config?error={exc}", status_code=302)


@_fastapi.post("/config/topics")
async def save_topics(request: Request, topics_yaml: str = Form(...)):
    try:
        parsed = yaml.safe_load(topics_yaml)
        assert isinstance(parsed, dict) and "topics" in parsed, "Missing 'topics' key"
        (CONFIG_DIR / "topics.yaml").write_text(topics_yaml, encoding="utf-8")
        return RedirectResponse("/config?saved=topics", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/config?error={exc}", status_code=302)


@_fastapi.post("/config/settings")
async def save_settings(request: Request, settings_yaml: str = Form(...)):
    try:
        yaml.safe_load(settings_yaml)
        (CONFIG_DIR / "settings.yaml").write_text(settings_yaml, encoding="utf-8")
        return RedirectResponse("/config?saved=settings", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/config?error={exc}", status_code=302)


# ── Logs ───────────────────────────────────────────────────────────────────────

@_fastapi.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    history = broadcaster.get_all()
    last_seq = history[-1]["seq"] if history else 0
    return templates.TemplateResponse(request, "logs.html", {
        "history": history,
        "last_seq": last_seq,
    })


@_fastapi.get("/api/logs/stream")
async def log_stream(last_seq: int = 0):
    async def generate():
        current_seq = last_seq
        tick = 0
        while True:
            await asyncio.sleep(0.5)
            tick += 1
            new_lines = broadcaster.get_lines_after(current_seq)
            if new_lines:
                for line in new_lines:
                    current_seq = line["seq"]
                    yield f"data: {json.dumps(line)}\n\n"
            elif tick % 30 == 0:
                yield ": keepalive\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Run ────────────────────────────────────────────────────────────────────────

@_fastapi.post("/api/run")
async def trigger_run(
    run_mode: str = Form("full"),
    skip_scraped: str = Form("off"),
    skip_extracted: str = Form("off"),
):
    if app_state.is_running:
        return RedirectResponse("/logs", status_code=302)

    _skip_scraped = (skip_scraped == "on")
    _skip_extracted = (skip_extracted == "on")

    async def _run() -> None:
        app_state.is_running = True
        started = datetime.now(timezone.utc)
        success = False
        pair_logs: list = []
        try:
            pair_logs = await run_pipeline(
                app_state.cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                run_mode=run_mode,
                skip_scraped=_skip_scraped,
                skip_extracted=_skip_extracted,
            )
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
        except Exception as exc:
            log.error("manual_run_failed", error=str(exc))
        finally:
            app_state.is_running = False
            if app_state.db_path:
                from ..db import record_run
                record_run(app_state.db_path, started, datetime.now(timezone.utc),
                           run_mode, success,
                           json.dumps(pair_logs) if pair_logs else None)

    asyncio.create_task(_run())
    return RedirectResponse("/logs", status_code=302)


@_fastapi.get("/api/status")
async def status():
    return {
        "is_running": app_state.is_running,
        "last_run_at": app_state.last_run_at.isoformat() if app_state.last_run_at else None,
    }


# ── Cache ──────────────────────────────────────────────────────────────────────

@_fastapi.get("/cache", response_class=HTMLResponse)
async def cache_page(request: Request):
    entries = []
    if app_state.cache_manager:
        entries = app_state.cache_manager.get_index()
    return templates.TemplateResponse(request, "cache.html", {"entries": entries})


@_fastapi.get("/cache/{url_hash}", response_class=HTMLResponse)
async def cache_detail(request: Request, url_hash: str):
    if not app_state.cache_manager:
        return RedirectResponse("/cache", status_code=302)

    entry = app_state.cache_manager.get_entry(url_hash)
    if not entry:
        return RedirectResponse("/cache", status_code=302)

    # Find matching records in the final data store
    store_records = []
    city = entry.get("city", "")
    topic = entry.get("topic", "")
    url = entry.get("url", "")
    if city and topic and url:
        file = DATA_DIR / _normalize(city) / _normalize(topic) / "communities.json"
        if file.exists():
            try:
                all_records = json.loads(file.read_text(encoding="utf-8"))
                store_records = [r for r in all_records if r.get("source_url") == url]
            except Exception:
                pass

    return templates.TemplateResponse(request, "cache_detail.html", {
        "entry": entry,
        "store_records": store_records,
    })


@_fastapi.post("/cache/{url_hash}/delete-scraped")
async def cache_delete_scraped(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_scraped(url_hash)
    return RedirectResponse("/cache", status_code=302)


@_fastapi.post("/cache/{url_hash}/delete-extracted")
async def cache_delete_extracted(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_extracted(url_hash)
    return RedirectResponse("/cache", status_code=302)


@_fastapi.post("/cache/{url_hash}/delete")
async def cache_delete_entry(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_entry(url_hash)
    return RedirectResponse("/cache", status_code=302)


# ── Run detail ─────────────────────────────────────────────────────────────────

@_fastapi.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail(request: Request, run_id: int):
    if not app_state.db_path:
        return RedirectResponse("/", status_code=302)
    from ..db import get_run_detail
    run = get_run_detail(app_state.db_path, run_id)
    if not run:
        return RedirectResponse("/", status_code=302)
    pair_logs = json.loads(run["search_log"]) if run.get("search_log") else []
    return templates.TemplateResponse(request, "run_detail.html", {
        "run": run,
        "pair_logs": pair_logs,
    })


# ── History ────────────────────────────────────────────────────────────────────

@_fastapi.get("/history", response_class=HTMLResponse)
async def history(request: Request):
    result = subprocess.run(
        ["git", "log", "--pretty=format:%h|%ai|%s", "-30"],
        cwd=str(BASE_DIR), capture_output=True, text=True,
    )
    commits = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("|", 2)
        if len(parts) == 3:
            commits.append({"hash": parts[0], "date": parts[1][:16].replace("T", " "), "message": parts[2]})

    return templates.TemplateResponse(request, "history.html", {"commits": commits})


@_fastapi.get("/history/{commit_hash}", response_class=HTMLResponse)
async def history_detail(request: Request, commit_hash: str):
    # Validate: only hex chars allowed
    if not all(c in "0123456789abcdefABCDEF" for c in commit_hash):
        return RedirectResponse("/history", status_code=302)

    stat = subprocess.run(
        ["git", "show", "--stat", "--no-color", commit_hash],
        cwd=str(BASE_DIR), capture_output=True, text=True,
    )
    diff = subprocess.run(
        ["git", "show", "--no-color", commit_hash, "--", "data/"],
        cwd=str(BASE_DIR), capture_output=True, text=True,
    )
    return templates.TemplateResponse(request, "history_detail.html", {
        "commit_hash": commit_hash,
        "stat": stat.stdout,
        "diff": diff.stdout,
    })
