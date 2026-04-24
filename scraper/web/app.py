import asyncio
import importlib.metadata
import json
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

from ..models import CommunityRecord
from ..pipeline import run_pipeline
from ..store import _normalize
from .log_stream import broadcaster
from .state import app_state

log = structlog.get_logger()

BASE_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"

app = FastAPI(title="Community Scraper Admin")
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
            if resp.status_code == 200:
                return "ok"
            return f"HTTP {resp.status_code}"
    except Exception:
        return "unreachable"


# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
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

    cfg = app_state.pipeline_cfg
    ollama_url = cfg.ollama_url if cfg else "http://localhost:11434"
    ollama_model = cfg.ollama_model if cfg else "?"
    searxng_url = cfg.searxng_url if cfg else "http://localhost:8080"

    ollama_ver, searxng_st = await asyncio.gather(
        _ollama_version(ollama_url),
        _searxng_status(searxng_url),
    )

    software = {
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

    cache_defaults = {}
    if cfg:
        cache_defaults = {
            "skip_scraped": cfg.cache_skip_scraped,
            "skip_extracted": cfg.cache_skip_extracted,
        }

    cache_stats = {}
    if app_state.cache_manager:
        idx = app_state.cache_manager.get_index()
        cache_stats = {
            "total": len(idx),
            "with_text": sum(1 for e in idx if e["has_text"]),
            "with_extract": sum(1 for e in idx if e["extracted_at"]),
        }

    return templates.TemplateResponse(request, "dashboard.html", {
        "metadata": metadata,
        "commits": commits,
        "is_running": app_state.is_running,
        "last_run_at": app_state.last_run_at,
        "next_run": next_run,
        "city_count": len(app_state.cities),
        "topic_count": len(app_state.topics),
        "software": software,
        "cache_defaults": cache_defaults,
        "cache_stats": cache_stats,
    })


# ── Results ────────────────────────────────────────────────────────────────────

@app.get("/results", response_class=HTMLResponse)
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


@app.get("/results/{city}/{topic}", response_class=HTMLResponse)
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

@app.get("/config", response_class=HTMLResponse)
async def config_page(request: Request, saved: Optional[str] = None, error: Optional[str] = None):
    return templates.TemplateResponse(request, "config.html", {
        "cities_yaml": (CONFIG_DIR / "cities.yaml").read_text(encoding="utf-8"),
        "topics_yaml": (CONFIG_DIR / "topics.yaml").read_text(encoding="utf-8"),
        "settings_yaml": (CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8"),
        "saved": saved,
        "error": error,
    })


@app.post("/config/cities")
async def save_cities(request: Request, cities_yaml: str = Form(...)):
    try:
        parsed = yaml.safe_load(cities_yaml)
        assert isinstance(parsed, dict) and "cities" in parsed, "Missing 'cities' key"
        (CONFIG_DIR / "cities.yaml").write_text(cities_yaml, encoding="utf-8")
        return RedirectResponse("/config?saved=cities", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/config?error={exc}", status_code=302)


@app.post("/config/topics")
async def save_topics(request: Request, topics_yaml: str = Form(...)):
    try:
        parsed = yaml.safe_load(topics_yaml)
        assert isinstance(parsed, dict) and "topics" in parsed, "Missing 'topics' key"
        (CONFIG_DIR / "topics.yaml").write_text(topics_yaml, encoding="utf-8")
        return RedirectResponse("/config?saved=topics", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/config?error={exc}", status_code=302)


@app.post("/config/settings")
async def save_settings(request: Request, settings_yaml: str = Form(...)):
    try:
        yaml.safe_load(settings_yaml)
        (CONFIG_DIR / "settings.yaml").write_text(settings_yaml, encoding="utf-8")
        return RedirectResponse("/config?saved=settings", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/config?error={exc}", status_code=302)


# ── Logs ───────────────────────────────────────────────────────────────────────

@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    history = broadcaster.get_all()
    last_seq = history[-1]["seq"] if history else 0
    return templates.TemplateResponse(request, "logs.html", {
        "history": history,
        "last_seq": last_seq,
    })


@app.get("/api/logs/stream")
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

@app.post("/api/run")
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
        try:
            await run_pipeline(
                app_state.cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                run_mode=run_mode,
                skip_scraped=_skip_scraped,
                skip_extracted=_skip_extracted,
            )
            app_state.last_run_at = datetime.now(timezone.utc)
        except Exception as exc:
            log.error("manual_run_failed", error=str(exc))
        finally:
            app_state.is_running = False

    asyncio.create_task(_run())
    return RedirectResponse("/logs", status_code=302)


@app.get("/api/status")
async def status():
    return {
        "is_running": app_state.is_running,
        "last_run_at": app_state.last_run_at.isoformat() if app_state.last_run_at else None,
    }


# ── Cache ──────────────────────────────────────────────────────────────────────

@app.get("/cache", response_class=HTMLResponse)
async def cache_page(request: Request):
    entries = []
    if app_state.cache_manager:
        entries = app_state.cache_manager.get_index()
    return templates.TemplateResponse(request, "cache.html", {"entries": entries})


@app.post("/cache/{url_hash}/delete-scraped")
async def cache_delete_scraped(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_scraped(url_hash)
    return RedirectResponse("/cache", status_code=302)


@app.post("/cache/{url_hash}/delete-extracted")
async def cache_delete_extracted(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_extracted(url_hash)
    return RedirectResponse("/cache", status_code=302)


@app.post("/cache/{url_hash}/delete")
async def cache_delete_entry(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_entry(url_hash)
    return RedirectResponse("/cache", status_code=302)


# ── History ────────────────────────────────────────────────────────────────────

@app.get("/history", response_class=HTMLResponse)
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
