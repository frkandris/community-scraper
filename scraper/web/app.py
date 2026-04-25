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
from urllib.parse import quote as _url_quote

import httpx
import structlog
import yaml
from fastapi import APIRouter, FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
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

TOPIC_ICONS: dict[str, str] = {
    "running": "person-simple-run",
    "board_games": "puzzle-piece",
    "choir": "microphone-stage",
    "dance": "person-simple",
    "cycling": "bicycle",
    "hiking": "mountains",
    "yoga": "flower-lotus",
    "photography": "camera",
    "book_club": "books",
    "chess": "chess-knight",
    "cooking": "cooking-pot",
    "theater": "theater-masks",
    "music": "music-notes",
    "martial_arts": "sword",
    "gaming": "game-controller",
    "volunteering": "hand-heart",
    "language_exchange": "translate",
    "art": "paint-brush",
    "meditation": "spiral",
    "swimming": "waves",
    "community_general": "users-three",
    "gardening": "plant",
    "film_club": "popcorn",
    "trivia": "lightbulb",
    "sustainability": "recycle",
    "crafts": "scissors",
    "fitness": "barbell",
}

TOPIC_LABELS: dict[str, str] = {
    "running": "Running",
    "board_games": "Board Games",
    "choir": "Choir",
    "dance": "Dance",
    "cycling": "Cycling",
    "hiking": "Hiking",
    "yoga": "Yoga",
    "photography": "Photography",
    "book_club": "Book Club",
    "chess": "Chess",
    "cooking": "Cooking",
    "theater": "Theater",
    "music": "Music",
    "martial_arts": "Martial Arts",
    "gaming": "Gaming",
    "volunteering": "Volunteering",
    "language_exchange": "Language Exchange",
    "art": "Art",
    "meditation": "Meditation",
    "swimming": "Swimming",
    "community_general": "Communities",
    "gardening": "Gardening",
    "film_club": "Film Club",
    "trivia": "Trivia & Quizzes",
    "sustainability": "Sustainability",
    "crafts": "Crafts & Making",
    "fitness": "Fitness",
}


class _BasicAuth:
    """Pure ASGI auth middleware — protects /admin/* only, no SSE buffering."""

    def __init__(self, inner: ASGIApp) -> None:
        self._inner = inner

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self._inner(scope, receive, send)
            return

        path = scope.get("path", "")
        if not path.startswith("/admin"):
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
                [b"www-authenticate", b'Basic realm="Community Scraper Admin"'],
                [b"content-length", b"0"],
            ],
        })
        await send({"type": "http.response.body", "body": b""})


_fastapi = FastAPI(title="Community Scraper")
app = _BasicAuth(_fastapi)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.filters["urlencode"] = lambda s: _url_quote(str(s), safe="")

_static_dir = Path(__file__).parent / "static"
_static_dir.mkdir(exist_ok=True)
_fastapi.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


admin = APIRouter(prefix="/admin")


# ── Helpers ────────────────────────────────────────────────────────────────────

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


CITY_COORDS: dict[str, tuple[float, float]] = {
    # Hungary
    "Szentendre": (47.67, 19.07), "Budapest": (47.50, 19.04), "Debrecen": (47.53, 21.63),
    "Miskolc": (48.10, 20.78), "Győr": (47.68, 17.63), "Pécs": (46.07, 18.23),
    "Szeged": (46.25, 20.15), "Kecskemét": (46.91, 19.69), "Nyíregyháza": (47.95, 21.72),
    "Székesfehérvár": (47.19, 18.41),
    # Austria
    "Vienna": (48.21, 16.37), "Graz": (47.07, 15.44), "Salzburg": (47.80, 13.04),
    # Germany
    "Berlin": (52.52, 13.40), "Munich": (48.14, 11.58), "Hamburg": (53.55, 10.00),
    "Frankfurt": (50.11, 8.68), "Cologne": (50.94, 6.96), "Düsseldorf": (51.23, 6.78),
    "Stuttgart": (48.78, 9.18), "Leipzig": (51.34, 12.38), "Nürnberg": (49.45, 11.08),
    "Dresden": (51.05, 13.74), "Hannover": (52.37, 9.74),
    # Switzerland
    "Zurich": (47.38, 8.54), "Bern": (46.95, 7.45), "Geneva": (46.20, 6.15),
    # UK
    "London": (51.51, -0.13), "Manchester": (53.48, -2.24), "Birmingham": (52.48, -1.90),
    "Edinburgh": (55.95, -3.19), "Bristol": (51.45, -2.59),
    # Ireland
    "Dublin": (53.33, -6.25),
    # USA
    "New York": (40.71, -74.01), "Los Angeles": (34.05, -118.24), "Chicago": (41.88, -87.63),
    "San Francisco": (37.77, -122.42), "Seattle": (47.61, -122.33), "Boston": (42.36, -71.06),
    "Austin": (30.27, -97.74), "Portland": (45.52, -122.68), "Denver": (39.74, -104.98),
    "Miami": (25.77, -80.19), "Atlanta": (33.75, -84.39), "Minneapolis": (44.98, -93.27),
    "Philadelphia": (39.95, -75.16), "Detroit": (42.33, -83.05),
    # Canada
    "Toronto": (43.65, -79.38), "Vancouver": (49.25, -123.12), "Montreal": (45.51, -73.55),
    "Calgary": (51.04, -114.07), "Ottawa": (45.42, -75.70),
    # Australia
    "Sydney": (-33.87, 151.21), "Melbourne": (-37.81, 144.96), "Brisbane": (-27.47, 153.02),
    "Perth": (-31.95, 115.86), "Adelaide": (-34.93, 138.60),
    # New Zealand
    "Auckland": (-36.85, 174.76), "Wellington": (-41.29, 174.78),
    # France
    "Paris": (48.86, 2.35), "Lyon": (45.75, 4.83), "Marseille": (43.30, 5.37),
    "Toulouse": (43.60, 1.44), "Nice": (43.71, 7.26), "Bordeaux": (44.84, -0.58),
    "Strasbourg": (48.58, 7.75), "Nantes": (47.22, -1.55),
    # Belgium
    "Brussels": (50.85, 4.35), "Antwerp": (51.22, 4.40),
    # Netherlands
    "Amsterdam": (52.37, 4.90), "Rotterdam": (51.92, 4.47), "The Hague": (52.08, 4.31),
    # Spain
    "Madrid": (40.42, -3.70), "Barcelona": (41.39, 2.17), "Seville": (37.39, -5.99),
    "Valencia": (39.47, -0.38), "Bilbao": (43.26, -2.93), "Zaragoza": (41.65, -0.88),
    # Portugal
    "Lisbon": (38.72, -9.14), "Porto": (41.16, -8.63),
    # Italy
    "Rome": (41.90, 12.50), "Milan": (45.47, 9.19), "Florence": (43.77, 11.25),
    "Turin": (45.07, 7.69), "Naples": (40.85, 14.27), "Bologna": (44.49, 11.34),
    # Poland
    "Warsaw": (52.23, 21.01), "Krakow": (50.06, 19.94), "Wroclaw": (51.11, 17.04),
    "Gdansk": (54.35, 18.65), "Poznan": (52.41, 16.93),
    # Czech Republic
    "Prague": (50.08, 14.44), "Brno": (49.19, 16.61),
    # Slovakia
    "Bratislava": (48.15, 17.11),
    # Hungary → Slovenia
    "Ljubljana": (46.05, 14.51),
    # Romania
    "Bucharest": (44.43, 26.10), "Cluj-Napoca": (46.77, 23.59),
    # Serbia
    "Belgrade": (44.82, 20.46),
    # Croatia
    "Zagreb": (45.81, 15.98),
    # Bulgaria
    "Sofia": (42.70, 23.32),
    # Ukraine
    "Kyiv": (50.45, 30.52),
    # Baltic
    "Riga": (56.95, 24.11), "Tallinn": (59.44, 24.75), "Vilnius": (54.69, 25.28),
    # Greece
    "Athens": (37.98, 23.73), "Thessaloniki": (40.64, 22.94),
    # Scandinavia
    "Copenhagen": (55.68, 12.57), "Stockholm": (59.33, 18.07), "Oslo": (59.91, 10.75),
    "Helsinki": (60.17, 24.94), "Gothenburg": (57.71, 11.97), "Malmö": (55.61, 13.00),
    # Turkey
    "Istanbul": (41.01, 28.95), "Ankara": (39.93, 32.86),
    # Middle East
    "Dubai": (25.20, 55.27), "Tel Aviv": (32.08, 34.78), "Beirut": (33.89, 35.50),
    # Latin America
    "Mexico City": (19.43, -99.13), "Buenos Aires": (-34.60, -58.38),
    "Bogota": (4.71, -74.07), "Lima": (-12.05, -77.04), "Santiago": (-33.45, -70.67),
    "Sao Paulo": (-23.55, -46.63), "Rio de Janeiro": (-22.91, -43.17),
    "Guadalajara": (20.67, -103.35), "Medellin": (6.23, -75.57),
    "Montevideo": (-34.90, -56.19), "Quito": (-0.22, -78.51),
    # Africa
    "Cape Town": (-33.93, 18.42), "Johannesburg": (-26.20, 28.04),
    "Cairo": (30.06, 31.25), "Lagos": (6.52, 3.38), "Nairobi": (-1.29, 36.82),
    "Accra": (5.56, -0.20), "Casablanca": (33.59, -7.61),
    # Japan
    "Tokyo": (35.69, 139.69), "Osaka": (34.69, 135.50), "Kyoto": (35.02, 135.76),
    # Korea
    "Seoul": (37.57, 126.98), "Busan": (35.10, 129.04),
    # China
    "Beijing": (39.91, 116.39), "Shanghai": (31.23, 121.47),
    "Shenzhen": (22.54, 114.06), "Chengdu": (30.57, 104.07),
    # SE Asia
    "Singapore": (1.35, 103.82), "Bangkok": (13.76, 100.50), "Taipei": (25.05, 121.56),
    "Kuala Lumpur": (3.14, 101.69), "Hong Kong": (22.28, 114.17),
    "Jakarta": (-6.21, 106.85), "Manila": (14.60, 120.98),
    "Ho Chi Minh City": (10.82, 106.63), "Hanoi": (21.03, 105.85),
    # India
    "Bangalore": (12.97, 77.59), "Mumbai": (19.08, 72.88), "Delhi": (28.61, 77.21),
    "Chennai": (13.08, 80.27), "Hyderabad": (17.39, 78.49), "Pune": (18.52, 73.86),
}


def _load_communities(city: str, topic: str) -> list[dict]:
    path = DATA_DIR / _normalize(city) / _normalize(topic) / "communities.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def _global_topic_counts() -> dict[str, int]:
    """Total community count per topic across all cities."""
    meta_file = DATA_DIR / "metadata.json"
    if not meta_file.exists():
        return {}
    try:
        metadata = json.loads(meta_file.read_text(encoding="utf-8"))
        result: dict[str, int] = {}
        for city_topics in metadata.get("records_by_city_topic", {}).values():
            for topic, count in city_topics.items():
                result[topic] = result.get(topic, 0) + int(count)
        return result
    except Exception:
        return {}


def _top_cities(n: int = 8) -> list[tuple[str, str, int]]:
    """Return top N cities (name, country, total_count) sorted by community count."""
    meta_file = DATA_DIR / "metadata.json"
    if not meta_file.exists():
        return []
    try:
        metadata = json.loads(meta_file.read_text(encoding="utf-8"))
        city_totals: dict[str, int] = {}
        for city, topics in metadata.get("records_by_city_topic", {}).items():
            city_totals[city] = sum(int(c) for c in topics.values())
        sorted_cities = sorted(city_totals.items(), key=lambda x: x[1], reverse=True)
        cities_map = {c.name: c.country for c in (app_state.cities or [])}
        return [(name, cities_map.get(name, ""), count)
                for name, count in sorted_cities[:n] if count > 0]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@_fastapi.get("/", response_class=HTMLResponse)
async def public_home(request: Request, city: str = ""):
    cities = app_state.cities or []
    topics = app_state.topics or []
    return templates.TemplateResponse(request, "public_home.html", {
        "cities": cities,
        "topics": topics,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "selected_city": city,
        "topic_counts": _global_topic_counts(),
        "featured_cities": _top_cities(8),
    })


@_fastapi.get("/explore", response_class=HTMLResponse)
async def public_explore(
    request: Request,
    city: str = "",
    topic: list[str] = Query(default=[]),
    subscribed: str = "",
):
    cities = app_state.cities or []
    topics = app_state.topics or []

    # Load communities for each selected city+topic
    sections: list[dict] = []
    total = 0
    for t in (topic if topic else []):
        records = _load_communities(city, t) if city else []
        total += len(records)
        sections.append({
            "topic": t,
            "label": TOPIC_LABELS.get(t, t.replace("_", " ").title()),
            "icon": TOPIC_ICONS.get(t, "circle"),
            "records": records,
        })

    # If city provided but no topic, show available topics for that city
    available_topics: dict[str, int] = {}
    if city and not topic:
        for t in topics:
            count = len(_load_communities(city, t.name))
            if count > 0:
                available_topics[t.name] = count

    # If no city but topics selected, aggregate across all cities
    cross_city_sections: list[dict] = []
    if not city and topic:
        for t in topic:
            city_results = []
            for c in cities:
                records = _load_communities(c.name, t)
                if records:
                    city_results.append({"city": c.name, "country": c.country, "records": records})
            city_results.sort(key=lambda x: len(x["records"]), reverse=True)
            cross_city_sections.append({
                "topic": t,
                "label": TOPIC_LABELS.get(t, t.replace("_", " ").title()),
                "icon": TOPIC_ICONS.get(t, "circle"),
                "city_results": city_results[:6],
                "total": sum(len(cr["records"]) for cr in city_results),
            })

    return templates.TemplateResponse(request, "public_explore.html", {
        "city": city,
        "topics": topics,
        "selected_topics": topic,
        "sections": sections,
        "total": total,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "available_topics": available_topics,
        "cross_city_sections": cross_city_sections,
        "cities": cities,
        "subscribed": subscribed == "1",
    })


@_fastapi.post("/subscribe")
async def public_subscribe(
    request: Request,
    email: str = Form(...),
    city: str = Form(...),
    topics: list[str] = Form(default=[]),
):
    if not app_state.db_path or not email or not city or not topics:
        return RedirectResponse(
            f"/explore?city={city}&" + "&".join(f"topic={t}" for t in topics),
            status_code=302,
        )
    from ..db import save_subscription
    for t in topics:
        save_subscription(app_state.db_path, email, city, t)

    qs = f"city={city}&" + "&".join(f"topic={t}" for t in topics) + "&subscribed=1"
    return RedirectResponse(f"/explore?{qs}", status_code=302)


@_fastapi.get("/unsubscribe", response_class=HTMLResponse)
async def public_unsubscribe(request: Request, token: str = ""):
    removed = False
    if token and app_state.db_path:
        from ..db import delete_subscription
        removed = delete_subscription(app_state.db_path, token)
    return templates.TemplateResponse(request, "public_unsubscribe.html", {"removed": removed})


@_fastapi.get("/api/city-topics")
async def api_city_topics(city: str = ""):
    """Return per-topic community counts for a city (used by home page JS)."""
    if not city:
        return JSONResponse({})
    result = {}
    for t in (app_state.topics or []):
        result[t.name] = len(_load_communities(city, t.name))
    return JSONResponse(result)


@_fastapi.get("/map", response_class=HTMLResponse)
async def public_map(request: Request):
    cities_data = []
    for city in (app_state.cities or []):
        coords = CITY_COORDS.get(city.name)
        if not coords:
            continue
        count = sum(len(_load_communities(city.name, t.name)) for t in (app_state.topics or []))
        cities_data.append({
            "name": city.name,
            "country": city.country,
            "lat": coords[0],
            "lng": coords[1],
            "count": count,
        })

    total = sum(c["count"] for c in cities_data)
    cities_with_data = [c for c in cities_data if c["count"] > 0]
    return templates.TemplateResponse(request, "public_map.html", {
        "cities_json": json.dumps(cities_with_data),
        "total": total,
        "cities_with_data": len(cities_with_data),
        "cities_tracked": len(cities_data),
    })


@_fastapi.get("/about", response_class=HTMLResponse)
async def public_about(request: Request):
    metadata = {}
    meta_file = DATA_DIR / "metadata.json"
    if meta_file.exists():
        try:
            metadata = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    return templates.TemplateResponse(request, "public_about.html", {
        "city_count": len(app_state.cities or []),
        "topic_count": len(app_state.topics or []),
        "total_records": metadata.get("total_records", 0),
        "topics": app_state.topics or [],
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "featured_cities": _top_cities(12),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN ROUTES  (prefix: /admin, protected by _BasicAuth)
# ═══════════════════════════════════════════════════════════════════════════════

@admin.get("/", response_class=HTMLResponse)
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

    try:
        _settings = yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8"))
        _pipe = _settings.get("pipeline", {})
        test_mode = _pipe.get("test_mode", False)
        test_cities = _pipe.get("test_cities", [])
    except Exception:
        test_mode = False
        test_cities = []

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
        "test_mode": test_mode,
        "test_cities": test_cities,
    })


@admin.get("/results", response_class=HTMLResponse)
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


@admin.get("/results/{city}/{topic}", response_class=HTMLResponse)
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


@admin.get("/config", response_class=HTMLResponse)
async def config_page(request: Request, saved: Optional[str] = None, error: Optional[str] = None):
    software = await _build_software_info()

    sub_count = 0
    if app_state.db_path:
        from ..db import get_subscriptions
        sub_count = len(get_subscriptions(app_state.db_path))

    return templates.TemplateResponse(request, "config.html", {
        "cities_yaml": (CONFIG_DIR / "cities.yaml").read_text(encoding="utf-8"),
        "topics_yaml": (CONFIG_DIR / "topics.yaml").read_text(encoding="utf-8"),
        "settings_yaml": (CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8"),
        "saved": saved,
        "error": error,
        "software": software,
        "sub_count": sub_count,
    })


@admin.post("/config/cities")
async def save_cities(request: Request, cities_yaml: str = Form(...)):
    try:
        parsed = yaml.safe_load(cities_yaml)
        assert isinstance(parsed, dict) and "cities" in parsed, "Missing 'cities' key"
        (CONFIG_DIR / "cities.yaml").write_text(cities_yaml, encoding="utf-8")
        return RedirectResponse("/admin/config?saved=cities", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/admin/config?error={exc}", status_code=302)


@admin.post("/config/topics")
async def save_topics(request: Request, topics_yaml: str = Form(...)):
    try:
        parsed = yaml.safe_load(topics_yaml)
        assert isinstance(parsed, dict) and "topics" in parsed, "Missing 'topics' key"
        (CONFIG_DIR / "topics.yaml").write_text(topics_yaml, encoding="utf-8")
        return RedirectResponse("/admin/config?saved=topics", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/admin/config?error={exc}", status_code=302)


@admin.post("/config/settings")
async def save_settings(request: Request, settings_yaml: str = Form(...)):
    try:
        yaml.safe_load(settings_yaml)
        (CONFIG_DIR / "settings.yaml").write_text(settings_yaml, encoding="utf-8")
        return RedirectResponse("/admin/config?saved=settings", status_code=302)
    except Exception as exc:
        return RedirectResponse(f"/admin/config?error={exc}", status_code=302)


@admin.get("/subscriptions", response_class=HTMLResponse)
async def subscriptions_page(request: Request):
    subs = []
    if app_state.db_path:
        from ..db import get_subscriptions
        subs = get_subscriptions(app_state.db_path)
    return templates.TemplateResponse(request, "subscriptions.html", {
        "subs": subs,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
    })


@admin.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    history = broadcaster.get_all()
    last_seq = history[-1]["seq"] if history else 0
    return templates.TemplateResponse(request, "logs.html", {
        "history": history,
        "last_seq": last_seq,
    })


@admin.get("/api/logs/stream")
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


@admin.post("/api/run")
async def trigger_run(
    run_mode: str = Form("full"),
    skip_scraped: str = Form("off"),
    skip_extracted: str = Form("off"),
):
    if app_state.is_running:
        return RedirectResponse("/admin/logs", status_code=302)

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
    return RedirectResponse("/admin/logs", status_code=302)


@admin.get("/api/status")
async def status():
    return {
        "is_running": app_state.is_running,
        "last_run_at": app_state.last_run_at.isoformat() if app_state.last_run_at else None,
    }


@admin.get("/cache", response_class=HTMLResponse)
async def cache_page(request: Request):
    entries = []
    if app_state.cache_manager:
        entries = app_state.cache_manager.get_index()
    return templates.TemplateResponse(request, "cache.html", {"entries": entries})


@admin.get("/cache/{url_hash}", response_class=HTMLResponse)
async def cache_detail(request: Request, url_hash: str):
    if not app_state.cache_manager:
        return RedirectResponse("/admin/cache", status_code=302)

    entry = app_state.cache_manager.get_entry(url_hash)
    if not entry:
        return RedirectResponse("/admin/cache", status_code=302)

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


@admin.post("/cache/{url_hash}/delete-scraped")
async def cache_delete_scraped(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_scraped(url_hash)
    return RedirectResponse("/admin/cache", status_code=302)


@admin.post("/cache/{url_hash}/delete-extracted")
async def cache_delete_extracted(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_extracted(url_hash)
    return RedirectResponse("/admin/cache", status_code=302)


@admin.post("/cache/{url_hash}/delete")
async def cache_delete_entry(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_entry(url_hash)
    return RedirectResponse("/admin/cache", status_code=302)


@admin.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail(request: Request, run_id: int):
    if not app_state.db_path:
        return RedirectResponse("/admin", status_code=302)
    from ..db import get_run_detail
    run = get_run_detail(app_state.db_path, run_id)
    if not run:
        return RedirectResponse("/admin", status_code=302)
    pair_logs = json.loads(run["search_log"]) if run.get("search_log") else []
    return templates.TemplateResponse(request, "run_detail.html", {
        "run": run,
        "pair_logs": pair_logs,
    })


@admin.get("/history", response_class=HTMLResponse)
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


@admin.get("/history/{commit_hash}", response_class=HTMLResponse)
async def history_detail(request: Request, commit_hash: str):
    if not all(c in "0123456789abcdefABCDEF" for c in commit_hash):
        return RedirectResponse("/admin/history", status_code=302)

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


_fastapi.include_router(admin)
