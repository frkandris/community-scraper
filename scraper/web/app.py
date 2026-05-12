import asyncio
import base64
import html
import hmac
import importlib.metadata
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote as _url_quote, urlsplit

import httpx
import structlog
import yaml
from fastapi import APIRouter, BackgroundTasks, FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.types import ASGIApp, Receive, Scope, Send

from ..config import load_config, load_config_from_docs
from ..db import (
    delete_all_communities,
    find_community_by_id,
    get_community_history,
    get_all_communities,
    get_city_topic_counts,
    get_city_totals,
    get_communities,
    get_communities_by_ids,
    get_communities_for_city,
    search_communities_by_tag,
    save_not_community_report,
    get_not_community_reports,
    delete_not_community_report,
    get_communities_needing_revalidation,
    count_communities_needing_revalidation,
    set_community_revalidate_fingerprint,
    set_community_hidden,
    _community_record_key,
    get_topic_counts,
    get_topic_counts_for_cities,
    get_total_community_count,
    get_all_venues,
    get_venue_counts,
    get_venues_by_city_topic,
    get_venues,
    get_communities_for_venue,
    get_venue_person_counts_by_url,
    get_venue_history,
    get_persons,
    get_all_persons,
    get_person_counts,
    get_person_history,
    get_cache_cost_stats,
    get_scope_stats,
    get_prompt_overrides,
    upsert_prompt_override,
    delete_prompt_override,
    save_city_request,
    init_db,
    get_duplicate_candidates,
    resolve_duplicate_candidate,
    merge_community_into,
    get_community_by_record_key,
    save_community_data,
    save_edit_request,
    get_edit_requests,
    resolve_edit_request,
    apply_community_edit,
    search_all,
    get_venue_for_community,
    get_persons_for_community,
    save_community_submission,
    get_community_submissions,
    resolve_community_submission,
    get_other_communities,
    upsert_recategorize_suggestion,
    get_recategorize_suggestions,
    apply_recategorize_suggestion,
    update_recategorize_status,
)
from ..false_positives import (add as fp_add, diff_html as fp_diff_html,
                               load as fp_load, load_history as fp_load_history,
                               remove as fp_remove, build_prompt_section)
from ..extract import (ENRICH_SCHEMA, ENRICH_SYSTEM_PROMPT, EXTRACTION_SCHEMA,
                       SYSTEM_PROMPT, USER_PROMPT_TEMPLATE, _prompt_hash,
                       VENUE_SCHEMA, VENUE_SYSTEM_PROMPT, VENUE_USER_PROMPT_TEMPLATE,
                       PERSON_SCHEMA, PERSON_SYSTEM_PROMPT, PERSON_USER_PROMPT_TEMPLATE,
                       PROMPT_KEYS, get_prompt, set_prompt_override,
                       DeepSeekExtractor, FallbackExtractor, GroqExtractor, OllamaExtractor)
from ..fetch import fetch_and_clean
from ..models import CommunityRecord
from ..pipeline import _enrich_record, _needs_enrichment, run_pipeline, scrape_submitted_url, reextract_community
from ..search import BraveSearchClient, SearXNGClient
from ..store import patch_results, save_results
from .i18n import get_topic_labels, lang_context
from .log_stream import broadcaster
from .schema import records_to_jsonld
from .state import app_state

log = structlog.get_logger()

BASE_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"

_ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

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
    "chess": "crown",
    "cooking": "cooking-pot",
    "theater": "ticket",
    "music": "music-notes",
    "martial_arts": "sword",
    "gaming": "game-controller",
    "volunteering": "hand-heart",
    "language_exchange": "translate",
    "art": "paint-brush",
    "meditation": "spiral",
    "swimming": "waves",
    "hagyomanyorzes": "scroll",
    "gardening": "plant",
    "film_club": "popcorn",
    "trivia": "lightbulb",
    "sustainability": "recycle",
    "crafts": "scissors",
    "fitness": "barbell",
    "religion": "hands-praying",
    "baby": "baby",
    "senior": "sun-horizon",
    "kisallat": "paw-print",
    "vallalkozas": "briefcase",
    "nok": "gender-female",
    "fogyatekossag": "wheelchair",
    "tech": "code",
    "other": "circles-four",
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
    "hagyomanyorzes": "Hagyományőrzés",
    "gardening": "Gardening",
    "film_club": "Film Club",
    "trivia": "Trivia & Quizzes",
    "sustainability": "Sustainability",
    "crafts": "Crafts & Making",
    "fitness": "Fitness",
    "religion": "Religion & Faith",
    "baby": "Baba & Szülő",
    "senior": "Seniors",
    "kisallat": "Kisállat",
    "vallalkozas": "Entrepreneurship",
    "nok": "Women",
    "fogyatekossag": "Disability",
    "tech": "Tech",
    "other": "Other",
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

        if not _ADMIN_PASSWORD:
            await self._send_plain(send, 503, b"ADMIN_PASSWORD is not configured")
            return

        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        auth = headers.get(b"authorization", b"").decode("latin-1")

        if auth.lower().startswith("basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode("utf-8")
                user, _, pwd = decoded.partition(":")
                if (
                    hmac.compare_digest(user, _ADMIN_USER)
                    and hmac.compare_digest(pwd, _ADMIN_PASSWORD)
                    and self._same_origin_admin_write(scope, headers)
                ):
                    await self._inner(scope, receive, send)
                    return
            except Exception:
                auth = ""

        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [
                [b"www-authenticate", b'Basic realm="Community Scraper Admin"'],
                [b"content-length", b"0"],
            ],
        })
        await send({"type": "http.response.body", "body": b""})

    @staticmethod
    async def _send_plain(send: Send, status: int, body: bytes) -> None:
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                [b"content-type", b"text/plain; charset=utf-8"],
                [b"content-length", str(len(body)).encode("ascii")],
            ],
        })
        await send({"type": "http.response.body", "body": body})

    @staticmethod
    def _same_origin_admin_write(scope: Scope, headers: dict[bytes, bytes]) -> bool:
        method = scope.get("method", "GET").upper()
        if method in ("GET", "HEAD", "OPTIONS"):
            return True

        host = headers.get(b"host", b"").decode("latin-1")
        origin = headers.get(b"origin", b"").decode("latin-1")
        referer = headers.get(b"referer", b"").decode("latin-1")
        candidate = origin or referer
        if not candidate:
            return False
        try:
            return urlsplit(candidate).netloc == host
        except Exception:
            return False


_fastapi = FastAPI(title="Community Scraper")
app = _BasicAuth(_fastapi)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.filters["urlencode"] = lambda s: _url_quote(str(s), safe="")

def _sha256_16(url: str) -> str:
    import hashlib
    return hashlib.sha256(str(url).encode()).hexdigest()[:16]

templates.env.filters["sha256_16"] = _sha256_16


def _fmt_dur(s: float | None) -> str:
    if s is None:
        return ""
    s = float(s)
    if s < 60:
        return f"{s:.1f}s"
    return f"{int(s / 60)}m {int(s % 60)}s"


templates.env.filters["fmt_dur"] = _fmt_dur


_LINK_PLATFORMS = [
    (["facebook.com", "fb.com", "fb.me"],            "Facebook",  "ph-facebook-logo",  "text-blue-700 bg-blue-50 hover:bg-blue-100"),
    (["instagram.com"],                               "Instagram", "ph-instagram-logo", "text-pink-600 bg-pink-50 hover:bg-pink-100"),
    (["twitter.com", "x.com"],                        "X",         "ph-x-logo",         "text-gray-800 bg-gray-100 hover:bg-gray-200"),
    (["youtube.com", "youtu.be"],                     "YouTube",   "ph-youtube-logo",   "text-red-600 bg-red-50 hover:bg-red-100"),
    (["linkedin.com"],                                "LinkedIn",  "ph-linkedin-logo",  "text-blue-800 bg-blue-50 hover:bg-blue-100"),
    (["meetup.com"],                                  "Meetup",    "ph-users-three",    "text-red-500 bg-red-50 hover:bg-red-100"),
    (["t.me", "telegram.me", "telegram.org"],         "Telegram",  "ph-telegram-logo",  "text-sky-600 bg-sky-50 hover:bg-sky-100"),
    (["discord.gg", "discord.com"],                   "Discord",   "ph-discord-logo",   "text-indigo-600 bg-indigo-50 hover:bg-indigo-100"),
    (["whatsapp.com", "wa.me"],                       "WhatsApp",  "ph-whatsapp-logo",  "text-green-600 bg-green-50 hover:bg-green-100"),
    (["tiktok.com"],                                  "TikTok",    "ph-music-note",     "text-gray-900 bg-gray-100 hover:bg-gray-200"),
    (["github.com"],                                  "GitHub",    "ph-github-logo",    "text-gray-800 bg-gray-100 hover:bg-gray-200"),
    (["linktr.ee", "linktree.com"],                   "Linktree",  "ph-tree-structure", "text-green-700 bg-green-50 hover:bg-green-100"),
]


def _valid_url(url: str) -> bool:
    from urllib.parse import urlparse
    try:
        p = urlparse(url)
        host = p.netloc.lower()
        return (
            p.scheme in ("http", "https")
            and bool(host)
            and "." in host
            and " " not in host
            and "%20" not in host
        )
    except Exception:
        return False


def _link_meta(url: str) -> dict:
    from urllib.parse import urlparse
    try:
        host = urlparse(url).netloc.lower().removeprefix("www.")
        for domains, label, icon, color in _LINK_PLATFORMS:
            if any(d in host for d in domains):
                return {"label": label, "icon": icon, "color": color}
        domain = host[:28] or url[:28]
    except Exception:
        domain = url[:28]
    return {"label": domain, "icon": "ph-globe", "color": "text-indigo-600 bg-indigo-50 hover:bg-indigo-100"}


templates.env.filters["valid_url"] = _valid_url
templates.env.filters["link_meta"] = _link_meta


def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


templates.env.filters["slugify"] = _slugify

_ROLE_HU = {
    "leader": "vezető",
    "instructor": "oktató",
    "speaker": "előadó",
    "organizer": "szervező",
    "founder": "alapító",
    "coach": "edző",
    "trainer": "tréner",
    "moderator": "moderátor",
    "admin": "adminisztrátor",
    "member": "tag",
    "volunteer": "önkéntes",
    "coordinator": "koordinátor",
}
templates.env.filters["role_hu"] = lambda r: _ROLE_HU.get((r or "").lower(), r)


def _safe_redirect_target(target: str, fallback: str) -> str:
    return target if target.startswith("/") and not target.startswith("//") else fallback


def _config_error_redirect(exc: Exception) -> RedirectResponse:
    return RedirectResponse(f"/admin/config?error={_url_quote(str(exc), safe='')}", status_code=302)


def _read_config_yaml(name: str) -> object:
    return yaml.safe_load((CONFIG_DIR / name).read_text(encoding="utf-8"))


def _validate_candidate_config(
    cities_yaml: str | None = None,
    topics_yaml: str | None = None,
    settings_yaml: str | None = None,
) -> None:
    cities_raw = yaml.safe_load(cities_yaml) if cities_yaml is not None else _read_config_yaml("cities.yaml")
    topics_raw = yaml.safe_load(topics_yaml) if topics_yaml is not None else _read_config_yaml("topics.yaml")
    settings_raw = yaml.safe_load(settings_yaml) if settings_yaml is not None else _read_config_yaml("settings.yaml")
    load_config_from_docs(_db(), cities_raw, topics_raw, settings_raw)


def _reload_runtime_config() -> None:
    if not app_state.db_path:
        return
    cities, topics, pipeline_cfg = load_config(app_state.db_path)
    app_state.cities = cities
    app_state.topics = topics
    app_state.pipeline_cfg = pipeline_cfg
    _load_prompt_overrides()


def _load_prompt_overrides() -> None:
    if not app_state.db_path:
        return
    overrides = get_prompt_overrides(app_state.db_path)
    for key in PROMPT_KEYS:
        set_prompt_override(key, overrides.get(key))

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
    brave_key = cfg.brave_api_key if cfg else ""
    if brave_key:
        search_info = {"label": "Brave Search", "status": "ok", "backend": "brave"}
        ollama_ver = await _ollama_version(ollama_url)
    else:
        searxng_url = cfg.searxng_url if cfg else "http://localhost:8080"
        ollama_ver, searxng_st = await asyncio.gather(
            _ollama_version(ollama_url),
            _searxng_status(searxng_url),
        )
        search_info = {"label": "SearXNG", "status": searxng_st, "backend": "searxng"}
    return {
        "searxng": search_info,
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
    "Aba": (47.10, 18.53), "Abaújszántó": (48.30, 21.21), "Abony": (47.18, 20.01),
    "Abádszalók": (47.47, 20.60), "Adony": (47.12, 18.87), "Ajak": (48.09, 21.89),
    "Ajka": (47.10, 17.55), "Albertirsa": (47.24, 19.60), "Alsózsolca": (48.09, 20.92),
    "Aszód": (47.65, 19.49), "Badacsonytomaj": (46.80, 17.48), "Baja": (46.18, 18.96),
    "Baktalórántháza": (47.98, 21.99), "Balassagyarmat": (48.08, 19.30),
    "Balatonalmádi": (47.03, 18.02), "Balatonboglár": (46.77, 17.65),
    "Balatonföldvár": (46.85, 17.90), "Balatonfüred": (46.96, 17.89),
    "Balatonfűzfő": (47.06, 18.04), "Balatonkenese": (47.05, 18.12),
    "Balatonlelle": (46.79, 17.70), "Balkány": (47.77, 21.87),
    "Balmazújváros": (47.62, 21.35), "Barcs": (45.96, 17.46), "Battonya": (46.28, 21.01),
    "Beled": (47.48, 17.06), "Berettyóújfalu": (47.22, 21.55), "Berhida": (47.11, 18.17),
    "Besenyszög": (47.20, 20.25), "Biatorbágy": (47.46, 18.82), "Bicske": (47.49, 18.64),
    "Biharkeresztes": (47.13, 21.72), "Bodajk": (47.32, 18.26), "Bonyhád": (46.30, 18.53),
    "Borsodnádasd": (48.12, 20.10), "Budakalász": (47.62, 19.04), "Budakeszi": (47.51, 18.91),
    "Budapest": (47.50, 19.04), "Budaörs": (47.45, 18.97),
    "Bábolna": (47.68, 18.04), "Bácsalmás": (46.12, 19.33), "Bátaszék": (46.21, 18.72),
    "Bátonyterenye": (47.96, 19.82), "Békés": (46.77, 21.13), "Békéscsaba": (46.68, 21.09),
    "Bélapátfalva": (47.99, 20.26), "Bóly": (45.94, 18.52), "Bük": (47.38, 16.75),
    "Cegléd": (47.17, 19.80), "Celldömölk": (47.26, 17.15), "Cigánd": (48.34, 21.82),
    "Csanádpalota": (46.25, 20.73), "Csenger": (47.84, 22.69), "Csepreg": (47.39, 16.70),
    "Csongrád": (46.71, 20.14), "Csorna": (47.61, 17.26), "Csorvás": (46.62, 20.83),
    "Csurgó": (46.25, 17.10), "Csákvár": (47.39, 18.47), "Dabas": (47.18, 19.32),
    "Debrecen": (47.53, 21.63), "Demecser": (47.92, 21.97), "Derecske": (47.36, 21.57),
    "Devecser": (47.10, 17.44), "Dombrád": (48.23, 21.86), "Dombóvár": (46.38, 18.13),
    "Dorog": (47.72, 18.73), "Dunaföldvár": (46.80, 18.92), "Dunaharaszti": (47.35, 19.09),
    "Dunakeszi": (47.63, 19.14), "Dunavarsány": (47.29, 18.98), "Dunavecse": (46.91, 18.97),
    "Dunaújváros": (46.98, 18.93), "Dévaványa": (47.02, 20.96), "Edelény": (48.30, 20.73),
    "Eger": (47.90, 20.37), "Elek": (46.53, 21.26), "Emőd": (47.95, 20.84),
    "Encs": (48.33, 21.12), "Enying": (46.93, 18.24), "Ercsi": (47.25, 18.88),
    "Esztergom": (47.79, 18.74), "Fegyvernek": (47.26, 20.52), "Fehérgyarmat": (47.99, 22.52),
    "Felsőzsolca": (48.11, 20.88), "Fertőd": (47.63, 16.86), "Fertőszentmiklós": (47.58, 16.85),
    "Fonyód": (46.75, 17.56), "Fót": (47.61, 19.20), "Füzesabony": (47.75, 20.42),
    "Füzesgyarmat": (46.98, 21.23), "Gyomaendrőd": (46.93, 20.83), "Gyula": (46.65, 21.28),
    "Gyál": (47.38, 19.24), "Gyömrő": (47.42, 19.40), "Gyöngyös": (47.78, 19.93),
    "Gyöngyöspata": (47.82, 19.85), "Gyönk": (46.47, 18.49), "Győr": (47.68, 17.63),
    "Gárdony": (47.20, 18.61), "Göd": (47.69, 19.13), "Gödöllő": (47.60, 19.36),
    "Gönc": (48.47, 21.26), "Hajdúböszörmény": (47.67, 21.52), "Hajdúdorog": (47.82, 21.66),
    "Hajdúhadház": (47.68, 21.66), "Hajdúnánás": (47.85, 21.43),
    "Hajdúszoboszló": (47.45, 21.40), "Hajdúsámson": (47.58, 21.77), "Hajós": (46.40, 19.10),
    "Halásztelek": (47.35, 18.99), "Harkány": (45.86, 18.23), "Hatvan": (47.67, 19.67),
    "Herend": (47.14, 17.75), "Heves": (47.60, 20.28), "Hévíz": (46.79, 17.19),
    "Hódmezővásárhely": (46.42, 20.33), "Ibrány": (48.13, 21.72), "Igal": (46.38, 17.79),
    "Isaszeg": (47.54, 19.46), "Izsák": (46.78, 19.35), "Jánoshalma": (46.30, 19.32),
    "Jánosháza": (47.12, 17.16), "Jánossomorja": (47.78, 17.14), "Jászapáti": (47.51, 20.15),
    "Jászberény": (47.50, 19.92), "Jászfényszaru": (47.55, 19.71), "Jászkisér": (47.44, 20.21),
    "Jászárokszállás": (47.63, 19.99), "Kaba": (47.36, 21.26), "Kadarkút": (46.24, 17.63),
    "Kalocsa": (46.53, 18.99), "Kaposvár": (46.36, 17.80), "Kapuvár": (47.60, 17.03),
    "Karcag": (47.32, 20.93), "Kazincbarcika": (48.25, 20.65), "Kecel": (46.52, 19.26),
    "Kecskemét": (46.91, 19.69), "Kemecse": (47.92, 21.77), "Kenderes": (47.25, 20.71),
    "Kerekegyháza": (46.97, 19.47), "Kerepes": (47.60, 19.29), "Keszthely": (46.77, 17.24),
    "Kisbér": (47.51, 18.03), "Kiskunfélegyháza": (46.71, 19.84),
    "Kiskunhalas": (46.43, 19.49), "Kiskunmajsa": (46.49, 19.74), "Kisköre": (47.52, 20.51),
    "Kiskőrös": (46.62, 19.29), "Kistarcsa": (47.55, 19.27), "Kistelek": (46.47, 20.01),
    "Kisvárda": (48.22, 22.08), "Kisújszállás": (47.20, 20.76), "Komló": (46.20, 18.26),
    "Komádi": (47.00, 21.51), "Komárom": (47.74, 18.12), "Kozármisleny": (46.02, 18.25),
    "Kunhegyes": (47.37, 20.64), "Kunszentmiklós": (46.93, 19.12),
    "Kunszentmárton": (46.84, 20.28), "Körmend": (47.01, 16.60),
    "Körösladány": (46.93, 21.17), "Kőszeg": (47.39, 16.54), "Lajosmizse": (46.98, 19.57),
    "Lengyeltóti": (46.67, 17.72), "Lenti": (46.63, 16.54), "Letenye": (46.43, 16.71),
    "Lábatlan": (47.74, 18.61), "Lébény": (47.72, 17.38), "Létavértes": (47.38, 21.91),
    "Lőrinci": (47.74, 19.68), "Maglód": (47.45, 19.38), "Makó": (46.22, 20.48),
    "Marcali": (46.58, 17.41), "Martfű": (47.01, 20.29), "Martonvásár": (47.32, 18.79),
    "Medgyesegyháza": (46.51, 21.05), "Mezőberény": (46.82, 21.02),
    "Mezőcsát": (47.83, 20.91), "Mezőhegyes": (46.32, 20.83),
    "Mezőkeresztes": (47.83, 20.70), "Mezőkovácsháza": (46.42, 20.91),
    "Mezőkövesd": (47.82, 20.57), "Mezőtúr": (47.00, 20.62), "Mindszent": (46.52, 20.18),
    "Miskolc": (48.10, 20.78), "Mohács": (45.99, 18.68), "Monor": (47.35, 19.45),
    "Mosonmagyaróvár": (47.87, 17.27), "Mágocs": (46.34, 18.22), "Mándok": (48.33, 22.11),
    "Máriapócs": (47.83, 22.04), "Mátészalka": (47.95, 22.32), "Mélykút": (46.19, 19.39),
    "Mór": (47.38, 18.21), "Mórahalom": (46.22, 19.89), "Nagyatád": (46.23, 17.36),
    "Nagybajom": (46.39, 17.52), "Nagyecsed": (47.88, 22.40), "Nagyhalász": (48.08, 21.76),
    "Nagykanizsa": (46.45, 16.99), "Nagykálló": (47.87, 22.01), "Nagykáta": (47.41, 19.74),
    "Nagykőrös": (47.04, 19.78), "Nagymaros": (47.78, 18.96), "Nagymányok": (46.28, 18.36),
    "Nyergesújfalu": (47.76, 18.55), "Nyékládháza": (48.07, 20.93),
    "Nyíradony": (47.69, 21.92), "Nyírbátor": (47.83, 22.12), "Nyíregyháza": (47.95, 21.72),
    "Nyírlugos": (47.71, 22.07), "Nyírmada": (48.04, 22.22), "Nyírtelek": (47.98, 21.64),
    "Nádudvar": (47.42, 21.17), "Orosháza": (46.57, 20.67), "Oroszlány": (47.49, 18.31),
    "Pacsa": (46.70, 17.02), "Paks": (46.63, 18.86), "Pannonhalma": (47.55, 17.76),
    "Pilis": (47.28, 19.55), "Pilisvörösvár": (47.62, 18.91), "Polgár": (47.87, 21.11),
    "Polgárdi": (47.08, 18.29), "Pomáz": (47.64, 19.02), "Pusztaszabolcs": (47.14, 18.77),
    "Putnok": (48.30, 20.43), "Pálháza": (48.50, 21.41), "Pápa": (47.33, 17.47),
    "Pásztó": (47.92, 19.70), "Pécel": (47.49, 19.38), "Pécs": (46.07, 18.23),
    "Pécsvárad": (46.16, 18.42), "Pétervására": (47.92, 20.08),
    "Püspökladány": (47.32, 21.12), "Rakamaz": (48.04, 21.47), "Rudabánya": (48.38, 20.63),
    "Rácalmás": (47.10, 18.93), "Ráckeve": (47.16, 19.00), "Rákóczifalva": (47.09, 20.11),
    "Répcelak": (47.43, 17.00), "Rétság": (47.93, 19.11), "Sajóbábony": (48.10, 20.94),
    "Sajószentpéter": (48.22, 20.73), "Salgótarján": (48.10, 19.80), "Sarkad": (46.75, 21.39),
    "Sellye": (45.87, 17.85), "Siklós": (45.86, 18.30), "Simontornya": (46.75, 18.56),
    "Siófok": (46.91, 18.05), "Solt": (46.79, 18.97), "Soltvadkert": (46.58, 19.30),
    "Sopron": (47.68, 16.59), "Szabadszállás": (46.87, 19.22), "Szarvas": (46.87, 20.56),
    "Szeged": (46.25, 20.15), "Szeghalom": (47.02, 21.17), "Szekszárd": (46.35, 18.71),
    "Szendrő": (48.41, 20.74), "Szentendre": (47.67, 19.07), "Szentes": (46.65, 20.26),
    "Szentgotthárd": (46.95, 16.28), "Szentlőrinc": (46.05, 17.99),
    "Szerencs": (48.16, 21.20), "Szigethalom": (47.31, 18.98),
    "Szigetszentmiklós": (47.34, 19.05), "Szigetvár": (46.04, 17.80),
    "Szikszó": (48.21, 20.94), "Szob": (47.81, 18.87), "Szolnok": (47.17, 20.19),
    "Szombathely": (47.23, 16.62), "Százhalombatta": (47.32, 18.91),
    "Szécsény": (48.08, 19.52), "Székesfehérvár": (47.19, 18.41),
    "Sándorfalva": (46.35, 20.05), "Sárbogárd": (46.89, 18.62),
    "Sárospatak": (48.32, 21.57), "Sárvár": (47.25, 16.94), "Sásd": (46.25, 18.10),
    "Sátoraljaújhely": (48.40, 21.66), "Sümeg": (46.98, 17.28), "Tab": (46.74, 18.04),
    "Tamási": (46.63, 18.28), "Tapolca": (46.88, 17.44), "Tata": (47.65, 18.33),
    "Tatabánya": (47.57, 18.40), "Tiszacsege": (47.71, 21.05),
    "Tiszaföldvár": (47.02, 20.26), "Tiszafüred": (47.62, 20.76),
    "Tiszakécske": (46.93, 20.10), "Tiszalök": (47.97, 21.35),
    "Tiszavasvári": (47.96, 21.56), "Tiszaújváros": (47.92, 21.05),
    "Tokaj": (48.12, 21.41), "Tolna": (46.43, 18.79), "Tompa": (46.19, 19.53),
    "Tura": (47.60, 19.61), "Tápiószele": (47.37, 19.89), "Tát": (47.77, 18.67),
    "Téglás": (47.72, 21.68), "Tét": (47.50, 17.52), "Tótkomlós": (46.42, 20.75),
    "Tököl": (47.32, 18.97), "Törökbálint": (47.43, 18.89),
    "Törökszentmiklós": (47.18, 20.41), "Túrkeve": (47.10, 20.74), "Vaja": (47.95, 22.11),
    "Vasvár": (47.06, 16.80), "Vecsés": (47.41, 19.27), "Velence": (47.24, 18.65),
    "Veresegyház": (47.67, 19.30), "Verpelét": (47.86, 20.21), "Veszprém": (47.09, 17.91),
    "Villány": (45.87, 18.46), "Visegrád": (47.79, 18.97), "Vác": (47.78, 19.13),
    "Vámospércs": (47.54, 21.93), "Várpalota": (47.20, 18.14),
    "Vásárosnamény": (48.12, 22.32), "Vép": (47.30, 16.77), "Vésztő": (46.92, 21.26),
    "Zalaegerszeg": (46.84, 16.84), "Zalakaros": (46.55, 17.14),
    "Zalalövő": (46.85, 16.57), "Zalaszentgrót": (46.93, 17.08), "Zamárdi": (46.88, 17.96),
    "Zirc": (47.26, 17.87), "Zsámbék": (47.54, 18.72), "Záhony": (48.40, 22.18),
    "Ács": (47.71, 18.00), "Érd": (47.39, 18.90), "Ócsa": (47.29, 19.22),
    "Ózd": (48.22, 20.30), "Örkény": (47.13, 19.36), "Újfehértó": (47.81, 21.68),
    "Újkígyós": (46.60, 21.08), "Újszász": (47.29, 20.12), "Üllő": (47.38, 19.33),
    "Őriszentpéter": (46.87, 16.42),
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


def _ensure_community_id(record: dict) -> dict:
    if not record.get("community_id"):
        import hashlib
        key = f"{record.get('name', '').lower()}|{record.get('city', '').lower()}"
        record = dict(record, community_id=hashlib.sha256(key.encode()).hexdigest()[:12])
    if "community_url" not in record:
        city_sl = _slugify(record.get("city", ""))
        name_sl = _slugify(record.get("name", ""))
        record = dict(record, community_url=f"/{city_sl}/{name_sl}")
    return record


def _db() -> Path:
    return app_state.db_path or DATA_DIR / "scraper.db"


_HU_SORT_MAP = str.maketrans({
    # Each accented char maps to base + a byte > 'z'(122) so it sorts after all base-char words
    # Multiple variants of same base use ascending bytes: ó < ö < ő, ú < ü < ű
    ord('á'): 'a\x7f', ord('Á'): 'a\x7f',
    ord('é'): 'e\x7f', ord('É'): 'e\x7f',
    ord('í'): 'i\x7f', ord('Í'): 'i\x7f',
    ord('ó'): 'o\x7d', ord('Ó'): 'o\x7d',
    ord('ö'): 'o\x7e', ord('Ö'): 'o\x7e',
    ord('ő'): 'o\x7f', ord('Ő'): 'o\x7f',
    ord('ú'): 'u\x7d', ord('Ú'): 'u\x7d',
    ord('ü'): 'u\x7e', ord('Ü'): 'u\x7e',
    ord('ű'): 'u\x7f', ord('Ű'): 'u\x7f',
})


def _hu_sort_key(name: str) -> str:
    """Sort key for Hungarian alphabetical order: á after all a-words, é after e-words, etc."""
    return name.lower().translate(_HU_SORT_MAP)


def _city_from_slug(city_slug: str) -> str | None:
    for city in (app_state.cities or []):
        if _slugify(city.name) == city_slug:
            return city.name
    return None


def _city_locale(city_name: str) -> str:
    for city in (app_state.cities or []):
        if city.name == city_name:
            return city.locale or "en"
    return "en"


def _topic_url_slug(topic_name: str, locale: str) -> str:
    """Return the URL slug for a topic in the given locale."""
    labels = get_topic_labels(locale)
    label = labels.get(topic_name, topic_name.replace("_", " ").title())
    return _slugify(label)


def _topic_from_url_slug(slug: str, locale: str) -> str | None:
    """Given a localized URL slug and locale, return the canonical topic name."""
    labels = get_topic_labels(locale)
    for topic_name, label in labels.items():
        if _slugify(label) == slug:
            return topic_name
    # fall back to English
    if locale != "en":
        en_labels = get_topic_labels("en")
        for topic_name, label in en_labels.items():
            if _slugify(label) == slug:
                return topic_name
    return None


def _find_community_by_slug(city_name: str, name_slug: str) -> dict | None:
    for r in get_communities_for_city(_db(), city_name):
        r = _ensure_community_id(r)
        if _slugify(r.get("name", "")) == name_slug:
            return r
    return None


def _load_communities(city: str, topic: str) -> list[dict]:
    return [_ensure_community_id(r) for r in get_communities(_db(), city, topic)]


def _find_community(community_id: str) -> dict | None:
    r = find_community_by_id(_db(), community_id)
    return _ensure_community_id(r) if r else None


def _hu_city_names() -> set[str]:
    """Return the set of city names that belong to Hungary."""
    return {c.name for c in (app_state.cities or []) if c.country == "Hungary"}


def _global_topic_counts() -> dict[str, int]:
    return get_topic_counts(_db())


def _hu_topic_counts() -> dict[str, int]:
    """Topic counts restricted to Hungarian cities (single SQL query)."""
    hu = _hu_city_names()
    if not hu:
        return get_topic_counts(_db())
    return get_topic_counts_for_cities(_db(), hu)


def _top_cities(n: int = 8) -> list[tuple[str, str, int]]:
    city_totals = get_city_totals(_db())
    cities_map = {c.name: c.country for c in (app_state.cities or [])}
    return [(name, cities_map.get(name, ""), count)
            for name, count in city_totals[:n] if count > 0]


# ISO-3166-1 alpha-2 → country name as used in cities.yaml
_ISO2_COUNTRY: dict[str, str] = {
    "AR": "Argentina", "AU": "Australia", "AT": "Austria", "BE": "Belgium",
    "BR": "Brazil", "BG": "Bulgaria", "CA": "Canada", "CL": "Chile",
    "CN": "China", "CO": "Colombia", "HR": "Croatia", "CZ": "Czech Republic",
    "DK": "Denmark", "EC": "Ecuador", "EG": "Egypt", "EE": "Estonia",
    "FI": "Finland", "FR": "France", "DE": "Germany", "GH": "Ghana",
    "GR": "Greece", "HK": "Hong Kong", "HU": "Hungary", "IN": "India",
    "ID": "Indonesia", "IE": "Ireland", "IL": "Israel", "IT": "Italy",
    "JP": "Japan", "KE": "Kenya", "LV": "Latvia", "LB": "Lebanon",
    "LT": "Lithuania", "MY": "Malaysia", "MX": "Mexico", "MA": "Morocco",
    "NL": "Netherlands", "NZ": "New Zealand", "NG": "Nigeria", "NO": "Norway",
    "PE": "Peru", "PH": "Philippines", "PL": "Poland", "PT": "Portugal",
    "RO": "Romania", "RS": "Serbia", "SG": "Singapore", "SK": "Slovakia",
    "SI": "Slovenia", "ZA": "South Africa", "KR": "South Korea", "ES": "Spain",
    "SE": "Sweden", "CH": "Switzerland", "TW": "Taiwan", "TH": "Thailand",
    "TR": "Turkey", "AE": "UAE", "UA": "Ukraine", "GB": "United Kingdom",
    "US": "United States", "UY": "Uruguay", "VN": "Vietnam",
}

# language tag → likely ISO2 country code (Accept-Language fallback)
_LANG_COUNTRY: dict[str, str] = {
    "hu": "HU", "de": "DE", "fr": "FR", "es": "ES", "it": "IT",
    "pt": "PT", "ru": "RU", "uk": "UA", "zh": "CN", "ja": "JP",
    "ko": "KR", "ar": "EG", "fa": "IR", "he": "IL", "hi": "IN",
    "tr": "TR", "id": "ID", "nl": "NL", "pl": "PL", "sv": "SE",
    "cs": "CZ", "ro": "RO", "el": "GR", "vi": "VN", "th": "TH",
    "da": "DK", "no": "NO", "fi": "FI", "sk": "SK", "hr": "HR",
    "bg": "BG", "sr": "RS", "ms": "MY", "tl": "PH",
}


def _detect_country(request: Request) -> str | None:
    """Return the full country name for the visitor, or None."""
    # Cloudflare adds this header automatically
    cf = request.headers.get("CF-IPCountry", "").strip().upper()
    if cf and cf != "XX" and cf in _ISO2_COUNTRY:
        return _ISO2_COUNTRY[cf]

    # Generic reverse-proxy headers
    for header in ("X-Country-Code", "X-GeoIP-Country", "X-Real-IP-Country"):
        val = request.headers.get(header, "").strip().upper()
        if val and val in _ISO2_COUNTRY:
            return _ISO2_COUNTRY[val]

    # Fall back to primary Accept-Language tag  (best-effort, not accurate)
    al = request.headers.get("Accept-Language", "")
    for part in re.split(r"[,;]", al):
        tag = part.strip().split("-")
        if len(tag) >= 2:
            iso2 = tag[1].upper()
            if iso2 in _ISO2_COUNTRY:
                return _ISO2_COUNTRY[iso2]
        if len(tag) == 1:
            code = _LANG_COUNTRY.get(tag[0].lower())
            if code and code in _ISO2_COUNTRY:
                return _ISO2_COUNTRY[code]
    return None


def _cities_by_country(
    user_country: str | None,
    user_top: int = 20,
    other_countries: int = 3,
    other_top: int = 8,
) -> dict:
    """Return grouped city data for the home page city browser."""
    city_totals = dict(get_city_totals(_db()))
    cities_map = {c.name: c.country for c in (app_state.cities or [])}

    # Group by country
    country_cities: dict[str, list[tuple[str, int]]] = {}
    for name, country in cities_map.items():
        count = city_totals.get(name, 0)
        if count > 0:
            country_cities.setdefault(country, []).append((name, count))

    for cities_list in country_cities.values():
        cities_list.sort(key=lambda x: x[1], reverse=True)

    user_cities: list[tuple[str, str, int]] = []
    if user_country and user_country in country_cities:
        user_cities = [
            (name, user_country, count)
            for name, count in country_cities[user_country][:user_top]
        ]

    # Top other countries by total community count
    other_sorted = sorted(
        [(c, cities) for c, cities in country_cities.items() if c != user_country],
        key=lambda x: sum(cnt for _, cnt in x[1]),
        reverse=True,
    )
    other_sections = [
        {
            "country": country,
            "cities": [(name, country, count) for name, count in cities[:other_top]],
            "total": sum(cnt for _, cnt in cities),
        }
        for country, cities in other_sorted[:other_countries]
    ]

    return {
        "user_country": user_country,
        "user_cities": user_cities,
        "other_sections": other_sections,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC ROUTES
# ═══════════════════════════════════════════════════════════════════════════════

@_fastapi.get("/", response_class=HTMLResponse)
async def public_home(request: Request, city: str = ""):
    global _home_stats_cache
    hu_names = _hu_city_names()
    hu_cities = [c for c in (app_state.cities or []) if c.name in hu_names]
    topics = app_state.topics or []
    topic_url_slugs = {t.name: _topic_url_slug(t.name, "hu") for t in topics}
    if _home_stats_cache is None:
        topic_counts = _hu_topic_counts()
        venue_counts = {k: v for k, v in (get_venue_counts(_db()) if app_state.db_path else {}).items() if k in hu_names}
        person_counts = {k: v for k, v in (get_person_counts(_db()) if app_state.db_path else {}).items() if k in hu_names}
        city_totals = dict(get_city_totals(_db())) if app_state.db_path else {}
        hu_city_list = sorted(
            [{"name": c.name, "slug": _slugify(c.name), "count": city_totals.get(c.name, 0)} for c in hu_cities],
            key=lambda x: (-x["count"], _hu_sort_key(x["name"])),
        )
        _home_stats_cache = {
            "topic_counts": topic_counts,
            "total_records": sum(topic_counts.values()),
            "total_venues": sum(venue_counts.values()),
            "total_persons": sum(person_counts.values()),
            "hu_city_list": hu_city_list,
        }
    topic_counts = _home_stats_cache["topic_counts"]
    hu_city_list = _home_stats_cache["hu_city_list"]
    return templates.TemplateResponse(request, "public_home.html", {
        "cities": hu_cities,
        "topics": topics,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "selected_city": city,
        "topic_counts": topic_counts,
        "topic_url_slugs": topic_url_slugs,
        "total_records": _home_stats_cache["total_records"],
        "total_venues": _home_stats_cache["total_venues"],
        "total_persons": _home_stats_cache["total_persons"],
        "hu_city_list": hu_city_list,
        **lang_context(request),
    })


@_fastapi.get("/healthz")
async def healthz():
    db_ok = True
    total_records = 0
    try:
        total_records = get_total_community_count(_db())
    except Exception:
        db_ok = False
    return {
        "ok": db_ok,
        "version": app_state.version,
        "db": "ok" if db_ok else "error",
        "total_records": total_records,
    }


async def _render_explore(
    request: Request,
    city: str = "",
    topic: list[str] | None = None,
    tag: str = "",
    subscribed: str = "",
) -> HTMLResponse:
    if topic is None:
        topic = []
    cities = app_state.cities or []
    topics = app_state.topics or []

    _topic_labels = get_topic_labels(lang_context(request)["lang"])

    sections: list[dict] = []
    total = 0
    for t in topic:
        records = _load_communities(city, t) if city else []
        total += len(records)
        sections.append({
            "topic": t,
            "label": _topic_labels.get(t, t.replace("_", " ").title()),
            "icon": TOPIC_ICONS.get(t, "circle"),
            "records": records,
        })

    available_topics: dict[str, int] = {}
    if city:
        for t in topics:
            count = len(_load_communities(city, t.name))
            if count > 0:
                available_topics[t.name] = count

    # City page with no topic filter: show all communities (small chips will filter client-side)
    if city and not topic and available_topics:
        for t_name, count in available_topics.items():
            records = _load_communities(city, t_name)
            total += len(records)
            sections.append({
                "topic": t_name,
                "label": _topic_labels.get(t_name, t_name.replace("_", " ").title()),
                "icon": TOPIC_ICONS.get(t_name, "circle"),
                "records": records,
            })

    # Country-grouped multi-city view (when no specific city is selected and no tag)
    country_sections: list[dict] = []
    if not city and not tag:
        user_country = _detect_country(request)
        city_totals = dict(get_city_totals(_db()))
        cities_map = {c.name: c.country for c in cities}

        # Group cities by country, only include Hungarian cities that have data
        country_cities: dict[str, list[tuple[str, int]]] = {}
        for name, country in cities_map.items():
            if country != "Hungary":
                continue
            count = city_totals.get(name, 0)
            if count > 0:
                country_cities.setdefault(country, []).append((name, count))
        for v in country_cities.values():
            v.sort(key=lambda x: x[1], reverse=True)

        # Order: user's country first, then top 3 others by total community count
        country_order: list[tuple[str, bool]] = []
        if user_country and user_country in country_cities:
            country_order.append((user_country, True))
        other_sorted = sorted(
            [(c, clist) for c, clist in country_cities.items() if c != user_country],
            key=lambda x: sum(cnt for _, cnt in x[1]),
            reverse=True,
        )
        for c_name, _ in other_sorted[:3]:
            country_order.append((c_name, False))

        all_city_topic_counts = get_city_topic_counts(_db())
        for country, is_user in country_order:
            # When browsing by topic, show ALL cities and ALL records so counts match.
            # Without topic, show 3-city sample for discovery.
            all_city_entries = country_cities.get(country, [])
            city_entries = all_city_entries if topic else all_city_entries[:3]
            city_sections: list[dict] = []
            for city_name, city_count in city_entries:
                if topic:
                    recs: list[dict] = []
                    for t in topic:
                        recs.extend(_load_communities(city_name, t))
                else:
                    recs = [_ensure_community_id(r)
                            for r in get_communities_for_city(_db(), city_name)][:10]
                if recs:
                    city_url = "/" + _slugify(city_name)
                    if topic and len(topic) == 1:
                        city_url += "/" + _topic_url_slug(topic[0], _city_locale(city_name))
                    topic_count = len(recs) if topic else city_count
                    city_locale_str = _city_locale(city_name)
                    city_chips = sorted(
                        [
                            {
                                "name": t_obj.name,
                                "label": _topic_labels.get(t_obj.name, t_obj.name.replace("_", " ").title()),
                                "icon": TOPIC_ICONS.get(t_obj.name, "circle"),
                                "count": cnt,
                                "url": f"/{_slugify(city_name)}/{_topic_url_slug(t_obj.name, city_locale_str)}",
                            }
                            for t_obj in topics
                            if (cnt := all_city_topic_counts.get(city_name, {}).get(t_obj.name, 0)) > 0
                        ],
                        key=lambda x: x["count"],
                        reverse=True,
                    )
                    city_sections.append({
                        "city": city_name,
                        "country": country,
                        "records": recs,
                        "total": topic_count,
                        "city_url": city_url,
                        "topic_chips": city_chips,
                    })
            if city_sections:
                country_sections.append({
                    "country": country,
                    "is_user_country": is_user,
                    "cities": city_sections,
                })

    # Tag-based search: filter across communities by free-form tag
    tag_records: list[dict] = []
    if tag:
        tag_records = [_ensure_community_id(r)
                       for r in search_communities_by_tag(_db(), tag, city)]
        total += len(tag_records)

    all_records: list = []
    for s in sections:
        all_records.extend(s["records"])
    for cs in country_sections:
        for cs_city in cs["cities"]:
            all_records.extend(cs_city["records"])
    all_records.extend(tag_records)
    schema_json = records_to_jsonld(all_records)

    topic_venues: list[dict] = []
    if city and len(topic) == 1 and app_state.db_path:
        topic_venues = get_venues_by_city_topic(app_state.db_path, city, topic[0])

    city_venues: list[dict] = []
    city_persons: list[dict] = []
    if city and not topic and app_state.db_path:
        city_venues = get_venues(app_state.db_path, city)
        all_p = get_persons(app_state.db_path, city)
        seen_slugs: dict[str, dict] = {}
        for p in all_p:
            slug = _slugify(p.get("name", ""))
            if slug and slug not in seen_slugs:
                seen_slugs[slug] = p
        city_persons = list(seen_slugs.values())

    city_locale = _city_locale(city) if city else "en"
    topic_url_slugs = {t.name: _topic_url_slug(t.name, city_locale) for t in (app_state.topics or [])}

    return templates.TemplateResponse(request, "public_explore.html", {
        "city": city,
        "topics": topics,
        "selected_topics": topic,
        "sections": sections,
        "total": total,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "available_topics": available_topics,
        "country_sections": country_sections,
        "cities": cities,
        "subscribed": subscribed == "1",
        "schema_json": schema_json,
        "tag": tag,
        "tag_records": tag_records,
        "topic_venues": topic_venues,
        "city_venues": city_venues,
        "city_persons": city_persons,
        "topic_url_slugs": topic_url_slugs,
        **lang_context(request),
    })


@_fastapi.get("/felfedezes", response_class=HTMLResponse)
async def public_explore(
    request: Request,
    city: str = "",
    topic: list[str] = Query(default=[]),
    tag: str = "",
    subscribed: str = "",
):
    city_sl = _slugify(city) if city else ""
    if not tag:
        if city_sl and len(topic) == 1:
            qs = "?subscribed=1" if subscribed == "1" else ""
            topic_slug = _topic_url_slug(topic[0], _city_locale(city)) if city else topic[0]
            return RedirectResponse(f"/{city_sl}/{topic_slug}{qs}", status_code=301)
        if city_sl and not topic:
            return RedirectResponse(f"/{city_sl}", status_code=301)
    return await _render_explore(request, city=city, topic=topic, tag=tag, subscribed=subscribed)


@_fastapi.get("/felfedezes/{topic_slug}", response_class=HTMLResponse)
async def public_explore_topic_slug(request: Request, topic_slug: str):
    topic_name = _topic_from_url_slug(topic_slug, "hu")
    if not topic_name:
        return RedirectResponse("/felfedezes", status_code=302)
    return await _render_explore(request, topic=[topic_name])


@_fastapi.get("/explore", response_class=HTMLResponse)
async def public_explore_en(request: Request):
    qs = str(request.url.query)
    return RedirectResponse("/felfedezes" + (f"?{qs}" if qs else ""), status_code=301)


@_fastapi.get("/community/{community_id}", response_class=HTMLResponse)
async def public_community_legacy(request: Request, community_id: str):
    record = _find_community(community_id)
    if not record:
        return RedirectResponse("/", status_code=302)
    return RedirectResponse(record["community_url"], status_code=301)


@_fastapi.get("/source/{url_hash}", response_class=HTMLResponse)
async def public_source_page(request: Request, url_hash: str):
    """Public provenance page: search queries, scraped text, prompt, extracted records."""
    import hashlib
    if not app_state.cache_manager:
        return RedirectResponse("/", status_code=302)
    entry = app_state.cache_manager.get_entry(url_hash)
    if not entry:
        return RedirectResponse("/", status_code=302)

    cfg = app_state.pipeline_cfg
    max_text_chars = cfg.ollama_max_text_chars if cfg else 6000

    extract_user_prompt = ""
    if entry.get("raw_text") and entry.get("topic") and entry.get("city"):
        extract_user_prompt = USER_PROMPT_TEMPLATE.format(
            topic=entry.get("topic", ""),
            city=entry.get("city", ""),
            source_url=entry.get("url", ""),
            page_text=entry.get("raw_text", "")[:max_text_chars],
        )

    # Look up search cache to find what queries led to this URL
    search_queries: list[str] = entry.get("source_queries") or []

    return templates.TemplateResponse(request, "public_source.html", {
        "entry": entry,
        "extract_system_prompt": SYSTEM_PROMPT,
        "extract_user_prompt": extract_user_prompt,
        "search_queries": search_queries,
        "topic_labels": TOPIC_LABELS,
        **lang_context(request),
    })


@_fastapi.post("/subscribe")
async def public_subscribe(
    request: Request,
    email: str = Form(...),
    city: str = Form(...),
    topics: list[str] = Form(default=[]),
):
    city_sl = _slugify(city) if city else ""
    city_locale = _city_locale(city) if city else "en"
    if not app_state.db_path or not email or not city or not topics:
        if city_sl and len(topics) == 1:
            return RedirectResponse(f"/{city_sl}/{_topic_url_slug(topics[0], city_locale)}", status_code=302)
        return RedirectResponse(
            f"/felfedezes?city={city}&" + "&".join(f"topic={t}" for t in topics),
            status_code=302,
        )
    from ..db import save_subscription
    for t in topics:
        save_subscription(app_state.db_path, email, city, t)

    if city_sl and len(topics) == 1:
        return RedirectResponse(f"/{city_sl}/{_topic_url_slug(topics[0], city_locale)}?subscribed=1", status_code=302)
    qs = f"city={city}&" + "&".join(f"topic={t}" for t in topics) + "&subscribed=1"
    return RedirectResponse(f"/felfedezes?{qs}", status_code=302)


_FEEDBACK_EMAIL = os.environ.get("FEEDBACK_EMAIL", "")
_RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
_RESEND_FROM = os.environ.get("RESEND_FROM", "onboarding@resend.dev")


@_fastapi.post("/feedback")
async def public_feedback(
    community_name: str = Form(""),
    city: str = Form(""),
    topic: str = Form(""),
    page_url: str = Form(""),
    message: str = Form(""),
    user_email: str = Form(""),
):
    if _FEEDBACK_EMAIL and message and _RESEND_API_KEY:
        try:
            import resend
            resend.api_key = _RESEND_API_KEY
            safe_user_email = html.escape(user_email)
            safe_page_url = html.escape(page_url, quote=True)
            safe_message = html.escape(message).replace("\n", "<br>")
            reply_line = f"<b>Reply-to:</b> {safe_user_email}<br>" if user_email else ""
            resend.Emails.send({
                "from": _RESEND_FROM,
                "to": _FEEDBACK_EMAIL,
                "reply_to": user_email or None,
                "subject": f"[kozossegek.com feedback] {community_name} - {city}",
                "html": (
                    f"<p><b>Community:</b> {html.escape(community_name)}<br>"
                    f"<b>City:</b> {html.escape(city)}<br>"
                    f"<b>Topic:</b> {html.escape(topic)}<br>"
                    f"{reply_line}"
                    f"<b>Page:</b> <a href='{safe_page_url}'>{safe_page_url}</a></p>"
                    f"<hr><p>{safe_message}</p>"
                ),
            })
            log.info("feedback_email_sent", to=_FEEDBACK_EMAIL, community=community_name)
        except Exception as exc:
            log.warning("feedback_email_failed", error=str(exc))
    return JSONResponse({"ok": True})


@_fastapi.post("/report-not-community")
async def public_report_not_community(
    community_id: str = Form(""),
    community_name: str = Form(""),
    city: str = Form(""),
    topic: str = Form(""),
    source_url: str = Form(""),
    page_url: str = Form(""),
):
    if not community_name or not app_state.db_path:
        return JSONResponse({"ok": False})
    save_not_community_report(
        _db(), community_id, community_name, city, topic, source_url, page_url
    )
    log.info("not_community_reported", name=community_name, city=city)
    return JSONResponse({"ok": True})


@_fastapi.post("/suggest-edit")
async def public_suggest_edit(
    entity_type: str = Form("community"),
    entity_id: str = Form(""),
    entity_name: str = Form(""),
    entity_city: str = Form(""),
    entity_topic: str = Form(""),
    record_key: str = Form(""),
    change_type: str = Form(""),
    new_value: str = Form(""),
    notes: str = Form(""),
    email: str = Form(""),
):
    if not entity_name or not change_type or not notes.strip() or not email.strip():
        return JSONResponse({"ok": False, "error": "missing_fields"})
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    if entity_type not in {"community", "venue"}:
        return JSONResponse({"ok": False, "error": "invalid_entity_type"})
    community_change_types = {"wrong_city", "wrong_topic", "name_correction", "archive", "delete"}
    venue_change_types = {"wrong_info", "closed", "name_correction"}
    valid_types = community_change_types if entity_type == "community" else venue_change_types
    if change_type not in valid_types:
        return JSONResponse({"ok": False, "error": "invalid_change_type"})
    if change_type in {"wrong_city", "wrong_topic", "name_correction"} and not new_value.strip():
        return JSONResponse({"ok": False, "error": "missing_new_value"})
    save_edit_request(
        _db(), entity_type, entity_id, entity_name, entity_city, entity_topic,
        record_key, change_type, new_value.strip() or None, notes.strip(), email.strip(),
    )
    log.info("edit_request_submitted", entity=entity_name, change_type=change_type)
    return JSONResponse({"ok": True})


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


@_fastapi.get("/set-lang")
async def set_lang(lang: str = "en", next: str = "/"):
    from .i18n import LANGUAGES
    if lang not in LANGUAGES:
        lang = "en"
    safe_next = _safe_redirect_target(next, "/")
    resp = RedirectResponse(safe_next, status_code=302)
    resp.set_cookie("lang", lang, max_age=60 * 60 * 24 * 365, samesite="lax")
    return resp


@_fastapi.get("/terkep", response_class=HTMLResponse)
async def public_map(request: Request):
    cities_data = []
    for city in (app_state.cities or []):
        if city.country != "Hungary":
            continue
        coords = CITY_COORDS.get(city.name)
        if not coords:
            continue
        count = sum(len(_load_communities(city.name, t.name)) for t in (app_state.topics or []))
        cities_data.append({
            "name": city.name,
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
        **lang_context(request),
    })


@_fastapi.get("/varosok", response_class=HTMLResponse)
async def public_cities(request: Request, requested: str = ""):
    city_totals = dict(get_city_totals(_db())) if app_state.db_path else {}
    hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
    cities_list = sorted(
        [{"name": c.name, "slug": _slugify(c.name), "count": city_totals.get(c.name, 0)} for c in hu_cities],
        key=lambda c: (-c["count"], _hu_sort_key(c["name"])),
    )
    return templates.TemplateResponse(request, "public_cities.html", {
        "cities_list": cities_list,
        "total_cities": len(cities_list),
        "requested": requested,
        **lang_context(request),
    })


@_fastapi.get("/cities", response_class=HTMLResponse)
async def public_cities_en():
    return RedirectResponse("/varosok", status_code=301)


@_fastapi.post("/varosok/kerelem")
async def request_city(request: Request, city_name: str = Form(""), email: str = Form("")):
    if city_name.strip() and app_state.db_path:
        save_city_request(app_state.db_path, city_name, email)
    return RedirectResponse("/varosok?requested=" + city_name.strip(), status_code=303)


@_fastapi.post("/cities/request")
async def request_city_en(request: Request, city_name: str = Form(""), email: str = Form("")):
    return RedirectResponse("/varosok", status_code=301)


@_fastapi.get("/admin", response_class=HTMLResponse)
async def admin_root_redirect():
    return RedirectResponse("/admin/", status_code=301)


@_fastapi.get("/rolunk", response_class=HTMLResponse)
async def public_about(request: Request):
    hu_names = _hu_city_names()
    hu_topic_counts = _hu_topic_counts()
    city_totals = dict(get_city_totals(_db())) if app_state.db_path else {}
    venue_counts = {k: v for k, v in (get_venue_counts(_db()) if app_state.db_path else {}).items() if k in hu_names}
    person_counts = {k: v for k, v in (get_person_counts(_db()) if app_state.db_path else {}).items() if k in hu_names}
    all_hu_cities = sorted(
        [{"name": c.name, "slug": _slugify(c.name), "count": city_totals.get(c.name, 0)}
         for c in (app_state.cities or []) if c.country == "Hungary"],
        key=lambda c: _hu_sort_key(c["name"]),
    )
    return templates.TemplateResponse(request, "public_about.html", {
        "city_count": len(hu_names),
        "topic_count": len(app_state.topics or []),
        "total_records": sum(hu_topic_counts.values()),
        "total_venues": sum(venue_counts.values()),
        "total_persons": sum(person_counts.values()),
        "topics": app_state.topics or [],
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "topic_counts": hu_topic_counts,
        "all_hu_cities": all_hu_cities,
        **lang_context(request),
    })


@_fastapi.get("/about", response_class=HTMLResponse)
async def public_about_en():
    return RedirectResponse("/rolunk", status_code=301)


@_fastapi.get("/map", response_class=HTMLResponse)
async def public_map_en():
    return RedirectResponse("/terkep", status_code=301)


@_fastapi.get("/kozosseg-bekuldes", response_class=HTMLResponse)
async def submit_community_get(request: Request, city: str = "", topic: str = ""):
    init_db(_db())
    submitted = request.query_params.get("submitted") == "1"
    all_cities = sorted((c.name for c in (app_state.cities or [])), key=_hu_sort_key)
    _topic_labels = get_topic_labels(lang_context(request)["lang"])
    all_topics = [
        {"name": t.name, "label": _topic_labels.get(t.name, t.name.replace("_", " ").title())}
        for t in sorted(app_state.topics or [], key=lambda t: t.name)
    ]
    return templates.TemplateResponse(request, "public_submit_community.html", {
        "submitted": submitted,
        "city": city,
        "topic": topic,
        "all_cities": all_cities,
        "all_topics": all_topics,
        **lang_context(request),
    })


@_fastapi.post("/kozosseg-bekuldes")
async def submit_community_post(
    request: Request,
    name: str = Form(""),
    city: str = Form(""),
    topic: str = Form(""),
    source_url: str = Form(""),
    submitter_email: str = Form(""),
):
    if not all([name.strip(), city.strip(), topic.strip(), source_url.strip()]):
        return JSONResponse({"error": "missing_required_field"}, status_code=400)
    init_db(_db())
    save_community_submission(
        _db(), name.strip(), city.strip(), topic.strip(),
        source_url.strip(), submitter_email.strip() or None,
    )
    return RedirectResponse("/kozosseg-bekuldes?submitted=1", status_code=302)


@_fastapi.get("/robots.txt")
async def robots_txt():
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(
        "User-agent: *\n"
        "Disallow: /admin\n"
        "Disallow: /source/\n"
        "Disallow: /api/\n"
        "Disallow: /set-lang\n"
        "Disallow: /unsubscribe\n"
        "Sitemap: https://kozossegek.com/sitemap.xml\n"
    )


@_fastapi.get("/sitemap.xml")
async def sitemap(request: Request):
    from fastapi.responses import Response as _Response
    base = "https://kozossegek.com"

    locs: list[str] = [
        base + "/",
        base + "/rolunk",
        base + "/terkep",
        base + "/varosok",
        base + "/felfedezes",
        base + "/helyszinek",
        base + "/emberek",
        base + "/kozosseg-bekuldes",
    ]

    if app_state.db_path:
        init_db(app_state.db_path)

        # City pages + city+topic pages + community detail pages
        counts = get_city_topic_counts(_db())
        for city_name, topics in counts.items():
            city_sl = _slugify(city_name)
            city_locale = _city_locale(city_name)
            locs.append(f"{base}/{city_sl}")
            for topic_name in topics:
                topic_sl = _topic_url_slug(topic_name, city_locale)
                locs.append(f"{base}/{city_sl}/{topic_sl}")
                for record in get_communities(_db(), city_name, topic_name):
                    name_sl = _slugify(record.get("name", ""))
                    if name_sl:
                        locs.append(f"{base}/{city_sl}/{name_sl}")

        # Venue detail pages
        for v in get_all_venues(app_state.db_path):
            city_sl = _slugify(v.get("city", ""))
            name_sl = _slugify(v.get("name", ""))
            if city_sl and name_sl:
                locs.append(f"{base}/{city_sl}/helyszin/{name_sl}")

        # Person detail pages — deduplicated by name+city slug
        seen_persons: set[tuple[str, str]] = set()
        for p in get_all_persons(app_state.db_path):
            city_sl = _slugify(p.get("city", ""))
            name_sl = _slugify(p.get("name", ""))
            if city_sl and name_sl and (city_sl, name_sl) not in seen_persons:
                seen_persons.add((city_sl, name_sl))
                locs.append(f"{base}/{city_sl}/ember/{name_sl}")

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for loc in dict.fromkeys(locs):  # deduplicate while preserving order
        lines.append(
            f"  <url><loc>{loc}</loc><changefreq>weekly</changefreq></url>"
        )
    lines.append("</urlset>")
    return _Response("\n".join(lines), media_type="application/xml")


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN ROUTES  (prefix: /admin, protected by _BasicAuth)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_run_scopes() -> dict:
    """Compute expected search/fetch/AI call counts for each Run Now preset."""
    from ..extract import _prompt_hash, get_prompt as _ep
    cfg = app_state.pipeline_cfg
    if not cfg or not app_state.db_path:
        return {}
    # Best currently active model (same priority as pipeline extractor selection)
    if cfg.deepseek_api_key:
        model = cfg.deepseek_model or "deepseek-chat"
    elif cfg.groq_api_key:
        model = cfg.groq_model or "llama-3.1-70b-versatile"
    else:
        model = cfg.ollama_model or "llama3"
    try:
        extract_fp = _prompt_hash(_ep("extraction_system") + model)
        venue_fp   = _prompt_hash(_ep("venue_system") + model)
        person_fp  = _prompt_hash(_ep("person_system") + model)
        stats = get_scope_stats(app_state.db_path, extract_fp, venue_fp, person_fp)
        hu_names = list(_hu_city_names())
        stats_hu = get_scope_stats(app_state.db_path, extract_fp, venue_fp, person_fp, cities=hu_names) if hu_names else {"with_text": 0, "extract_match": 0, "venue_match": 0, "person_match": 0}
    except Exception:
        return {}
    n              = stats["with_text"]
    extract_needed = n - stats["extract_match"]
    venue_needed   = n - stats["venue_match"]
    person_needed  = n - stats["person_match"]
    n_hu              = stats_hu["with_text"]
    extract_needed_hu = n_hu - stats_hu["extract_match"]
    venue_needed_hu   = n_hu - stats_hu["venue_match"]
    person_needed_hu  = n_hu - stats_hu["person_match"]
    city_count     = len(app_state.cities or [])
    topic_count    = len(app_state.topics or [])
    hu_city_count  = len(hu_names)
    search_pairs   = city_count * topic_count
    search_pairs_hu = hu_city_count * topic_count
    return {
        "smart": {
            "search":    search_pairs,
            "search_hu": search_pairs_hu,
            "fetch":     None,
            "fetch_hu":  None,
            "ai":        extract_needed + venue_needed + person_needed,
            "ai_hu":     extract_needed_hu + venue_needed_hu + person_needed_hu,
        },
        "rebuild": {
            "search":    search_pairs,
            "search_hu": search_pairs_hu,
            "fetch":     n,
            "fetch_hu":  n_hu,
            "ai":        n + venue_needed + person_needed,
            "ai_hu":     n_hu + venue_needed_hu + person_needed_hu,
        },
        "reai": {
            "search":    0,
            "search_hu": 0,
            "fetch":     0,
            "fetch_hu":  0,
            "ai":        n + venue_needed + person_needed,
            "ai_hu":     n_hu + venue_needed_hu + person_needed_hu,
        },
    }


@admin.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    next_run = None
    schedule_cron = None
    if app_state.scheduler:
        jobs = app_state.scheduler.get_jobs()
        if jobs and jobs[0].next_run_time:
            next_run = jobs[0].next_run_time.strftime("%Y-%m-%d %H:%M UTC")
        if jobs:
            schedule_cron = str(jobs[0].trigger)

    cache_defaults = {}
    if app_state.pipeline_cfg:
        cache_defaults = {
            "skip_scraped": app_state.pipeline_cfg.cache_skip_scraped,
            "skip_extracted": app_state.pipeline_cfg.cache_skip_extracted,
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

    revalidation_pending = 0
    revalidation_pending_hu = 0
    reai_pending = 0
    reai_pending_hu = 0
    if app_state.db_path and app_state.db_path.exists():
        try:
            fp = _revalidation_fingerprint()
            revalidation_pending = count_communities_needing_revalidation(_db(), fp)
            hu_names = _hu_city_names()
            revalidation_pending_hu = count_communities_needing_revalidation(_db(), fp, list(hu_names))
        except Exception:
            pass
        try:
            from ..extract import _prompt_hash, get_prompt as _ep
            cfg = app_state.pipeline_cfg
            if cfg:
                if cfg.deepseek_api_key:
                    _model = cfg.deepseek_model or "deepseek-chat"
                elif cfg.groq_api_key:
                    _model = cfg.groq_model or "llama-3.1-70b-versatile"
                else:
                    _model = cfg.ollama_model or "llama3"
                extract_fp = _prompt_hash(_ep("extraction_system") + _model)
                venue_fp   = _prompt_hash(_ep("venue_system") + _model)
                person_fp  = _prompt_hash(_ep("person_system") + _model)
                _stats = get_scope_stats(app_state.db_path, extract_fp, venue_fp, person_fp)
                reai_pending = (
                    (_stats["with_text"] - _stats["extract_match"]) +
                    (_stats["with_text"] - _stats["venue_match"]) +
                    (_stats["with_text"] - _stats["person_match"])
                )
                _hu = list(_hu_city_names())
                if _hu:
                    _stats_hu = get_scope_stats(app_state.db_path, extract_fp, venue_fp, person_fp, cities=_hu)
                    reai_pending_hu = (
                        (_stats_hu["with_text"] - _stats_hu["extract_match"]) +
                        (_stats_hu["with_text"] - _stats_hu["venue_match"]) +
                        (_stats_hu["with_text"] - _stats_hu["person_match"])
                    )
        except Exception:
            pass

    all_cities = app_state.cities or []
    run_countries = sorted({c.country for c in all_cities if c.country})
    run_cities = sorted([{"name": c.name, "country": c.country} for c in all_cities],
                        key=lambda c: (c["country"], c["name"]))

    return templates.TemplateResponse(request, "dashboard.html", {
        "is_running": app_state.is_running,
        "last_run_at": app_state.last_run_at,
        "next_run": next_run,
        "schedule_cron": schedule_cron,
        "cache_defaults": cache_defaults,
        "run_history": run_history,
        "test_mode": test_mode,
        "test_cities": test_cities,
        "revalidation_pending": revalidation_pending,
        "revalidation_pending_hu": revalidation_pending_hu,
        "reai_pending": reai_pending,
        "reai_pending_hu": reai_pending_hu,
        "revalidate_state": _revalidate_state,
        "current_run_mode": app_state.current_run_mode,
        "run_countries": run_countries,
        "run_cities": run_cities,
    })


@admin.get("/results", response_class=HTMLResponse)
async def results(request: Request):
    city_topic_counts = get_city_topic_counts(_db())
    cities_map = {c.name: c.country for c in (app_state.cities or [])}
    rows = []
    for city, topics in city_topic_counts.items():
        country = cities_map.get(city, "")
        for topic, count in topics.items():
            rows.append({"city": city, "country": country, "topic": topic, "count": count})
    return templates.TemplateResponse(request, "results.html", {"rows": rows})


@admin.get("/results/{city}/{topic}", response_class=HTMLResponse)
async def result_detail(request: Request, city: str, topic: str):
    import hashlib
    records_data = get_communities(_db(), city, topic)
    records = [CommunityRecord.model_validate(r) for r in records_data]
    url_hashes = {
        r.source_url: hashlib.sha256(r.source_url.encode()).hexdigest()[:16]
        for r in records if r.source_url
    }
    return templates.TemplateResponse(request, "result_detail.html", {
        "city": city,
        "topic": topic,
        "records": records,
        "url_hashes": url_hashes,
    })


@admin.post("/false-positive/add")
async def fp_add_route(
    name: str = Form(...),
    city: str = Form(...),
    topic: str = Form(...),
    reason: str = Form(...),
    source_url: str = Form(""),
    fp_type: str = Form("extraction"),
    redirect_to: str = Form(""),
):
    fp_add(_db(), name, city, topic, reason, source_url, fp_type=fp_type)
    return RedirectResponse(_safe_redirect_target(redirect_to, "/admin/progress"), status_code=302)


@admin.post("/false-positive/remove")
async def fp_remove_route(
    name: str = Form(...),
    city: str = Form(...),
    topic: str = Form(...),
    fp_type: str = Form("extraction"),
    redirect_to: str = Form(""),
):
    fp_remove(_db(), name, city, topic, fp_type=fp_type)
    return RedirectResponse(_safe_redirect_target(redirect_to, "/admin/progress"), status_code=302)


@admin.get("/prompts", response_class=HTMLResponse)
async def prompts_page(request: Request):
    fps = fp_load(_db())

    def _versioned(fp_type: str, base: str) -> list[dict]:
        history = fp_load_history(_db(), fp_type)
        out = []
        for i, v in enumerate(reversed(history)):
            prev = history[-(i + 2)]["content"] if i + 1 < len(history) else base
            out.append({**v, "diff_html": fp_diff_html(prev, v["content"])})
        return out

    nc_reports = get_not_community_reports(_db()) if app_state.db_path else []
    extraction_rules = [fp for fp in fps if fp.get("fp_type") == "extraction_rule"]
    active_overrides = get_prompt_overrides(_db()) if app_state.db_path else {}
    return templates.TemplateResponse(request, "prompts.html", {
        "extraction_history": _versioned("extraction", SYSTEM_PROMPT),
        "enrichment_history": _versioned("enrichment", ENRICH_SYSTEM_PROMPT),
        "extraction_prompt": get_prompt("extraction_system") + build_prompt_section(fps, fp_type="extraction"),
        "enrichment_prompt": get_prompt("enrich_system") + build_prompt_section(fps, fp_type="enrichment"),
        "venue_prompt": get_prompt("venue_system"),
        "venue_user_template": get_prompt("venue_user"),
        "venue_schema": json.dumps(VENUE_SCHEMA, indent=2),
        "person_prompt": get_prompt("person_system"),
        "person_user_template": get_prompt("person_user"),
        "person_schema": json.dumps(PERSON_SCHEMA, indent=2),
        "false_positives": fps,
        "nc_reports": nc_reports,
        "extraction_rules": extraction_rules,
        "prompt_overrides": active_overrides,
        "prompt_defaults": {k: PROMPT_KEYS[k]() for k in PROMPT_KEYS},
    })


@admin.post("/prompts/save")
async def admin_prompt_save(key: str = Form(...), content: str = Form(...)):
    """Save an edited prompt override to DB and activate it immediately."""
    if key not in PROMPT_KEYS or not app_state.db_path:
        return JSONResponse({"ok": False, "error": "invalid key"})
    upsert_prompt_override(_db(), key, content)
    set_prompt_override(key, content)
    if key in ("extraction_system", "enrich_system"):
        _reload_fp_history(key)
    return JSONResponse({"ok": True})


@admin.post("/prompts/reset")
async def admin_prompt_reset(key: str = Form(...)):
    """Delete a prompt override (revert to hardcoded default)."""
    if key not in PROMPT_KEYS or not app_state.db_path:
        return JSONResponse({"ok": False, "error": "invalid key"})
    delete_prompt_override(_db(), key)
    set_prompt_override(key, None)
    return JSONResponse({"ok": True})


def _reload_fp_history(key: str) -> None:
    """Record a new prompt history entry when a base prompt is edited."""
    fp_type = "extraction" if key == "extraction_system" else "enrichment"
    from ..false_positives import _record_history
    _record_history(_db(), fp_type)


async def _ai_chat(user_msg: str, temperature: float = 0.3) -> str:
    """Chat via the configured extractor chain (DeepSeek → Groq → Ollama)."""
    cfg = app_state.pipeline_cfg
    if not cfg:
        raise RuntimeError("Pipeline not configured")
    extractor = _build_extractor(cfg)
    return await extractor.chat(user_msg, temperature)


@admin.post("/prompts/nc-assist")
async def prompts_nc_assist(notes: str = Form("")):
    """Ask DeepSeek to formulate a prompt addition based on not-community reports + admin notes."""
    if not app_state.db_path:
        return JSONResponse({"ok": False, "suggestion": ""})
    from ..extract import SYSTEM_PROMPT as _SP
    reports = get_not_community_reports(_db())

    examples_part = ""
    if reports:
        examples_part = "Flagged items (extracted but NOT real communities):\n" + "\n".join(
            f'- "{r["community_name"]}" ({r["city"]}, {r["topic"]})'
            for r in reports[:30]
        ) + "\n\n"

    notes_part = f"Admin context / reason:\n{notes.strip()}\n\n" if notes.strip() else ""

    user_msg = (
        f"{examples_part}"
        f"{notes_part}"
        "Current extraction system prompt (excerpt):\n"
        f"```\n{_SP[:2000]}\n```\n\n"
        "Write a short, concrete addition (1–5 sentences) to insert into the system prompt "
        "that will help the extractor avoid similar mistakes in the future. "
        "Write only the text to add — no commentary, no markdown headers, no quotes around it."
    )

    try:
        suggestion = await _ai_chat(user_msg, temperature=0.3)
        return JSONResponse({"ok": True, "suggestion": suggestion})
    except Exception as exc:
        log.warning("nc_assist_failed", error=str(exc))
        return JSONResponse({"ok": False, "suggestion": f"Error: {exc}"})


@admin.post("/prompts/nc-accept")
async def prompts_nc_accept(rule_text: str = Form(...)):
    """Save an AI-generated extraction rule into the prompt."""
    if not app_state.db_path or not rule_text.strip():
        return JSONResponse({"ok": False})
    from ..false_positives import add as fp_add
    fp_add(
        _db(),
        name=f"[AI rule] {rule_text[:60]}",
        city="",
        topic="",
        reason=rule_text.strip(),
        source_url="",
        fp_type="extraction_rule",
    )
    return JSONResponse({"ok": True})


@admin.post("/prompts/nc-rule-remove")
async def prompts_nc_rule_remove(name: str = Form(...)):
    """Remove an AI-generated extraction rule."""
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    from ..false_positives import remove as fp_remove
    fp_remove(_db(), name=name, city="", topic="", fp_type="extraction_rule")
    return JSONResponse({"ok": True})


@admin.get("/submissions", response_class=HTMLResponse)
async def admin_submissions_list(request: Request):
    init_db(_db())
    submissions = get_community_submissions(_db(), status="pending")
    return templates.TemplateResponse(request, "submissions.html", {
        "submissions": submissions,
    })


@admin.post("/submissions/{sub_id}/approve")
async def admin_submission_approve(sub_id: int, background_tasks: BackgroundTasks):
    if not app_state.db_path or not app_state.pipeline_cfg:
        return JSONResponse({"ok": False, "error": "not_configured"})
    sub_rows = get_community_submissions(_db(), status="pending")
    sub = next((r for r in sub_rows if r["id"] == sub_id), None)
    if not sub:
        return JSONResponse({"ok": False, "error": "not_found"})
    resolve_community_submission(_db(), sub_id, "approved")
    background_tasks.add_task(
        scrape_submitted_url,
        app_state.db_path,
        app_state.pipeline_cfg,
        sub["city"],
        sub["topic"],
        sub["source_url"],
    )
    return JSONResponse({"ok": True})


@admin.post("/submissions/{sub_id}/reject")
async def admin_submission_reject(sub_id: int):
    if not app_state.db_path:
        return JSONResponse({"ok": False, "error": "not_configured"})
    resolve_community_submission(_db(), sub_id, "rejected")
    return JSONResponse({"ok": True})


@admin.post("/communities/{community_id}/reai")
async def admin_community_reai(community_id: str, background_tasks: BackgroundTasks):
    if not app_state.db_path or not app_state.pipeline_cfg:
        return JSONResponse({"ok": False, "error": "not_configured"})
    community = find_community_by_id(_db(), community_id)
    if not community:
        return JSONResponse({"ok": False, "error": "not_found"})
    background_tasks.add_task(
        reextract_community,
        app_state.db_path,
        app_state.pipeline_cfg,
        community_id,
    )
    return JSONResponse({"ok": True})


# ── Re-validate existing communities ─────────────────────────────────────────

_home_stats_cache: dict | None = None  # invalidated after each pipeline run

_revalidate_state: dict = {"running": False, "done": 0, "total": 0, "flagged": 0, "skipped": 0, "error": ""}


def _revalidation_fingerprint() -> str:
    fps = fp_load(_db())
    rules_section = build_prompt_section(fps, fp_type="extraction")
    rules_section += build_prompt_section(fps, fp_type="extraction_rule") if fps else ""
    return _prompt_hash(
        SYSTEM_PROMPT[:1500] + rules_section +
        "Is this a GENUINE ongoing community group (not a business, event, or false positive)?"
    )


@admin.post("/revalidate/start")
async def admin_revalidate_start(
    city: str = Form(""),
    topic: str = Form(""),
    filter_city: str = Form(""),
    filter_country: str = Form(""),
):
    """Start a background re-validation of existing communities against the current prompt."""
    if not app_state.db_path or not app_state.pipeline_cfg:
        return JSONResponse({"ok": False, "error": "Not configured"})
    if _revalidate_state["running"]:
        return JSONResponse({"ok": False, "error": "Already running"})
    _city = (city or filter_city).strip()
    _country = filter_country.strip()
    app_state._run_task = asyncio.create_task(_run_revalidate(_city, topic.strip(), _country))
    return JSONResponse({"ok": True})


@admin.get("/revalidate/status")
async def admin_revalidate_status():
    return JSONResponse(_revalidate_state)


async def _run_revalidate(city: str, topic: str, country: str = "") -> None:
    _revalidate_state.update({"running": True, "done": 0, "total": 0, "flagged": 0, "skipped": 0, "error": ""})
    app_state.is_running = True
    app_state.current_run_mode = "revalidate"
    app_state.current_phase = "revalidate"
    app_state.current_url = None
    started = datetime.now(timezone.utc)
    success = False

    # Resolve city list from country scope when no specific city given
    country_cities: list[str] = []
    if country and not city:
        country_cities = [c.name for c in (app_state.cities or []) if c.country == country]

    try:
        fps = fp_load(_db())
        rules_section = build_prompt_section(fps, fp_type="extraction")
        rules_section += build_prompt_section(fps, fp_type="extraction_rule") if fps else ""
        revalidate_fp = _revalidation_fingerprint()

        if country_cities:
            all_count = sum(len(get_communities_for_city(_db(), c)) for c in country_cities)
            communities = [
                rec for c in country_cities
                for rec in get_communities_needing_revalidation(_db(), revalidate_fp, c, topic)
            ]
        else:
            all_count = len(get_all_communities(_db()) if not city else
                            (get_communities(_db(), city, topic) if topic else
                             get_communities_for_city(_db(), city)))
            communities = get_communities_needing_revalidation(_db(), revalidate_fp, city, topic)

        skipped = all_count - len(communities)
        _revalidate_state["skipped"] = skipped
        _revalidate_state["total"] = len(communities)
        log.info("revalidate_started", total=len(communities), skipped=skipped)

        for record in communities:
            name = record.get("name", "")
            c = record.get("city", city)
            t = record.get("topic", topic)
            src = record.get("source_url", "") or ""

            app_state.current_url = src or name

            def _field(key: str) -> str:
                v = record.get(key)
                return str(v) if v not in (None, "", [], {}) else ""

            record_lines = [
                f"- Name: {name}",
                f"- City: {c}, Topic: {t}",
            ]
            for key, label in [
                ("description", "Description"),
                ("website", "Website"),
                ("source_url", "Source URL"),
                ("tags", "Tags"),
                ("joinable", "Joinable"),
                ("join_process", "Join process"),
                ("founding_year", "Founded"),
                ("member_count", "Members"),
                ("fee", "Fee"),
                ("age_range", "Age range"),
                ("skill_level", "Skill level"),
                ("leader", "Leader"),
                ("email", "Email"),
                ("phone", "Phone"),
                ("confidence", "Confidence"),
            ]:
                v = _field(key)
                if v:
                    record_lines.append(f"- {label}: {v}")

            prompt = (
                f"Extraction rules:\n{SYSTEM_PROMPT[:1500]}{rules_section}\n\n"
                f"Community record:\n"
                + "\n".join(record_lines)
                + "\n\nIs this a GENUINE ongoing community group (not a business, event, or false positive)? "
                "Reply with exactly YES or NO, then a short reason (one sentence)."
            )
            try:
                answer = await _ai_chat(prompt, temperature=0.1)
                verdict = "NO" if answer.upper().startswith("NO") else "YES"
                rk = _community_record_key(name, c, t)
                log.info("revalidate_checked", name=name, city=c, verdict=verdict)
                if verdict == "NO":
                    try:
                        save_not_community_report(
                            _db(),
                            community_id=record.get("community_id", ""),
                            community_name=name,
                            city=c,
                            topic=t,
                            page_url=src,
                            source_url=src,
                        )
                    except Exception:
                        pass
                    set_community_hidden(_db(), rk, True)
                    _revalidate_state["flagged"] += 1
                else:
                    set_community_hidden(_db(), rk, False)
                set_community_revalidate_fingerprint(_db(), rk, revalidate_fp)
            except Exception as exc:
                log.warning("revalidate_item_failed", name=name, error=str(exc))
            _revalidate_state["done"] += 1

        success = True
        log.info("revalidate_done", done=_revalidate_state["done"], flagged=_revalidate_state["flagged"])
    except Exception as exc:
        _revalidate_state["error"] = str(exc)
        log.warning("revalidate_failed", error=str(exc))
    finally:
        _revalidate_state["running"] = False
        app_state.is_running = False
        app_state.current_phase = None
        app_state.current_url = None
        app_state.current_run_mode = None
        if app_state.db_path:
            from ..db import record_run
            record_run(app_state.db_path, started, datetime.now(timezone.utc),
                       "revalidate", success, None, 0)


_RECATEGORIZE_AUTO_THRESHOLD = 0.85
_RECATEGORIZE_MIN_THRESHOLD = 0.50
_recategorize_state: dict = {"running": False, "done": 0, "total": 0, "auto_applied": 0, "pending": 0, "skipped": 0, "error": ""}

_RECATEGORIZE_PROMPT = """You are an expert at classifying community groups into interest categories.

Given a community name and description, select the single best matching category from this list:
{topics}

Respond with a JSON object only — no markdown, no explanation outside the JSON:
{{"topic": "<slug>", "confidence": <0.0-1.0>, "reasoning": "<one sentence>"}}

Rules:
- confidence >= 0.85 means you are very sure
- confidence 0.50-0.84 means you are fairly sure but not certain
- confidence < 0.50 means it is unclear (you may still output your best guess)
- If the community genuinely fits "other" better than any listed category, output "other" with high confidence

Community name: {name}
Description: {description}"""


async def _ai_suggest_topic(name: str, description: str, topics: list[str]) -> tuple[str, float, str]:
    import json as _json
    prompt = _RECATEGORIZE_PROMPT.format(
        topics=", ".join(topics),
        name=name,
        description=(description or "")[:500],
    )
    raw = await _ai_chat(prompt, temperature=0.1)
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    data = _json.loads(raw)
    return str(data.get("topic", "other")), float(data.get("confidence", 0.0)), str(data.get("reasoning", ""))


@admin.get("/recategorize", response_class=HTMLResponse)
async def admin_recategorize_page(request: Request):
    if not app_state.db_path:
        return RedirectResponse("/admin", status_code=302)
    init_db(app_state.db_path)
    other_count = len(get_other_communities(app_state.db_path))
    pending = get_recategorize_suggestions(app_state.db_path, "pending")
    skipped = get_recategorize_suggestions(app_state.db_path, "skipped")
    applied = get_recategorize_suggestions(app_state.db_path, "applied")
    return templates.TemplateResponse(request, "recategorize.html", {
        "request": request,
        "other_count": other_count,
        "pending": pending,
        "skipped": skipped,
        "applied_count": len(applied),
        "state": _recategorize_state,
        "topic_labels": TOPIC_LABELS,
        "auto_threshold": _RECATEGORIZE_AUTO_THRESHOLD,
        "min_threshold": _RECATEGORIZE_MIN_THRESHOLD,
    })


@admin.post("/recategorize/run")
async def admin_recategorize_run(background_tasks: BackgroundTasks):
    if not app_state.db_path or not app_state.pipeline_cfg:
        return JSONResponse({"ok": False, "error": "Not configured"})
    if _recategorize_state["running"]:
        return JSONResponse({"ok": False, "error": "Already running"})
    background_tasks.add_task(_run_recategorize)
    return JSONResponse({"ok": True})


@admin.get("/recategorize/status")
async def admin_recategorize_status():
    return JSONResponse(_recategorize_state)


@admin.post("/recategorize/{suggestion_id}/approve")
async def admin_recategorize_approve(suggestion_id: int, topic: str = Form("")):
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    suggestions = (
        get_recategorize_suggestions(app_state.db_path, "pending") +
        get_recategorize_suggestions(app_state.db_path, "skipped")
    )
    s = next((x for x in suggestions if x["id"] == suggestion_id), None)
    if not s:
        return JSONResponse({"ok": False, "error": "Not found"})
    chosen = topic.strip() or s["suggested_topic"]
    apply_recategorize_suggestion(app_state.db_path, s["record_key"], chosen)
    log.info("recategorize_approved", name=s["community_name"], topic=chosen)
    return JSONResponse({"ok": True})


@admin.post("/recategorize/{suggestion_id}/reject")
async def admin_recategorize_reject(suggestion_id: int):
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    update_recategorize_status(app_state.db_path, suggestion_id, "rejected")
    return JSONResponse({"ok": True})


async def _run_recategorize() -> None:
    _recategorize_state.update({"running": True, "done": 0, "total": 0, "auto_applied": 0, "pending": 0, "skipped": 0, "error": ""})
    known_topics = [t for t in TOPIC_LABELS if t != "other"]
    try:
        communities = get_other_communities(app_state.db_path)
        _recategorize_state["total"] = len(communities)
        log.info("recategorize_started", total=len(communities))
        for c in communities:
            rk = c.get("record_key", "")
            name = c.get("name", "")
            description = c.get("description", "") or ""
            city = c.get("city", "")
            try:
                topic, confidence, reasoning = await _ai_suggest_topic(name, description, known_topics)
                if topic not in known_topics:
                    topic = "other"
                if confidence < _RECATEGORIZE_MIN_THRESHOLD or topic == "other":
                    upsert_recategorize_suggestion(
                        app_state.db_path, rk, name, city, description, topic, confidence, reasoning, "skipped"
                    )
                    _recategorize_state["skipped"] += 1
                    log.info("recategorize_skip", name=name, confidence=confidence, topic=topic)
                elif confidence >= _RECATEGORIZE_AUTO_THRESHOLD:
                    apply_recategorize_suggestion(app_state.db_path, rk, topic)
                    upsert_recategorize_suggestion(
                        app_state.db_path, rk, name, city, description, topic, confidence, reasoning, "applied"
                    )
                    _recategorize_state["auto_applied"] += 1
                    log.info("recategorize_auto", name=name, topic=topic, confidence=confidence)
                else:
                    upsert_recategorize_suggestion(
                        app_state.db_path, rk, name, city, description, topic, confidence, reasoning, "pending"
                    )
                    _recategorize_state["pending"] += 1
                    log.info("recategorize_pending", name=name, topic=topic, confidence=confidence)
            except Exception as exc:
                log.warning("recategorize_item_failed", name=name, error=str(exc))
            _recategorize_state["done"] += 1
        log.info("recategorize_done", **{k: _recategorize_state[k] for k in ("done", "auto_applied", "pending", "skipped")})
    except Exception as exc:
        _recategorize_state["error"] = str(exc)
        log.warning("recategorize_failed", error=str(exc))
    finally:
        _recategorize_state["running"] = False


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
        _validate_candidate_config(cities_yaml=cities_yaml)
        (CONFIG_DIR / "cities.yaml").write_text(cities_yaml, encoding="utf-8")
        _reload_runtime_config()
        return RedirectResponse("/admin/config?saved=cities", status_code=302)
    except Exception as exc:
        return _config_error_redirect(exc)


@admin.post("/config/topics")
async def save_topics(request: Request, topics_yaml: str = Form(...)):
    try:
        _validate_candidate_config(topics_yaml=topics_yaml)
        (CONFIG_DIR / "topics.yaml").write_text(topics_yaml, encoding="utf-8")
        _reload_runtime_config()
        return RedirectResponse("/admin/config?saved=topics", status_code=302)
    except Exception as exc:
        return _config_error_redirect(exc)


@admin.post("/config/settings")
async def save_settings(request: Request, settings_yaml: str = Form(...)):
    try:
        _validate_candidate_config(settings_yaml=settings_yaml)
        (CONFIG_DIR / "settings.yaml").write_text(settings_yaml, encoding="utf-8")
        _reload_runtime_config()
        return RedirectResponse("/admin/config?saved=settings", status_code=302)
    except Exception as exc:
        return _config_error_redirect(exc)


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
    return templates.TemplateResponse(request, "logs.html", {})


@admin.get("/api/logs/history")
async def log_history():
    history = broadcaster.get_all()
    return JSONResponse(history)


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
    run_communities: str = Form("on"),
    run_venues: str = Form("on"),
    run_persons: str = Form("on"),
    filter_country: str = Form(""),
    filter_city: str = Form(""),
):
    if app_state.is_running:
        return JSONResponse({"ok": False, "error": "already running"})

    if run_mode not in ("full", "ai_only"):
        run_mode = "full"
    app_state.is_running = True
    app_state.current_run_mode = run_mode
    _skip_scraped = (skip_scraped == "on")
    _skip_extracted = (skip_extracted == "on")
    _run_communities = (run_communities == "on")
    _run_venues = (run_venues == "on")
    _run_persons = (run_persons == "on")

    cities = app_state.cities or []
    if filter_city.strip():
        cities = [c for c in cities if c.name == filter_city.strip()]
    elif filter_country.strip():
        cities = [c for c in cities if c.country == filter_country.strip()]

    def _on_progress(phase: str | None, url: str | None) -> None:
        app_state.current_phase = phase
        app_state.current_url = url

    async def _run() -> None:
        started = datetime.now(timezone.utc)
        success = False
        pair_logs: list = []
        total_new = 0
        try:
            pair_logs, total_new = await run_pipeline(
                cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                run_mode=run_mode,
                skip_scraped=_skip_scraped,
                skip_extracted=_skip_extracted,
                run_communities=_run_communities,
                run_venues=_run_venues,
                run_persons=_run_persons,
                on_progress=_on_progress,
            )
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
        except Exception as exc:
            log.error("manual_run_failed", error=str(exc))
        finally:
            app_state.is_running = False
            app_state.current_phase = None
            app_state.current_url = None
            app_state.current_run_mode = None
            global _home_stats_cache
            _home_stats_cache = None
            if app_state.db_path:
                from ..db import record_run
                record_run(app_state.db_path, started, datetime.now(timezone.utc),
                           run_mode, success,
                           json.dumps(pair_logs) if pair_logs else None,
                           total_new)

    def _clear_cancelled_run(task: asyncio.Task) -> None:
        if task.cancelled():
            app_state.is_running = False
            app_state.current_phase = None
            app_state.current_url = None

    app_state._run_task = asyncio.create_task(_run())
    app_state._run_task.add_done_callback(_clear_cancelled_run)
    return JSONResponse({"ok": True})


@admin.post("/api/stop")
async def stop_run():
    task = app_state._run_task
    if task and not task.done():
        task.cancel()
        log.info("run_cancelled_by_user")
    return RedirectResponse("/admin/", status_code=302)


@admin.get("/api/status")
async def status():
    return {
        "is_running": app_state.is_running,
        "current_run_mode": app_state.current_run_mode,
        "last_run_at": app_state.last_run_at.isoformat() if app_state.last_run_at else None,
    }


@admin.get("/api/test-searxng")
async def test_searxng(q: str = "running club Budapest"):
    if not app_state.pipeline_cfg:
        return JSONResponse({"error": "not configured"}, status_code=503)
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as hc:
            resp = await hc.get(f"{app_state.pipeline_cfg.searxng_url}/search",
                                params={"q": q, "format": "json", "language": "en-US"})
            data = resp.json()
        return {
            "url": app_state.pipeline_cfg.searxng_url,
            "query": q,
            "status": resp.status_code,
            "results": len(data.get("results", [])),
            "unresponsive_engines": data.get("unresponsive_engines", []),
            "top3": [{"url": r["url"], "title": r.get("title", "")} for r in data.get("results", [])[:3]],
        }
    except Exception as exc:
        return JSONResponse({"error": str(exc), "url": app_state.pipeline_cfg.searxng_url}, status_code=500)


def _build_extractor(cfg):
    """Build the extractor chain (DeepSeek → Groq → Ollama) from PipelineConfig."""
    ollama = OllamaExtractor(
        base_url=cfg.ollama_url, model=cfg.ollama_model,
        temperature=cfg.ollama_temperature, timeout_seconds=cfg.ollama_timeout,
        max_text_chars=cfg.ollama_max_text_chars,
    )
    primaries = []
    if cfg.deepseek_api_key:
        primaries.append(DeepSeekExtractor(
            api_key=cfg.deepseek_api_key, model=cfg.deepseek_model,
            temperature=cfg.deepseek_temperature, timeout_seconds=cfg.deepseek_timeout,
            max_text_chars=cfg.deepseek_max_text_chars,
            rate_limit_seconds=cfg.deepseek_rate_limit_seconds,
        ))
    if cfg.groq_api_key:
        primaries.append(GroqExtractor(
            api_key=cfg.groq_api_key, model=cfg.groq_model,
            temperature=cfg.groq_temperature, timeout_seconds=cfg.groq_timeout,
            max_text_chars=cfg.groq_max_text_chars,
            rate_limit_seconds=cfg.groq_rate_limit_seconds,
        ))
    return FallbackExtractor(primaries=primaries, fallback=ollama) if primaries else ollama


async def _queue_worker() -> None:
    while True:
        # Wait until there is a pending item
        while True:
            pending = [i for i in app_state.queue_items if i["status"] == "pending"]
            if pending:
                break
            app_state.get_queue_event().clear()
            await app_state.get_queue_event().wait()

        item = pending[0]
        fn = app_state._queue_fns.pop(item["id"], None)
        if fn is None:
            item["status"] = "error"
            item["error"] = "fn missing"
            continue

        item["status"] = "running"
        item["started_at"] = datetime.utcnow().isoformat()
        try:
            await fn()
            item["status"] = "done"
        except Exception as exc:
            item["status"] = "error"
            item["error"] = str(exc)
        finally:
            item["done_at"] = datetime.utcnow().isoformat()

        # Keep at most 30 completed items
        done = [i for i in app_state.queue_items if i["status"] in ("done", "error")]
        if len(done) > 30:
            remove_ids = {x["id"] for x in done[:-30]}
            app_state.queue_items = [i for i in app_state.queue_items if i["id"] not in remove_ids]


def _enqueue(op: str, url_hash: str, url: str, city: str, topic: str, fn,
             priority: bool = False) -> dict:
    import uuid
    item: dict = {
        "id": uuid.uuid4().hex[:8],
        "op": op,
        "url_hash": url_hash,
        "url": url,
        "city": city,
        "topic": topic,
        "status": "pending",
        "added_at": datetime.utcnow().isoformat(),
        "started_at": None,
        "done_at": None,
        "error": None,
    }
    app_state._queue_fns[item["id"]] = fn
    if priority:
        # Insert right after the currently running item (position 1), or at front if idle
        insert_at = next(
            (i + 1 for i, x in enumerate(app_state.queue_items) if x["status"] == "running"),
            0,
        )
        app_state.queue_items.insert(insert_at, item)
    else:
        app_state.queue_items.append(item)
    app_state.get_queue_event().set()
    if not app_state._queue_worker_task or app_state._queue_worker_task.done():
        app_state._queue_worker_task = asyncio.create_task(_queue_worker())
    return item



@admin.get("/api/progress")
async def api_progress():
    """Return the current pipeline phase and active URL hash for live cache indicators."""
    import hashlib
    url = app_state.current_url
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16] if url else None
    return JSONResponse({
        "phase": app_state.current_phase,
        "url_hash": url_hash,
        "url": url,
    })


@admin.get("/api/cache-entries")
async def api_cache_entries():
    """Return fresh cache entries as JSON for live table refresh."""
    entries = []
    if app_state.cache_manager:
        entries = app_state.cache_manager.get_index()
    return JSONResponse(entries)

_fill_fields_state: dict = {"running": False, "done": 0, "total": 0, "patched": 0, "error": ""}


@admin.post("/cache/fill-fields")
async def cache_fill_fields(background_tasks: BackgroundTasks):
    """Re-run AI on cached page texts and fill null fields on existing communities."""
    if not app_state.cache_manager or not app_state.pipeline_cfg:
        return JSONResponse({"ok": False, "error": "Not configured"})
    if _fill_fields_state["running"]:
        return JSONResponse({"ok": False, "error": "Already running"})
    background_tasks.add_task(_run_fill_fields)
    return JSONResponse({"ok": True})


@admin.get("/cache/fill-fields/status")
async def cache_fill_fields_status():
    return JSONResponse(_fill_fields_state)


async def _run_fill_fields() -> None:
    _fill_fields_state.update({"running": True, "done": 0, "total": 0, "patched": 0, "error": ""})
    try:
        cfg = app_state.pipeline_cfg
        fps = fp_load(_db())
        fp_section = build_prompt_section(fps, fp_type="extraction")
        all_pages = app_state.cache_manager.get_all_scraped()
        _fill_fields_state["total"] = len(all_pages)

        for url, raw_text, city, topic in all_pages:
            locale = next((c.locale for c in (app_state.cities or []) if c.name == city), "en")
            try:
                extractor = _build_extractor(cfg)
                extracted = await extractor.extract(raw_text, city, topic, locale, url, fp_section)
                joinable = [r for r in extracted if r.joinable]
                if joinable:
                    _fill_fields_state["patched"] += patch_results(city, topic, joinable, _db())
            except Exception as exc:
                log.warning("fill_fields_item_failed", url=url, error=str(exc))
            _fill_fields_state["done"] += 1
    except Exception as exc:
        _fill_fields_state["error"] = str(exc)
        log.warning("fill_fields_failed", error=str(exc))
    finally:
        _fill_fields_state["running"] = False


@admin.get("/cache")
async def cache_redirect():
    return RedirectResponse("/admin/progress", status_code=301)

@admin.get("/cache/{url_hash}")
async def cache_detail_redirect(url_hash: str):
    return RedirectResponse(f"/admin/progress/{url_hash}", status_code=301)

@admin.get("/progress", response_class=HTMLResponse)
async def cache_page(
    request: Request,
    page: int = 1,
    f_city: str = "",
    f_topic: str = "",
    f_scraped: str = "",
    f_model: str = "",
    f_enrich_scraped: str = "",
    f_enrich_model: str = "",
    f_venue: str = "",
    f_person: str = "",
):
    entries = []
    if app_state.cache_manager:
        entries = app_state.cache_manager.get_index()
    url_counts = get_venue_person_counts_by_url(_db())

    all_cities = sorted({e.get("city") for e in entries if e.get("city")})
    all_topics = sorted({e.get("topic") for e in entries if e.get("topic")})
    all_models = sorted({e.get("extract_model") for e in entries if e.get("extract_model")})
    all_enrich_models = sorted({e.get("enrich_model") for e in entries if e.get("enrich_model")})

    def _match(e: dict) -> bool:
        if f_city and e.get("city") != f_city:
            return False
        if f_topic and e.get("topic") != f_topic:
            return False
        if f_scraped == "1" and not e.get("scraped_at"):
            return False
        if f_scraped == "0" and e.get("scraped_at"):
            return False
        if f_model == "__none__" and e.get("extract_model"):
            return False
        if f_model and f_model != "__none__" and e.get("extract_model") != f_model:
            return False
        if f_enrich_scraped == "1" and not e.get("enrich_scraped_at"):
            return False
        if f_enrich_scraped == "0" and e.get("enrich_scraped_at"):
            return False
        if f_enrich_model == "__none__" and e.get("enrich_model"):
            return False
        if f_enrich_model and f_enrich_model != "__none__" and e.get("enrich_model") != f_enrich_model:
            return False
        counts = url_counts.get(e.get("url", ""), {})
        vc = counts.get("venues", 0)
        pc = counts.get("persons", 0)
        if f_venue == "1" and not vc:
            return False
        if f_venue == "0" and vc:
            return False
        if f_person == "1" and not pc:
            return False
        if f_person == "0" and pc:
            return False
        return True

    filtered = [e for e in entries if _match(e)]
    page_size = 100
    total_all = len(entries)
    total_filtered = len(filtered)
    pages = max(1, (total_filtered + page_size - 1) // page_size)
    page = max(1, min(page, pages))
    paged = filtered[(page - 1) * page_size: page * page_size]
    filter_params = {k: v for k, v in {
        "f_city": f_city, "f_topic": f_topic, "f_scraped": f_scraped,
        "f_model": f_model, "f_enrich_scraped": f_enrich_scraped,
        "f_enrich_model": f_enrich_model, "f_venue": f_venue, "f_person": f_person,
    }.items() if v}
    return templates.TemplateResponse(request, "progress.html", {
        "entries": paged,
        "url_counts": url_counts,
        "page": page,
        "pages": pages,
        "total": total_all,
        "total_filtered": total_filtered,
        "all_cities": all_cities,
        "all_topics": all_topics,
        "all_models": all_models,
        "all_enrich_models": all_enrich_models,
        "f_city": f_city,
        "f_topic": f_topic,
        "f_scraped": f_scraped,
        "f_model": f_model,
        "f_enrich_scraped": f_enrich_scraped,
        "f_enrich_model": f_enrich_model,
        "f_venue": f_venue,
        "f_person": f_person,
        "filter_params": filter_params,
    })


@admin.get("/progress/{url_hash}", response_class=HTMLResponse)
async def cache_detail(request: Request, url_hash: str):
    if not app_state.cache_manager:
        return RedirectResponse("/admin/progress", status_code=302)

    entry = app_state.cache_manager.get_entry(url_hash)
    if not entry:
        return RedirectResponse("/admin/progress", status_code=302)

    store_records = []
    city = entry.get("city", "")
    topic = entry.get("topic", "")
    url = entry.get("url", "")
    if city and topic and url:
        all_records = get_communities(_db(), city, topic)
        store_records = [_ensure_community_id(r) for r in all_records if r.get("source_url") == url]

    schema_records = store_records or (entry.get("records") or [])
    schema_json = records_to_jsonld(schema_records)

    ollama_model = app_state.pipeline_cfg.ollama_model if app_state.pipeline_cfg else "?"
    max_text_chars = app_state.pipeline_cfg.ollama_max_text_chars if app_state.pipeline_cfg else 6000

    extract_user_prompt = ""
    if entry.get("raw_text") and entry.get("topic") and entry.get("city"):
        extract_user_prompt = USER_PROMPT_TEMPLATE.format(
            topic=entry.get("topic", ""),
            city=entry.get("city", ""),
            source_url=entry.get("url", ""),
            page_text=entry.get("raw_text", "")[:max_text_chars],
        )

    fps = fp_load(_db())
    fp_extraction = {(fp["name"], fp["city"], fp["topic"])
                     for fp in fps if fp.get("fp_type", "extraction") == "extraction"}
    fp_enrichment = {(fp["name"], fp["city"], fp["topic"])
                     for fp in fps if fp.get("fp_type") == "enrichment"}

    # Other cache entries from the same city/topic pair
    related_entries: list[dict] = []
    if city and topic:
        related_entries = [
            e for e in app_state.cache_manager.get_index()
            if e.get("city") == city and e.get("topic") == topic and e.get("url_hash") != url_hash
        ]

    return templates.TemplateResponse(request, "progress_detail.html", {
        "entry": entry,
        "store_records": store_records,
        "schema_json": schema_json,
        "extract_system_prompt": SYSTEM_PROMPT,
        "extract_user_prompt": extract_user_prompt,
        "extract_schema": json.dumps(EXTRACTION_SCHEMA, indent=2),
        "enrich_system_prompt": ENRICH_SYSTEM_PROMPT,
        "enrich_schema": json.dumps(ENRICH_SCHEMA, indent=2),
        "ollama_model": ollama_model,
        "related_entries": related_entries,
        "fp_extraction": fp_extraction,
        "fp_enrichment": fp_enrichment,
        "current_url_hash": url_hash,
    })


@admin.post("/progress/{url_hash}/delete-scraped")
async def cache_delete_scraped(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_scraped(url_hash)
    return RedirectResponse("/admin/progress", status_code=302)


@admin.post("/progress/{url_hash}/delete-extracted")
async def cache_delete_extracted(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_extracted(url_hash)
    return RedirectResponse("/admin/progress", status_code=302)


@admin.post("/progress/{url_hash}/delete")
async def cache_delete_entry(url_hash: str):
    if app_state.cache_manager:
        app_state.cache_manager.delete_entry(url_hash)
    return RedirectResponse("/admin/progress", status_code=302)


@admin.post("/progress/clear-all")
async def cache_clear_all():
    if app_state.cache_manager:
        app_state.cache_manager.clear_all()
    deleted = delete_all_communities(_db())
    log.info("clear_all_data", deleted_communities=deleted)
    return RedirectResponse("/admin/progress", status_code=302)


@admin.post("/progress/clear-person-cache")
async def cache_clear_persons():
    updated = 0
    if app_state.cache_manager:
        updated = app_state.cache_manager.clear_person_extracted()
    log.info("person_cache_cleared_via_admin", updated=updated)
    return RedirectResponse("/admin/progress", status_code=302)


@admin.post("/progress/{url_hash}/run-scrape")
async def cache_run_scrape(url_hash: str):
    if not app_state.cache_manager or not app_state.pipeline_cfg:
        return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)
    entry = app_state.cache_manager.get_entry(url_hash)
    if not entry:
        return RedirectResponse("/admin/progress", status_code=302)

    url = entry["url"]
    city = entry.get("city", "")
    topic = entry.get("topic", "")
    cfg = app_state.pipeline_cfg

    async def _do() -> None:
        import time as _time
        app_state.current_phase = "scrape"
        app_state.current_url = url
        try:
            t0 = _time.monotonic()
            text = await fetch_and_clean(
                url, cfg.fetch_blocked_domains, cfg.fetch_timeout,
                cfg.fetch_min_text_length, asyncio.Semaphore(1),
            )
            if text:
                app_state.cache_manager.save_scraped(
                    url, text, city, topic, duration_s=_time.monotonic() - t0
                )
        except Exception as exc:
            log.error("manual_scrape_failed", url=url, error=str(exc))
            raise
        finally:
            app_state.current_phase = None
            app_state.current_url = None

    _enqueue("scrape", url_hash, url, city, topic, _do, priority=True)
    return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)


@admin.post("/progress/{url_hash}/run-extract")
async def cache_run_extract(url_hash: str):
    if not app_state.cache_manager or not app_state.pipeline_cfg:
        return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)
    entry = app_state.cache_manager.get_entry(url_hash)
    if not entry or not entry.get("raw_text"):
        return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)

    url = entry["url"]
    city = entry.get("city", "")
    topic = entry.get("topic", "")
    raw_text = entry["raw_text"]
    cfg = app_state.pipeline_cfg
    locale = next((c.locale for c in (app_state.cities or []) if c.name == city), "en")

    async def _do() -> None:
        import time as _time
        app_state.current_phase = "extract"
        app_state.current_url = url
        try:
            extractor = _build_extractor(cfg)
            t0 = _time.monotonic()
            extracted = await extractor.extract(raw_text, city, topic, locale, url)
            extract_dur = _time.monotonic() - t0
            joinable = [r for r in extracted if r.joinable]
            app_state.cache_manager.save_extracted(
                url,
                joinable,
                duration_s=extract_dur,
                fingerprint=extractor.model_fingerprint,
                model=extractor.model,
            )
            if joinable:
                save_results(city, topic, joinable, _db())
        except Exception as exc:
            log.error("manual_extract_failed", url=url, error=str(exc))
            raise
        finally:
            app_state.current_phase = None
            app_state.current_url = None

    _enqueue("extract", url_hash, url, city, topic, _do, priority=True)
    return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)


@admin.post("/progress/{url_hash}/run-enrich")
async def cache_run_enrich(url_hash: str):
    if not app_state.cache_manager or not app_state.pipeline_cfg:
        return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)
    entry = app_state.cache_manager.get_entry(url_hash)
    records = app_state.cache_manager.get_extracted(entry["url"]) if entry else None
    if not records:
        return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)

    url = entry["url"]
    city = entry.get("city", "")
    topic = entry.get("topic", "")
    cfg = app_state.pipeline_cfg

    def _on_progress(phase: str | None, p_url: str | None) -> None:
        app_state.current_phase = phase
        app_state.current_url = p_url

    async def _do() -> None:
        try:
            extractor = _build_extractor(cfg)
            if cfg.dataforseo_login and cfg.dataforseo_password:
                from ..search import DataForSEOClient
                searxng = DataForSEOClient(
                    cfg.dataforseo_login, cfg.dataforseo_password,
                    rate_limit_seconds=cfg.search_rate_limit,
                )
            elif cfg.brave_api_key:
                searxng: BraveSearchClient | SearXNGClient = BraveSearchClient(
                    cfg.brave_api_key, rate_limit_seconds=cfg.search_rate_limit
                )
            else:
                searxng = SearXNGClient(cfg.searxng_url, rate_limit_seconds=cfg.search_rate_limit)
            semaphore = asyncio.Semaphore(cfg.fetch_max_concurrent)
            timing = {"scrape": 0.0, "extract": 0.0, "count": 0, "needed": False}
            enriched: list = []
            for record in records:
                if _needs_enrichment(record):
                    timing["needed"] = True
                    record = await _enrich_record(
                        record, searxng, extractor, cfg, semaphore, _on_progress, timing
                    )
                enriched.append(record)
            app_state.cache_manager.save_enriched_records(url, enriched)
            if timing["needed"]:
                app_state.cache_manager.mark_enrich_scraped(url, timing["scrape"])
                app_state.cache_manager.mark_enrich_extracted(url, timing["count"], timing["extract"], model=extractor.model)
            if enriched:
                save_results(city, topic, enriched, _db())
        except Exception as exc:
            log.error("manual_enrich_failed", url=url, error=str(exc))
            raise
        finally:
            app_state.current_phase = None
            app_state.current_url = None

    _enqueue("enrich", url_hash, url, city, topic, _do, priority=True)
    return RedirectResponse(f"/admin/progress/{url_hash}", status_code=302)


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


@admin.get("/venues", response_class=HTMLResponse)
async def admin_venues(request: Request, city: str = ""):
    if not app_state.db_path:
        return RedirectResponse("/admin", status_code=302)
    counts = get_venue_counts(app_state.db_path)
    venues = get_all_venues(app_state.db_path)
    if city:
        venues = [v for v in venues if v.get("city", "").lower() == city.lower()]

    # Collect all community_ids across shown venues, bulk-fetch in one query
    all_cids: list[str] = []
    for v in venues:
        all_cids.extend(v.get("community_ids") or [])
    community_map: dict[str, dict] = {}
    if all_cids:
        for c in get_communities_by_ids(app_state.db_path, list(dict.fromkeys(all_cids))):
            community_map[c.get("community_id", "")] = c

    # Resolve topic labels for display
    _topic_labels = get_topic_labels("hu")

    venue_histories = {
        v.get("venue_id", ""): get_venue_history(app_state.db_path, v.get("venue_id", ""))
        for v in venues if v.get("venue_id")
    }
    return templates.TemplateResponse(request, "venues.html", {
        "venues": venues,
        "counts": counts,
        "selected_city": city,
        "cities": sorted(counts.keys()),
        "community_map": community_map,
        "topic_labels": _topic_labels,
        "topic_icons": TOPIC_ICONS,
        "venue_histories": venue_histories,
    })


@admin.get("/persons", response_class=HTMLResponse)
async def admin_persons(request: Request, city: str = "", topic: str = ""):
    if not app_state.db_path:
        return RedirectResponse("/admin", status_code=302)
    counts = get_person_counts(app_state.db_path)
    all_cities = sorted(counts.keys())
    if city:
        persons = get_persons(app_state.db_path, city, topic or None)
    else:
        persons = []
        for c in all_cities:
            persons.extend(get_persons(app_state.db_path, c, topic or None))
    person_histories = {
        p.get("person_id", ""): get_person_history(app_state.db_path, p.get("person_id", ""))
        for p in persons if p.get("person_id")
    }
    return templates.TemplateResponse(request, "persons.html", {
        "persons": persons,
        "counts": counts,
        "selected_city": city,
        "selected_topic": topic,
        "cities": all_cities,
        "person_histories": person_histories,
    })


@admin.get("/not-community", response_class=HTMLResponse)
async def admin_not_community(request: Request):
    if not app_state.db_path:
        return RedirectResponse("/admin", status_code=302)
    reports = get_not_community_reports(_db())
    fps = fp_load(_db())
    fp_keys = {(fp["name"], fp["city"], fp["topic"]) for fp in fps}
    return templates.TemplateResponse(request, "not_community.html", {
        "reports": reports,
        "fp_keys": fp_keys,
        "topic_labels": TOPIC_LABELS,
        "topic_icons": TOPIC_ICONS,
    })


@admin.post("/not-community/{report_id}/approve")
async def admin_not_community_approve(report_id: int):
    """Promote report → false positive list, then delete the report."""
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    reports = get_not_community_reports(_db())
    r = next((x for x in reports if x["id"] == report_id), None)
    if not r:
        return JSONResponse({"ok": False, "error": "not found"})
    fp_add(
        _db(),
        name=r["community_name"],
        city=r["city"] or "",
        topic=r["topic"] or "",
        reason="Flagged by user as not a community",
        source_url=r["source_url"] or "",
        fp_type="extraction",
    )
    delete_not_community_report(_db(), report_id)
    return JSONResponse({"ok": True})


@admin.post("/not-community/{report_id}/dismiss")
async def admin_not_community_dismiss(report_id: int):
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    delete_not_community_report(_db(), report_id)
    return JSONResponse({"ok": True})


@admin.post("/not-community/ai-suggest")
async def admin_not_community_ai_suggest():
    """Ask DeepSeek to suggest prompt improvements based on flagged items."""
    if not app_state.db_path:
        return JSONResponse({"ok": False, "suggestion": ""})
    from ..extract import SYSTEM_PROMPT
    reports = get_not_community_reports(_db())
    if not reports:
        return JSONResponse({"ok": True, "suggestion": "No flagged items yet."})

    examples = "\n".join(
        f'- "{r["community_name"]}" ({r["city"]}, {r["topic"]})'
        for r in reports[:30]
    )
    user_msg = (
        "The following items were extracted by the pipeline but users flagged them "
        "as NOT being genuine community groups:\n\n"
        f"{examples}\n\n"
        "Current extraction system prompt:\n"
        f"```\n{SYSTEM_PROMPT[:2000]}\n```\n\n"
        "Based on these false positives, suggest concrete additions or changes to the "
        "system prompt that would prevent similar mistakes in the future. "
        "Be specific: quote the exact text to add or change. "
        "Output only the suggested prompt change, nothing else."
    )

    try:
        suggestion = await _ai_chat(user_msg, temperature=0.3)
        return JSONResponse({"ok": True, "suggestion": suggestion})
    except Exception as exc:
        log.warning("ai_suggest_failed", error=str(exc))
        return JSONResponse({"ok": False, "suggestion": f"Error: {exc}"})


@admin.get("/duplicates", response_class=HTMLResponse)
async def admin_duplicates(request: Request):
    if not app_state.db_path:
        return RedirectResponse("/admin", status_code=302)
    init_db(_db())
    candidates = get_duplicate_candidates(_db())
    enriched = []
    for c in candidates:
        winner_data = None
        loser_data = None
        if c["entity_type"] == "community":
            winner_data = get_community_by_record_key(_db(), c["winner_key"])
            loser_data = get_community_by_record_key(_db(), c["loser_key"])
        enriched.append({**c, "winner_data": winner_data, "loser_data": loser_data})
    return templates.TemplateResponse(request, "duplicates.html", {
        "candidates": enriched,
        "topic_labels": TOPIC_LABELS,
        "topic_icons": TOPIC_ICONS,
    })


@admin.post("/duplicates/scan")
async def admin_duplicates_scan():
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    from ..duplicates import detect_all
    count = detect_all(_db())
    return JSONResponse({"ok": True, "new_candidates": count})


@admin.post("/duplicates/flag")
async def admin_duplicates_flag(
    winner_key: str = Form(...),
    loser_key: str = Form(...),
    entity_type: str = Form("community"),
):
    """Manually flag two records as duplicates."""
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    from ..db import insert_duplicate_candidate
    if winner_key == loser_key:
        return JSONResponse({"ok": False, "error": "same record"})
    # Canonical order
    k1, k2 = (winner_key, loser_key) if winner_key <= loser_key else (loser_key, winner_key)
    inserted = insert_duplicate_candidate(_db(), entity_type, "", "", k1, k2, 1.0, "manual")
    return JSONResponse({"ok": True, "inserted": inserted})


@admin.get("/api/communities/search")
async def admin_communities_search(q: str = ""):
    """Return communities matching a name query (for manual duplicate flagging)."""
    if not app_state.db_path or len(q) < 2:
        return JSONResponse([])
    q_lower = q.lower()
    all_c = get_all_communities(_db())
    matches = [
        {"key": _community_record_key(r["name"], r["city"], r["topic"]),
         "label": f"{r['name']} – {r['city']} ({r.get('topic', '')})",
         "name": r["name"], "city": r["city"], "topic": r.get("topic", "")}
        for r in all_c if q_lower in r["name"].lower()
    ][:20]
    return JSONResponse(matches)


async def _ai_merge_communities(winner: dict, loser: dict) -> dict:
    """Use LLM to intelligently merge two community records."""
    fields = [
        "description", "meeting_schedule", "location", "contact", "website",
        "fee", "age_range", "skill_level", "join_process", "leader",
        "language", "frequency", "founding_year", "member_count",
        "email", "phone", "history", "tags", "social_links",
    ]
    w_summary = {k: winner.get(k) for k in ["name", "city", "topic"] + fields}
    l_summary = {k: loser.get(k) for k in ["name", "city", "topic"] + fields}
    prompt = (
        "Merge these two duplicate community records into one best record.\n\n"
        f"RECORD A:\n{json.dumps(w_summary, ensure_ascii=False, indent=2)}\n\n"
        f"RECORD B:\n{json.dumps(l_summary, ensure_ascii=False, indent=2)}\n\n"
        "Rules:\n"
        "- Keep Record A's name, city, topic unchanged\n"
        "- For text fields: pick the more informative/detailed value, or combine if both add unique info\n"
        "- For lists (tags, social_links): union both, remove duplicates\n"
        "- Omit fields that are null/empty in both records\n"
        "Output ONLY a JSON object with keys: " + ", ".join(fields) + "\n"
        "No explanation, just the JSON object."
    )
    merged_str = await _ai_chat(prompt, temperature=0.1)
    try:
        json_match = re.search(r'\{.*\}', merged_str, re.DOTALL)
        merged_fields = json.loads(json_match.group() if json_match else merged_str)
    except Exception:
        merged_fields = {}
    result = dict(winner)
    for f in fields:
        if f in merged_fields and merged_fields[f] is not None:
            result[f] = merged_fields[f]
    # Union source_urls
    w_urls = list(result.get("source_urls") or [])
    l_urls = list(loser.get("source_urls") or [])
    if result.get("source_url") and result["source_url"] not in w_urls:
        w_urls = [result["source_url"]] + w_urls
    if loser.get("source_url") and loser["source_url"] not in l_urls:
        l_urls = [loser["source_url"]] + l_urls
    result["source_urls"] = list(dict.fromkeys(w_urls + l_urls))
    return result


async def _bg_merge_community(db_path: Path, candidate_id: int, c: dict) -> None:
    winner = get_community_by_record_key(db_path, c["winner_key"])
    loser = get_community_by_record_key(db_path, c["loser_key"])
    if winner and loser:
        try:
            merged = await _ai_merge_communities(winner, loser)
            save_community_data(db_path, c["winner_key"], merged)
            set_community_hidden(db_path, c["loser_key"], True)
            log.info("ai_merge_done", winner=c["winner_key"], loser=c["loser_key"])
        except Exception as exc:
            log.warning("ai_merge_failed_fallback", error=str(exc))
            merge_community_into(db_path, c["winner_key"], c["loser_key"])
    else:
        merge_community_into(db_path, c["winner_key"], c["loser_key"])
    resolve_duplicate_candidate(db_path, candidate_id, "merged")


@admin.post("/duplicates/{candidate_id}/merge")
async def admin_duplicates_merge(candidate_id: int, background_tasks: BackgroundTasks):
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    candidates = get_duplicate_candidates(_db())
    c = next((x for x in candidates if x["id"] == candidate_id), None)
    if not c:
        return JSONResponse({"ok": False, "error": "not found"})
    if c["entity_type"] == "community":
        background_tasks.add_task(_bg_merge_community, _db(), candidate_id, c)
    else:
        resolve_duplicate_candidate(_db(), candidate_id, "merged")
    return JSONResponse({"ok": True})


@admin.post("/duplicates/{candidate_id}/dismiss")
async def admin_duplicates_dismiss(candidate_id: int):
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    resolve_duplicate_candidate(_db(), candidate_id, "dismissed")
    return JSONResponse({"ok": True})


@admin.get("/edit-requests", response_class=HTMLResponse)
async def admin_edit_requests(request: Request):
    if not app_state.db_path:
        return RedirectResponse("/admin", status_code=302)
    init_db(_db())
    edit_requests_list = get_edit_requests(_db(), status="pending")
    return templates.TemplateResponse(request, "edit_requests.html", {
        "requests": edit_requests_list,
        "topic_labels": TOPIC_LABELS,
    })


@admin.post("/edit-requests/{request_id}/approve")
async def admin_edit_requests_approve(request_id: int):
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    edit_requests_list = get_edit_requests(_db(), status="pending")
    r = next((x for x in edit_requests_list if x["id"] == request_id), None)
    if not r:
        return JSONResponse({"ok": False, "error": "not found"})
    if r["entity_type"] == "community":
        applied = apply_community_edit(_db(), r["record_key"], r["change_type"], r["new_value"])
        if not applied:
            return JSONResponse({"ok": False, "error": "community not found or unsupported change type"})
    resolve_edit_request(_db(), request_id, "approved")
    return JSONResponse({"ok": True})


@admin.post("/edit-requests/{request_id}/reject")
async def admin_edit_requests_reject(request_id: int):
    if not app_state.db_path:
        return JSONResponse({"ok": False})
    resolve_edit_request(_db(), request_id, "rejected")
    return JSONResponse({"ok": True})


_fastapi.include_router(admin)


@_fastapi.get("/helyszinek", response_class=HTMLResponse)
async def public_venues(request: Request, city: str = "", topic: str = ""):
    if not app_state.db_path:
        return RedirectResponse("/", status_code=302)
    init_db(app_state.db_path)
    hu_names = _hu_city_names()
    all_venues = [v for v in get_all_venues(app_state.db_path) if v.get("city", "") in hu_names]

    # Filter
    filtered = all_venues
    if city:
        filtered = [v for v in filtered if v.get("city", "").lower() == city.lower()]
    if topic:
        filtered = [v for v in filtered if topic in (v.get("welcomed_topics") or [])]

    # Build filter options from HU dataset
    all_cities = sorted({v.get("city", "") for v in all_venues if v.get("city")})
    all_topics = sorted({
        t for v in all_venues for t in (v.get("welcomed_topics") or []) if t
    })

    # Build city grouping for unfiltered view
    from collections import defaultdict
    city_map: dict = defaultdict(list)
    for v in filtered:
        city_map[v.get("city") or "—"].append(v)
    city_sections = [
        {"name": ci, "venues": vs} for ci, vs in sorted(city_map.items())
    ]

    return templates.TemplateResponse(request, "public_venues.html", {
        "venues": filtered,
        "city_sections": city_sections,
        "all_cities": all_cities,
        "all_topics": all_topics,
        "selected_city": city,
        "selected_topic": topic,
        "total_all": len(all_venues),
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        **lang_context(request),
    })


@_fastapi.get("/venues", response_class=HTMLResponse)
async def public_venues_en():
    return RedirectResponse("/helyszinek", status_code=301)


@_fastapi.get("/emberek", response_class=HTMLResponse)
async def public_people(request: Request, city: str = "", role: str = ""):
    if not app_state.db_path:
        return templates.TemplateResponse(request, "public_people.html", {
            "city_groups": [], "total_persons": 0,
            "all_cities": [], "all_roles": [],
            "selected_city": city, "selected_role": role,
            **lang_context(request),
        })
    init_db(app_state.db_path)
    hu_names = _hu_city_names()
    all_persons = get_all_persons(app_state.db_path)
    hu_persons = [p for p in all_persons if p.get("city", "") in hu_names]

    # Deduplicate: one card per person (name+city slug), merged across communities
    from collections import defaultdict
    seen: dict[tuple, dict] = {}
    for p in hu_persons:
        key = (_slugify(p.get("name", "")), _slugify(p.get("city", "")))
        if key not in seen:
            seen[key] = p
    unique = list(seen.values())

    all_cities = sorted({p.get("city", "") for p in unique if p.get("city")})
    all_roles = sorted({p.get("role", "") for p in unique if p.get("role")})

    filtered = unique
    if city:
        filtered = [p for p in filtered if p.get("city", "").lower() == city.lower()]
    if role:
        filtered = [p for p in filtered if p.get("role", "") == role]

    city_map: dict = defaultdict(list)
    for p in filtered:
        city_map[p.get("city") or "—"].append(p)
    city_groups = [
        {"name": c, "persons": sorted(persons, key=lambda x: x.get("name", ""))}
        for c, persons in sorted(city_map.items())
    ]
    total = sum(len(g["persons"]) for g in city_groups)
    return templates.TemplateResponse(request, "public_people.html", {
        "city_groups": city_groups,
        "total_persons": total,
        "all_cities": all_cities,
        "all_roles": all_roles,
        "selected_city": city,
        "selected_role": role,
        **lang_context(request),
    })


@_fastapi.get("/kereses", response_class=HTMLResponse)
async def public_search(request: Request):
    q = request.query_params.get("q", "").strip()
    results: dict = {"communities": [], "venues": [], "persons": []}
    if app_state.db_path and len(q) >= 2:
        init_db(app_state.db_path)
        results = search_all(app_state.db_path, q)
    communities = results["communities"]
    venues = results["venues"]
    persons = results["persons"]
    total = len(communities) + len(venues) + len(persons)
    return templates.TemplateResponse(request, "public_search.html", {
        "q": q,
        "communities": communities,
        "venues": venues,
        "persons": persons,
        "total": total,
        "topic_icons": TOPIC_ICONS,
        **lang_context(request),
    })


@_fastapi.get("/people", response_class=HTMLResponse)
async def public_people_en():
    return RedirectResponse("/emberek", status_code=301)


@_fastapi.get("/{city_slug}/helyszin/{venue_slug}", response_class=HTMLResponse)
async def public_venue_detail(request: Request, city_slug: str, venue_slug: str):
    if not app_state.db_path:
        return RedirectResponse("/helyszinek", status_code=302)
    city_name = _city_from_slug(city_slug)
    if city_name:
        venues = get_venues(app_state.db_path, city_name)
    else:
        # Cities not yet loaded (e.g. test env) — scan all venues and match by slug
        all_venues = get_all_venues(app_state.db_path)
        venues = [v for v in all_venues if _slugify(v.get("city", "")) == city_slug]
        if venues:
            city_name = venues[0].get("city", city_slug)
    venue = next((v for v in venues if _slugify(v.get("name", "")) == venue_slug), None)
    if not venue or not city_name:
        return RedirectResponse("/helyszinek", status_code=302)
    community_ids = venue.get("community_ids") or []
    communities = get_communities_for_venue(
        app_state.db_path, community_ids, venue.get("name", ""), city_name
    )
    city_locale = _city_locale(city_name)
    topic_url_slugs = {t.name: _topic_url_slug(t.name, city_locale) for t in (app_state.topics or [])}
    return templates.TemplateResponse(request, "public_venue_detail.html", {
        "v": venue,
        "city": city_name,
        "city_slug": city_slug,
        "communities": communities,
        "topic_url_slugs": topic_url_slugs,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        **lang_context(request),
    })


@_fastapi.get("/{city_slug}/ember/{name_slug}", response_class=HTMLResponse)
async def public_person_detail(request: Request, city_slug: str, name_slug: str):
    if not app_state.db_path:
        return RedirectResponse("/emberek", status_code=302)
    city_name = _city_from_slug(city_slug)
    if city_name:
        all_persons = get_persons(app_state.db_path, city_name)
    else:
        # Cities not yet loaded (e.g. test env) — scan all persons and match by city slug
        all_persons_all = get_all_persons(app_state.db_path)
        all_persons = [p for p in all_persons_all if _slugify(p.get("city", "")) == city_slug]
        if all_persons:
            city_name = all_persons[0].get("city", city_slug)
    if not city_name:
        return RedirectResponse("/emberek", status_code=302)
    merged = [p for p in all_persons if _slugify(p.get("name", "")) == name_slug]
    if not merged:
        return RedirectResponse("/emberek", status_code=302)
    community_entries = []
    seen: dict = {}
    for p in merged:
        community_name = p.get("community_name", "")
        topic = p.get("topic", "")
        key = (community_name, topic)
        role = p.get("role", "")
        if key in seen:
            # accumulate extra roles on the existing entry
            if role and role not in seen[key]["roles"]:
                seen[key]["roles"].append(role)
            continue
        entry = {
            "name": community_name,
            "url": f"/{city_slug}/{_slugify(community_name)}",
            "roles": [role] if role else [],
            "topic": topic,
            "topic_label": TOPIC_LABELS.get(topic, topic.replace("_", " ").title()),
            "topic_icon": TOPIC_ICONS.get(topic, "circle"),
        }
        seen[key] = entry
        community_entries.append(entry)
    person = merged[0]
    bio = next((p.get("bio") for p in merged if p.get("bio")), None)
    website = next((p.get("website") for p in merged if p.get("website")), None)
    social_links = list(dict.fromkeys(
        lnk for p in merged for lnk in (p.get("social_links") or [])
    ))
    return templates.TemplateResponse(request, "public_person_detail.html", {
        "person": person,
        "bio": bio,
        "website": website,
        "social_links": social_links,
        "community_entries": community_entries,
        "city": city_name,
        "city_slug": city_slug,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        **lang_context(request),
    })


@_fastapi.get("/{city_slug}/{segment}", response_class=HTMLResponse)
async def public_city_segment(
    request: Request, city_slug: str, segment: str, subscribed: str = ""
):
    city_name = _city_from_slug(city_slug)
    if not city_name:
        return RedirectResponse("/", status_code=302)
    topic_names = {t.name for t in (app_state.topics or [])}
    city_locale = _city_locale(city_name)
    # Try localized slug first, then fall back to English slug
    actual_topic = _topic_from_url_slug(segment, city_locale)
    if actual_topic not in topic_names:
        actual_topic = segment if segment in topic_names else segment.replace("-", "_")
    if actual_topic in topic_names:
        return await _render_explore(
            request, city=city_name, topic=[actual_topic], subscribed=subscribed
        )
    record = _find_community_by_slug(city_name, segment)
    if record:
        schema_json = records_to_jsonld([record])
        history = get_community_history(app_state.db_path, record.get("community_id", ""))
        rec_topic = record.get("topic", "")
        city_locale = _city_locale(city_name)
        topic_url_slugs = {t.name: _topic_url_slug(t.name, city_locale) for t in (app_state.topics or [])}
        community_venue = get_venue_for_community(
            app_state.db_path, record.get("community_id", ""), city_name
        ) if app_state.db_path else None
        community_persons = get_persons_for_community(
            app_state.db_path, record["name"], city_name
        ) if app_state.db_path else []
        return templates.TemplateResponse(request, "public_community.html", {
            "r": record,
            "topic": rec_topic,
            "topic_slug": _topic_url_slug(rec_topic, city_locale),
            "city": city_name,
            "schema_json": schema_json,
            "topic_icons": TOPIC_ICONS,
            "topic_labels": TOPIC_LABELS,
            "community_history": history,
            "topic_url_slugs": topic_url_slugs,
            "record_key": _community_record_key(record["name"], city_name, rec_topic),
            "community_venue": community_venue,
            "community_persons": community_persons,
            "all_cities": sorted((c.name for c in (app_state.cities or [])), key=_hu_sort_key),
            "all_topic_names": [(t.name, TOPIC_LABELS.get(t.name, t.name.replace("_", " ").title()))
                                for t in (app_state.topics or [])],
            **lang_context(request),
        })
    return RedirectResponse(f"/{city_slug}", status_code=302)


@_fastapi.get("/{city_slug}", response_class=HTMLResponse)
async def public_city(request: Request, city_slug: str, subscribed: str = ""):
    city_name = _city_from_slug(city_slug)
    if not city_name:
        return RedirectResponse("/", status_code=302)
    return await _render_explore(request, city=city_name, subscribed=subscribed)
