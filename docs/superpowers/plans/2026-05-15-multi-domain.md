# Multi-Domain Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve `közösségek.com` (HU cities, HU UI) and `meetapedia.com` (all cities, EN UI) from one FastAPI container, with no DB or pipeline changes.

**Architecture:** Domain is detected from the `Host` header in `_detect_site(request)` in `i18n.py`. `lang_context(request)` becomes the single injection point for `site`, `site_name`, `site_url`, and default `lang` — these flow into every public template automatically. `_site_cities(request)` in `app.py` filters the city list per domain. The home-page stats cache (`_home_stats_cache`) becomes a `dict[str, dict]` keyed by site so each domain caches independently.

**Tech Stack:** FastAPI, Jinja2, Python 3.11, pytest, SQLite (unchanged)

---

## File Map

| File | Change |
|------|--------|
| `scraper/web/i18n.py` | Add `_detect_site()`, update `make_t()` + `lang_context()`, replace `közösségek.com` in strings |
| `scraper/web/app.py` | Add `_site_cities()`, make `_home_stats_cache` site-keyed, update 6 routes |
| `scraper/web/templates/public_base.html` | `site_name`, `site_url`, `lang`, `locale` in head + nav + footer |
| `scraper/web/templates/public_home.html` | title |
| `scraper/web/templates/public_about.html` | title, og_desc, header label |
| `scraper/web/templates/public_explore.html` | 4 title variants, breadcrumb |
| `scraper/web/templates/public_community.html` | title, breadcrumb |
| `scraper/web/templates/public_cities.html` | title |
| `scraper/web/templates/public_people.html` | title |
| `scraper/web/templates/public_venue_detail.html` | title, breadcrumb |
| `scraper/web/templates/public_person_detail.html` | title, breadcrumb |
| `scraper/web/templates/public_venues.html` | title |
| `scraper/web/templates/public_search.html` | title, og_desc |
| `scraper/web/templates/public_map.html` | title |
| `scraper/web/templates/public_source.html` | title, breadcrumb |
| `scraper/web/templates/public_unsubscribe.html` | title, back-link text |
| `scraper/web/templates/public_submit_community.html` | title |
| `scraper/main.py` | Add intl cities pipeline run after HU run in `_scheduled_run` + `_startup_run` |
| `tests/test_i18n.py` | New: unit tests for `_detect_site`, `lang_context`, `_site_cities` |

---

## Task 1: `_detect_site()`, `make_t()` defaults, `lang_context()` in `i18n.py`

**Files:**
- Modify: `scraper/web/i18n.py` (lines 2252–2280, end of file)
- Create: `tests/test_i18n.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_i18n.py`:

```python
from unittest.mock import MagicMock

import pytest


def _req(host: str):
    req = MagicMock()
    req.headers.get.return_value = host
    return req


def test_detect_site_meetapedia():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("meetapedia.com")) == "meetapedia"


def test_detect_site_www_meetapedia():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("www.meetapedia.com")) == "meetapedia"


def test_detect_site_meetapedia_with_port():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("meetapedia.com:8000")) == "meetapedia"


def test_detect_site_kozossegek():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("kozossegek.com")) == "kozossegek"


def test_detect_site_localhost_defaults_to_kozossegek():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("localhost:8000")) == "kozossegek"


def test_detect_site_empty_defaults_to_kozossegek():
    from scraper.web.i18n import _detect_site
    assert _detect_site(_req("")) == "kozossegek"


def test_lang_context_meetapedia_sets_en_and_site_info():
    from scraper.web.i18n import lang_context
    ctx = lang_context(_req("meetapedia.com"))
    assert ctx["lang"] == "en"
    assert ctx["site"] == "meetapedia"
    assert ctx["site_name"] == "meetapedia.com"
    assert ctx["site_url"] == "https://meetapedia.com"


def test_lang_context_kozossegek_sets_hu_and_site_info():
    from scraper.web.i18n import lang_context
    ctx = lang_context(_req("kozossegek.com"))
    assert ctx["lang"] == "hu"
    assert ctx["site"] == "kozossegek"
    assert ctx["site_name"] == "közösségek.com"
    assert ctx["site_url"] == "https://közösségek.com"


def test_lang_context_includes_locale():
    from scraper.web.i18n import lang_context
    hu_ctx = lang_context(_req("kozossegek.com"))
    en_ctx = lang_context(_req("meetapedia.com"))
    assert hu_ctx["locale"] == "hu_HU"
    assert en_ctx["locale"] == "en_US"


def test_make_t_substitutes_defaults():
    from scraper.web.i18n import make_t, _T
    _T["en"]["_fmt_test"] = "site is {site_name}"
    try:
        t = make_t("en", site_name="testsite.com")
        assert t("_fmt_test") == "site is testsite.com"
    finally:
        del _T["en"]["_fmt_test"]


def test_make_t_kwargs_override_defaults():
    from scraper.web.i18n import make_t, _T
    _T["en"]["_fmt_test2"] = "site is {site_name}"
    try:
        t = make_t("en", site_name="default.com")
        assert t("_fmt_test2", site_name="override.com") == "site is override.com"
    finally:
        del _T["en"]["_fmt_test2"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_i18n.py -v
```

Expected: all fail with `ImportError` or `AssertionError` (functions don't exist yet).

- [ ] **Step 3: Implement `_detect_site()`, updated `make_t()`, updated `lang_context()`**

In `scraper/web/i18n.py`, replace the current `make_t` and `lang_context` functions (lines 2252–2280) with:

```python
def _detect_site(request: Request) -> str:
    host = request.headers.get("host", "").lower().removeprefix("www.").split(":")[0]
    return "meetapedia" if "meetapedia" in host else "kozossegek"


def make_t(lang: str, **defaults):
    base = _T.get("en", {})
    overrides = _T.get(lang, {})
    merged = {**base, **overrides}

    def t(key: str, **kwargs) -> str:
        text = merged.get(key, key)
        all_kwargs = {**defaults, **kwargs}
        if all_kwargs:
            try:
                text = text.format(**all_kwargs)
            except (KeyError, IndexError):
                pass
        return text
    return t


def lang_context(request: Request) -> dict:
    site = _detect_site(request)
    lang = "en" if site == "meetapedia" else "hu"
    site_name = "meetapedia.com" if site == "meetapedia" else "közösségek.com"
    site_url = f"https://{site_name}"
    locale = "en_US" if site == "meetapedia" else "hu_HU"
    return {
        "lang": lang,
        "site": site,
        "site_name": site_name,
        "site_url": site_url,
        "locale": locale,
        "lang_dir": "ltr",
        "t": make_t(lang, site_name=site_name),
        "languages": dict(sorted(LANGUAGES.items(), key=lambda x: x[1]["name"])),
        "current_lang": LANGUAGES.get(lang, LANGUAGES["en"]),
        "topic_labels": get_topic_labels(lang),
        "venue_type_labels": get_venue_type_labels(lang),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_i18n.py -v
```

Expected: all 12 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_i18n.py scraper/web/i18n.py
git commit -m "feat: add _detect_site, site-aware lang_context with make_t defaults"
```

---

## Task 2: Replace `közösségek.com` in `i18n.py` translation strings

**Files:**
- Modify: `scraper/web/i18n.py` (the `_T` dict, starting around line 796)

The `make_t()` now auto-substitutes `{site_name}` via defaults. Replace every hardcoded `közösségek.com` in translation string values with `{site_name}`.

- [ ] **Step 1: Find all occurrences**

```bash
grep -n "közösségek\.com" scraper/web/i18n.py
```

This will show lines in `about_title`, `about_description`, and `home_subtitle` (HU). Note the line numbers.

- [ ] **Step 2: Replace in `about_title` strings**

For every language that has `közösségek.com` in its `about_title`, replace it with `{site_name}`. The pattern is:

- `"About közösségek.com"` → `"About {site_name}"`
- `"Über közösségek.com"` → `"Über {site_name}"`
- `"À propos de közösségek.com"` → `"À propos de {site_name}"`
- `"Acerca de közösségek.com"` → `"Acerca de {site_name}"`
- `"Informazioni su közösségek.com"` → `"Informazioni su {site_name}"`
- `"Sobre o közösségek.com"` → `"Sobre o {site_name}"`
- `"О közösségek.com"` → `"О {site_name}"`
- `"Про közösségek.com"` → `"Про {site_name}"`
- `"关于 közösségek.com"` → `"关于 {site_name}"`
- `"közösségek.com について"` → `"{site_name} について"`
- `"közösségek.com 소개"` → `"{site_name} 소개"`
- `"حول közösségek.com"` → `"حول {site_name}"`
- `"درباره közösségek.com"` → `"درباره {site_name}"`
- `"אודות közösségek.com"` → `"אודות {site_name}"`
- `"közösségek.com के बारे में"` → `"{site_name} के बारे में"`
- `"közösségek.com Hakkında"` → `"{site_name} Hakkında"`
- `"Tentang közösségek.com"` → `"Tentang {site_name}"`
- `"Over közösségek.com"` → `"Over {site_name}"`
- `"O közösségek.com"` (PT) → `"O {site_name}"`
- `"Om közösségek.com"` → `"Om {site_name}"`
- `"O közösségek.com"` (PL/CS) → `"O {site_name}"`
- `"Despre közösségek.com"` → `"Despre {site_name}"`
- `"Σχετικά με το közösségek.com"` → `"Σχετικά με το {site_name}"`
- `"Giới thiệu közösségek.com"` → `"Giới thiệu {site_name}"`
- `"เกี่ยวกับ közösségek.com"` → `"เกี่ยวกับ {site_name}"`

Use your editor or sed to replace these. Each language is in its own `_T["xx"]` dict block.

- [ ] **Step 3: Replace in `about_description` strings**

For every language with `közösségek.com` in `about_description`, replace with `{site_name}`:

- EN: `"közösségek.com started in 2026..."` → `"{site_name} started in 2026..."`
- HU: `"A közösségek.com 2026-ban..."` → `"A {site_name} 2026-ban..."`
- DE: `"közösségek.com wurde 2026..."` → `"{site_name} wurde 2026..."`
- FR: `"közösségek.com a débuté en 2026..."` → `"{site_name} a débuté en 2026..."`
- ES: `"közösségek.com comenzó en 2026..."` → `"{site_name} comenzó en 2026..."`
- IT: `"közösségek.com è nata nel 2026..."` → `"{site_name} è nata nel 2026..."`
- PT: `"O közösségek.com começou em 2026..."` → `"O {site_name} começou em 2026..."`
- RU: `"közösségek.com был создан в 2026..."` → `"{site_name} был создан в 2026..."`

Check if any other languages also have `about_description` with the site name and apply the same pattern.

- [ ] **Step 4: Verify no `közösségek.com` remains in translation strings**

```bash
grep -n "közösségek\.com" scraper/web/i18n.py
```

Expected: zero matches. (If any remain, fix them.)

- [ ] **Step 5: Run existing tests to verify nothing broke**

```bash
.venv/bin/python -m pytest tests/test_i18n.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add scraper/web/i18n.py
git commit -m "feat: replace hardcoded közösségek.com with {site_name} in i18n strings"
```

---

## Task 3: `_site_cities()` + site-aware `_home_stats_cache` in `app.py`

**Files:**
- Modify: `scraper/web/app.py`
- Modify: `tests/test_i18n.py` (add tests)

- [ ] **Step 1: Add `_site_cities` tests to `tests/test_i18n.py`**

Append to `tests/test_i18n.py`:

```python
def test_site_cities_kozossegek_filters_to_hungary(monkeypatch):
    from scraper.web.app import _site_cities
    from scraper.web.state import app_state

    class FakeCity:
        def __init__(self, name, country):
            self.name = name
            self.country = country

    monkeypatch.setattr(app_state, "cities", [
        FakeCity("Budapest", "Hungary"),
        FakeCity("London", "United Kingdom"),
    ])
    result = _site_cities(_req("kozossegek.com"))
    assert len(result) == 1
    assert result[0].name == "Budapest"


def test_site_cities_meetapedia_returns_all(monkeypatch):
    from scraper.web.app import _site_cities
    from scraper.web.state import app_state

    class FakeCity:
        def __init__(self, name, country):
            self.name = name
            self.country = country

    monkeypatch.setattr(app_state, "cities", [
        FakeCity("Budapest", "Hungary"),
        FakeCity("London", "United Kingdom"),
    ])
    result = _site_cities(_req("meetapedia.com"))
    assert len(result) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_i18n.py::test_site_cities_kozossegek_filters_to_hungary tests/test_i18n.py::test_site_cities_meetapedia_returns_all -v
```

Expected: FAIL with `ImportError` (`_site_cities` doesn't exist yet).

- [ ] **Step 3: Add `_site_cities()` to `app.py`**

In `scraper/web/app.py`, add this function right after `_hu_city_names()` (after line 790):

```python
def _site_cities(request: Request) -> list:
    from .i18n import _detect_site
    cities = app_state.cities or []
    if _detect_site(request) == "kozossegek":
        return [c for c in cities if c.country == "Hungary"]
    return cities
```

- [ ] **Step 4: Change `_home_stats_cache` type and reset value**

In `scraper/web/app.py`, find line ~2050:
```python
_home_stats_cache: dict | None = None  # invalidated after each pipeline run
```

Replace with:
```python
_home_stats_cache: dict[str, dict] = {}  # keyed by site ("kozossegek" | "meetapedia")
```

Find the cache invalidation in the pipeline-run callback (around line 2526):
```python
global _home_stats_cache
_home_stats_cache = None
```

Replace with:
```python
global _home_stats_cache
_home_stats_cache = {}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_i18n.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add scraper/web/app.py tests/test_i18n.py
git commit -m "feat: add _site_cities helper and site-aware home stats cache"
```

---

## Task 4: Update routes in `app.py`

**Files:**
- Modify: `scraper/web/app.py`

Six routes need updating: `public_home`, `public_map`, `public_cities`, `public_about`, `sitemap`, `robots_txt`.

- [ ] **Step 1: Update `public_home` (line ~925)**

Replace the entire `public_home` function with:

```python
@_fastapi.get("/", response_class=HTMLResponse)
async def public_home(request: Request, city: str = ""):
    from .i18n import _detect_site
    global _home_stats_cache
    site = _detect_site(request)
    site_cities = _site_cities(request)
    site_city_names = {c.name for c in site_cities}
    topics = app_state.topics or []
    topic_url_slugs = {t.name: _topic_url_slug(t.name, "hu") for t in topics}
    if site not in _home_stats_cache:
        topic_counts = _hu_topic_counts() if site == "kozossegek" else _global_topic_counts()
        venue_counts = {k: v for k, v in (get_venue_counts(_db()) if app_state.db_path else {}).items() if k in site_city_names}
        person_counts = {k: v for k, v in (get_person_counts(_db()) if app_state.db_path else {}).items() if k in site_city_names}
        city_totals = dict(get_city_totals(_db())) if app_state.db_path else {}
        city_list = sorted(
            [{"name": c.name, "slug": _slugify(c.name), "count": city_totals.get(c.name, 0)} for c in site_cities],
            key=lambda x: (-x["count"], _hu_sort_key(x["name"])),
        )
        _home_stats_cache[site] = {
            "topic_counts": topic_counts,
            "total_records": sum(topic_counts.values()),
            "total_venues": sum(venue_counts.values()),
            "total_persons": sum(person_counts.values()),
            "city_list": city_list,
        }
    topic_counts = _home_stats_cache[site]["topic_counts"]
    city_list = _home_stats_cache[site]["city_list"]
    return templates.TemplateResponse(request, "public_home.html", {
        "cities": site_cities,
        "topics": topics,
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "selected_city": city,
        "topic_counts": topic_counts,
        "topic_url_slugs": topic_url_slugs,
        "total_records": _home_stats_cache[site]["total_records"],
        "total_venues": _home_stats_cache[site]["total_venues"],
        "total_persons": _home_stats_cache[site]["total_persons"],
        "hu_city_list": city_list,
        **lang_context(request),
    })
```

- [ ] **Step 2: Update `public_map` (line ~1401)**

Replace the `for city in (app_state.cities or []):` loop body. The new function:

```python
@_fastapi.get("/terkep", response_class=HTMLResponse)
async def public_map(request: Request):
    cities_data = []
    for city in _site_cities(request):
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
```

- [ ] **Step 3: Update `public_cities` (line ~1429)**

```python
@_fastapi.get("/varosok", response_class=HTMLResponse)
async def public_cities(request: Request, requested: str = ""):
    city_totals = dict(get_city_totals(_db())) if app_state.db_path else {}
    site_cities = _site_cities(request)
    cities_list = sorted(
        [{"name": c.name, "slug": _slugify(c.name), "count": city_totals.get(c.name, 0)} for c in site_cities],
        key=lambda c: (-c["count"], _hu_sort_key(c["name"])),
    )
    return templates.TemplateResponse(request, "public_cities.html", {
        "cities_list": cities_list,
        "total_cities": len(cities_list),
        "requested": requested,
        **lang_context(request),
    })
```

- [ ] **Step 4: Update `public_about` (line ~1467)**

```python
@_fastapi.get("/rolunk", response_class=HTMLResponse)
async def public_about(request: Request):
    from .i18n import _detect_site
    site = _detect_site(request)
    site_cities = _site_cities(request)
    site_city_names = {c.name for c in site_cities}
    site_topic_counts = _hu_topic_counts() if site == "kozossegek" else _global_topic_counts()
    city_totals = dict(get_city_totals(_db())) if app_state.db_path else {}
    venue_counts = {k: v for k, v in (get_venue_counts(_db()) if app_state.db_path else {}).items() if k in site_city_names}
    person_counts = {k: v for k, v in (get_person_counts(_db()) if app_state.db_path else {}).items() if k in site_city_names}
    all_site_cities = sorted(
        [{"name": c.name, "slug": _slugify(c.name), "count": city_totals.get(c.name, 0)}
         for c in site_cities],
        key=lambda c: _hu_sort_key(c["name"]),
    )
    return templates.TemplateResponse(request, "public_about.html", {
        "city_count": len(site_city_names),
        "topic_count": len(app_state.topics or []),
        "total_records": sum(site_topic_counts.values()),
        "total_venues": sum(venue_counts.values()),
        "total_persons": sum(person_counts.values()),
        "topics": app_state.topics or [],
        "topic_icons": TOPIC_ICONS,
        "topic_labels": TOPIC_LABELS,
        "topic_counts": site_topic_counts,
        "all_hu_cities": all_site_cities,
        **lang_context(request),
    })
```

- [ ] **Step 5: Update `sitemap` (line ~1557)**

Replace the hardcoded `base` and add city filtering:

```python
@_fastapi.get("/sitemap.xml")
async def sitemap(request: Request):
    from fastapi.responses import Response as _Response
    ctx = lang_context(request)
    base = ctx["site_url"]
    site_city_names = {c.name for c in _site_cities(request)}

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

        counts = get_city_topic_counts(_db())
        for city_name, topics in counts.items():
            if city_name not in site_city_names:
                continue
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

        for v in get_all_venues(app_state.db_path):
            if v.get("city", "") not in site_city_names:
                continue
            city_sl = _slugify(v.get("city", ""))
            name_sl = _slugify(v.get("name", ""))
            if city_sl and name_sl:
                locs.append(f"{base}/{city_sl}/helyszin/{name_sl}")

        seen_persons: set[tuple[str, str]] = set()
        for p in get_all_persons(app_state.db_path):
            if p.get("city", "") not in site_city_names:
                continue
            city_sl = _slugify(p.get("city", ""))
            name_sl = _slugify(p.get("name", ""))
            if city_sl and name_sl and (city_sl, name_sl) not in seen_persons:
                seen_persons.add((city_sl, name_sl))
                locs.append(f"{base}/{city_sl}/ember/{name_sl}")
```

Keep the XML generation at the end of the function unchanged (the `lines = [...]` block).

- [ ] **Step 6: Update `robots_txt` (line ~1543)**

Add `request: Request` parameter and make the Sitemap URL dynamic:

```python
@_fastapi.get("/robots.txt")
async def robots_txt(request: Request):
    from fastapi.responses import PlainTextResponse
    from .i18n import _detect_site
    site = _detect_site(request)
    site_url = "https://meetapedia.com" if site == "meetapedia" else "https://kozossegek.com"
    return PlainTextResponse(
        "User-agent: *\n"
        "Disallow: /admin\n"
        "Disallow: /source/\n"
        "Disallow: /api/\n"
        "Disallow: /set-lang\n"
        "Disallow: /unsubscribe\n"
        f"Sitemap: {site_url}/sitemap.xml\n"
    )
```

- [ ] **Step 7: Run the full test suite**

```bash
.venv/bin/python -m pytest -v
```

Expected: all existing tests PASS (no regressions). The `test_healthz_is_public_and_reports_status` test uses `TestClient` and should still pass.

- [ ] **Step 8: Commit**

```bash
git add scraper/web/app.py
git commit -m "feat: make public routes site-aware via _site_cities and lang_context"
```

---

## Task 5: Replace `közösségek.com` in all public templates

**Files:**
- Modify: 17 template files under `scraper/web/templates/`

All `{{ site_name }}`, `{{ site_url }}`, `{{ lang }}`, `{{ lang_dir }}`, `{{ locale }}` variables are already injected by `lang_context(request)` into every public route. Templates just need to use them.

- [ ] **Step 1: Update `public_base.html`**

This is the master layout. Make these changes:

**Line 2** — `<html lang="hu" dir="ltr">`:
```html
<html lang="{{ lang }}" dir="{{ lang_dir }}">
```

**Line 6** — default title fallback:
```html
  <title>{% block title %}{{ site_name }}{% endblock %}</title>
```

**Line 17** — `og:site_name`:
```html
  <meta property="og:site_name" content="{{ site_name }}">
```

**Line 19** — `og:url`:
```html
  <meta property="og:url" content="{{ site_url }}{{ request.url.path }}">
```

**Line 22** — `og:image`:
```html
  <meta property="og:image" content="{{ site_url }}{% block og_image_url %}/static/img/og/default.png{% endblock %}">
```

**Line 25** — `og:locale`:
```html
  <meta property="og:locale" content="{{ locale }}">
```

**Line 29** — `twitter:image`:
```html
  <meta name="twitter:image" content="{{ site_url }}{{ self.og_image_url() }}">
```

**Line 48** — nav logo text:
```html
      <span>{{ site_name }}</span>
```

**Line 152** — footer brand:
```html
      <span class="font-semibold text-[#1C1917] text-sm">{{ site_name }}</span>
```

- [ ] **Step 2: Update `public_home.html`**

Line 2: `közösségek.com – Találd meg a közösséged` → `{{ site_name }} – Találd meg a közösséged`

- [ ] **Step 3: Update `public_about.html`**

Line 2 — title: `Rólunk – közösségek.com` → `Rólunk – {{ site_name }}`

Line 3 — og_desc: replace the inline Hungarian sentence with the i18n key (which will include `{site_name}` substitution after Task 2):
```html
{% block og_desc %}{{ t('about_description') }}{% endblock %}
```

Line 9 — header label: `közösségek.com` → `{{ site_name }}`

- [ ] **Step 4: Update `public_explore.html`**

Replace all occurrences of `közösségek.com` with `{{ site_name }}`. There are 5: four title block variants and one breadcrumb link text.

- [ ] **Step 5: Update `public_community.html`**

Replace both occurrences: title (`{{ r.name }} – közösségek.com`) and breadcrumb link text (`közösségek.com`) → `{{ site_name }}`.

- [ ] **Step 6: Update `public_cities.html`**

Title: `Magyar városok – közösségek.com` → `Magyar városok – {{ site_name }}`

- [ ] **Step 7: Update `public_people.html`**

Title: `Emberek – közösségek.com` → `Emberek – {{ site_name }}`

- [ ] **Step 8: Update `public_venue_detail.html`**

Replace both occurrences: title (`{{ v.name }} – közösségek.com`) and breadcrumb (`közösségek.com`) → `{{ site_name }}`.

- [ ] **Step 9: Update `public_person_detail.html`**

Replace both occurrences: title (`{{ person.name }} – közösségek.com`) and breadcrumb (`közösségek.com`) → `{{ site_name }}`.

- [ ] **Step 10: Update `public_venues.html`**

Title: `Helyszínek – közösségek.com` → `Helyszínek – {{ site_name }}`

- [ ] **Step 11: Update `public_search.html`**

Replace both occurrences of `– közösségek.com` (title and og_desc) → `– {{ site_name }}`.

- [ ] **Step 12: Update `public_map.html`**

Title: `Térkép – közösségek.com` → `Térkép – {{ site_name }}`

- [ ] **Step 13: Update `public_source.html`**

Replace both occurrences: title (`– közösségek.com`) and breadcrumb (`közösségek.com`) → `{{ site_name }}`.

- [ ] **Step 14: Update `public_unsubscribe.html`**

Title: `Leiratkozás – közösségek.com` → `Leiratkozás – {{ site_name }}`

Back-link text: `Back to közösségek.com` → `Back to {{ site_name }}`

- [ ] **Step 15: Update `public_submit_community.html`**

Title: `Közösség beküldése – közösségek.com` → `Közösség beküldése – {{ site_name }}`

- [ ] **Step 16: Verify no `közösségek.com` remains in templates**

```bash
grep -r "közösségek\.com" scraper/web/templates/
```

Expected: zero matches.

- [ ] **Step 17: Run full test suite**

```bash
.venv/bin/python -m pytest -v
```

Expected: all PASS.

- [ ] **Step 18: Commit**

```bash
git add scraper/web/templates/ scraper/web/i18n.py
git commit -m "feat: replace hardcoded közösségek.com with site_name in all public templates"
```

---

## Task 6: International cities scheduler run in `main.py`

**Files:**
- Modify: `scraper/main.py`

Add an international cities pipeline run after the HU run in both `_scheduled_run` and `_startup_run`.

Note: These are inner functions in `main()` and are not directly unit-testable in isolation. The logic is simple additive code — run HU first, then intl. Validate by checking logs after deploy.

- [ ] **Step 1: Update `_scheduled_run` in `main.py`**

Find `_scheduled_run` (around line 120). Add `intl_cities` variable and a second `run_pipeline` call inside the `try` block.

Replace:
```python
        hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
        try:
            pair_logs = await run_pipeline(
                hu_cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                on_progress=_on_progress,
            )
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
```

With:
```python
        hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
        intl_cities = [c for c in (app_state.cities or []) if c.country != "Hungary"]
        try:
            pair_logs = await run_pipeline(
                hu_cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                on_progress=_on_progress,
            )
            if intl_cities:
                intl_logs = await run_pipeline(
                    intl_cities,
                    app_state.topics,
                    app_state.pipeline_cfg,
                    cache=app_state.cache_manager,
                    on_progress=_on_progress,
                )
                pair_logs += intl_logs
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
```

- [ ] **Step 2: Update `_startup_run` in `main.py`**

Find `_startup_run` (around line 163). Apply the same pattern. Replace:

```python
        hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
        try:
            pair_logs = await run_pipeline(
                hu_cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                on_progress=_on_progress,
                run_mode=startup_mode,
            )
            app_state.last_run_at = datetime.now(timezone.utc)
            success = True
```

With:
```python
        hu_cities = [c for c in (app_state.cities or []) if c.country == "Hungary"]
        intl_cities = [c for c in (app_state.cities or []) if c.country != "Hungary"]
        try:
            pair_logs = await run_pipeline(
                hu_cities,
                app_state.topics,
                app_state.pipeline_cfg,
                cache=app_state.cache_manager,
                on_progress=_on_progress,
                run_mode=startup_mode,
            )
            if intl_cities:
                intl_logs = await run_pipeline(
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
```

- [ ] **Step 3: Run full test suite**

```bash
.venv/bin/python -m pytest -v
```

Expected: all PASS (existing `test_main.py` tests only test `_cron_fields` and are unaffected).

- [ ] **Step 4: Commit**

```bash
git add scraper/main.py
git commit -m "feat: add international cities pipeline run after HU run in scheduler and startup"
```

---

## Final: CHANGELOG update

- [ ] **Step 1: Add entry to CHANGELOG.md**

Add under the `## [Unreleased]` or today's date section:

```markdown
## 2026-05-15

### Added
- Multi-domain support: `közösségek.com` (HU cities, HU UI) and `meetapedia.com` (all cities, EN UI) served from one container
- `_detect_site(request)` in `i18n.py` for Host-header-based domain detection
- `_site_cities(request)` in `app.py` for per-domain city filtering
- Site-aware home stats cache keyed by domain
- International cities pipeline run in scheduler and startup after HU run
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: add 2026-05-15 multi-domain entry to CHANGELOG"
```
