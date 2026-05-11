# Venue & Person Detail Pages — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Public detail pages for venues (`/{city_slug}/helyszin/{venue_slug}`) and persons (`/{city_slug}/ember/{name_slug}`), linked from every place they appear.

**Architecture:** 5 tasks — (1) new DB function, (2) venue route+template, (3) person route+template, (4) link venue cards in list pages, (5) link persons from community page + populate /emberek list.

**Tech Stack:** FastAPI, Jinja2, SQLite, Tailwind (CDN), Phosphor Icons. No local server — read templates directly.

---

### Task 1: Add `get_communities_for_venue` to db.py

**Files:**
- Modify: `scraper/db.py` — after `get_communities_by_ids` (line ~782)
- Modify: `scraper/db.py` `__all__` if present (skip if no `__all__`)
- Test: `tests/test_venue_person_pages.py` (new file)

- [ ] **Step 1: Write the failing test**

Create `tests/test_venue_person_pages.py`:

```python
from pathlib import Path
from scraper.db import (
    init_db, upsert_venues, upsert_persons,
    get_communities_for_venue,
)
from scraper.models import VenueRecord, PersonRecord
from scraper.store import save_results
from scraper.models import CommunityRecord


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _venue(name="Müpa Budapest", city="Budapest", community_ids=None):
    return VenueRecord(
        name=name, city=city, locale="hu",
        source_url="https://mupa.hu",
        extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"],
        community_ids=community_ids or [],
    )


def test_get_communities_for_venue_by_community_ids(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Zenei Kör", topic="music", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "music", [r], db)
    cid = r.community_id
    upsert_venues(db, [_venue(community_ids=[cid]).model_dump()])

    result = get_communities_for_venue(db, [cid], "Müpa Budapest", "Budapest")
    assert len(result) == 1
    assert result[0]["name"] == "Zenei Kör"


def test_get_communities_for_venue_fallback_location(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Tánc Csoport", topic="dance", city="Budapest", locale="hu",
        source_url="https://b.test", extracted_at="2026-01-01T00:00:00+00:00",
        location="Müpa Budapest nagyszínpad",
    )
    save_results("Budapest", "dance", [r], db)
    upsert_venues(db, [_venue().model_dump()])

    result = get_communities_for_venue(db, [], "Müpa Budapest", "Budapest")
    assert len(result) == 1
    assert result[0]["name"] == "Tánc Csoport"


def test_get_communities_for_venue_empty(tmp_path):
    db = _db(tmp_path)
    upsert_venues(db, [_venue().model_dump()])
    result = get_communities_for_venue(db, [], "Müpa Budapest", "Budapest")
    assert result == []
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_venue_person_pages.py -v
```

Expected: ImportError — `cannot import name 'get_communities_for_venue'`

- [ ] **Step 3: Implement the function in db.py**

Add after `get_communities_by_ids` (after line 782):

```python
def get_communities_for_venue(
    db_path: Path,
    community_ids: list[str],
    venue_name: str,
    city: str,
) -> list[dict]:
    """Return communities associated with a venue.
    Tries community_ids first; falls back to location-text match."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        if community_ids:
            placeholders = ",".join("?" * len(community_ids))
            rows = conn.execute(
                f"SELECT data FROM communities WHERE community_id IN ({placeholders}) AND hidden=0",
                community_ids,
            ).fetchall()
            if rows:
                return [json.loads(r[0]) for r in rows]
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? AND hidden=0"
            " AND json_extract(data,'$.location') LIKE ?",
            (city, f"%{venue_name}%"),
        ).fetchall()
    return [json.loads(r[0]) for r in rows]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_venue_person_pages.py -v
```

Expected: 3 PASSED

- [ ] **Step 5: Import `get_venues` and `get_communities_for_venue` in app.py**

In `scraper/web/app.py`, add `get_venues` and `get_communities_for_venue` to the `from ..db import (...)` block (lines 26–71). Add after `get_venues_by_city_topic`:

```python
    get_venues,
    get_communities_for_venue,
```

- [ ] **Step 6: Commit**

```bash
git add scraper/db.py scraper/web/app.py tests/test_venue_person_pages.py
git commit -m "feat: add get_communities_for_venue DB function"
```

---

### Task 2: Venue detail route and template

**Files:**
- Create: `scraper/web/templates/public_venue_detail.html`
- Modify: `scraper/web/app.py` — insert route before `/{city_slug}/{segment}` (before line 2985)
- Test: `tests/test_venue_person_pages.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_venue_person_pages.py`:

```python
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def test_venue_detail_page_returns_200(tmp_path):
    db = _db(tmp_path)
    v = _venue(name="Müpa Budapest", city="Budapest")
    upsert_venues(db, [v.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/budapest/helyszin/mupa-budapest")
        assert resp.status_code == 200
        assert "Müpa Budapest" in resp.text
    finally:
        app_state.db_path = old_db


def test_venue_detail_page_404_redirects(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get(
            "/budapest/helyszin/nem-letezik", follow_redirects=False
        )
        assert resp.status_code == 302
        assert resp.headers["location"] == "/helyszinek"
    finally:
        app_state.db_path = old_db
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_venue_person_pages.py::test_venue_detail_page_returns_200 -v
```

Expected: 404 (route doesn't exist yet)

- [ ] **Step 3: Add the route to app.py**

Insert before `@_fastapi.get("/{city_slug}/{segment}", ...)` (before line 2985):

```python
@_fastapi.get("/{city_slug}/helyszin/{venue_slug}", response_class=HTMLResponse)
async def public_venue_detail(request: Request, city_slug: str, venue_slug: str):
    city_name = _city_from_slug(city_slug)
    if not city_name or not app_state.db_path:
        return RedirectResponse("/helyszinek", status_code=302)
    venues = get_venues(app_state.db_path, city_name)
    venue = next((v for v in venues if _slugify(v.get("name", "")) == venue_slug), None)
    if not venue:
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
```

- [ ] **Step 4: Create `scraper/web/templates/public_venue_detail.html`**

```html
{% extends "public_base.html" %}
{% block title %}{{ v.name }} – közösségek.com{% endblock %}
{% block og_desc %}{{ v.description or (v.name + ' helyszín ' + city + 'ban.') }}{% endblock %}
{% block content %}

<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">
      <a href="/{{ city_slug }}" class="hover:text-white transition-colors">{{ city }}</a>
      <span class="mx-1 opacity-60">›</span>
      <a href="/helyszinek?city={{ city | urlencode }}" class="hover:text-white transition-colors">Helyszínek</a>
    </p>
    <div class="flex items-start gap-3">
      <div>
        <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">{{ v.name }}</h1>
        {% if v.venue_type %}
        <span class="inline-block mt-1.5 text-[11px] font-medium px-2 py-0.5 rounded bg-white/20 text-white/90 border border-white/30">
          {{ v.venue_type.replace('_',' ') }}
        </span>
        {% endif %}
      </div>
    </div>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-6 grid grid-cols-1 lg:grid-cols-3 gap-6">

  <!-- Main info -->
  <div class="lg:col-span-2 flex flex-col gap-4">

    {% if v.description %}
    <div class="bg-white rounded-xl border border-[#EAE5DB] p-5">
      <p class="text-sm text-[#6A6259] leading-relaxed">{{ v.description }}</p>
    </div>
    {% endif %}

    {% if v.welcomed_topics %}
    <div class="bg-white rounded-xl border border-[#EAE5DB] p-5">
      <h2 class="text-xs font-semibold uppercase tracking-[0.08em] text-[#8C8478] mb-3">Fogadott érdeklődések</h2>
      <div class="flex flex-wrap gap-2">
        {% for t in v.welcomed_topics %}
        {% set t_slug = topic_url_slugs.get(t, t.replace('_','-')) %}
        <a href="/{{ city_slug }}/{{ t_slug }}"
           class="inline-flex items-center gap-1.5 text-sm px-3 py-1.5 rounded-lg bg-[#FDF0EA] text-[#A8512F] border border-[#FAD9C7] hover:bg-[#FAD9C7] transition-colors">
          <i class="ph ph-{{ topic_icons.get(t, 'circle') }}" style="font-size:14px"></i>
          {{ topic_labels.get(t, t.replace('_',' ').title()) }}
        </a>
        {% endfor %}
      </div>
    </div>
    {% endif %}

    {% if communities %}
    <div class="bg-white rounded-xl border border-[#EAE5DB] p-5">
      <h2 class="text-xs font-semibold uppercase tracking-[0.08em] text-[#8C8478] mb-3 flex items-center gap-2">
        <i class="ph ph-users-three text-[#C2613A]"></i>
        Közösségek itt
        <span class="font-normal text-[#B5ADA0]">{{ communities | length }}</span>
      </h2>
      <div class="flex flex-col gap-2">
        {% for c in communities %}
        {% set c_city_sl = c.city | slugify %}
        {% set c_name_sl = c.name | slugify %}
        <a href="/{{ c_city_sl }}/{{ c_name_sl }}"
           class="flex items-center gap-3 px-3 py-2.5 rounded-lg border border-[#EAE5DB] hover:border-[#E88E6B] hover:bg-[#FDF0EA]/30 transition-colors">
          <i class="ph ph-{{ topic_icons.get(c.topic, 'circle') }} text-[#C2613A] shrink-0" style="font-size:16px"></i>
          <div class="min-w-0">
            <p class="text-sm font-medium text-[#1C1917] truncate">{{ c.name }}</p>
            <p class="text-xs text-[#8C8478]">{{ topic_labels.get(c.topic, c.topic) }}</p>
          </div>
        </a>
        {% endfor %}
      </div>
    </div>
    {% endif %}

  </div>

  <!-- Sidebar: contact details -->
  <div class="flex flex-col gap-3">

    <div class="bg-white rounded-xl border border-[#EAE5DB] p-5 flex flex-col gap-3">
      <h2 class="text-xs font-semibold uppercase tracking-[0.08em] text-[#8C8478]">Elérhetőség</h2>

      {% if v.address %}
      <div class="flex items-start gap-2 text-sm text-[#4A4441]">
        <i class="ph ph-map-pin text-[#B5ADA0] mt-0.5 shrink-0"></i>
        <span>{{ v.address }}</span>
      </div>
      {% endif %}

      {% if v.website %}
      <a href="{{ v.website }}" target="_blank" rel="noopener"
         class="flex items-center gap-2 text-sm text-[#A8512F] hover:text-[#8A4226] hover:underline truncate">
        <i class="ph ph-arrow-square-out text-[#B5ADA0] shrink-0"></i>
        {{ v.website | replace('https://','') | replace('http://','') | truncate(36) }}
      </a>
      {% endif %}

      {% if v.email %}
      <a href="mailto:{{ v.email }}"
         class="flex items-center gap-2 text-sm text-[#A8512F] hover:underline truncate">
        <i class="ph ph-envelope text-[#B5ADA0] shrink-0"></i>
        {{ v.email | truncate(36) }}
      </a>
      {% endif %}

      {% if v.phone %}
      <div class="flex items-center gap-2 text-sm text-[#4A4441]">
        <i class="ph ph-phone text-[#B5ADA0] shrink-0"></i>
        {{ v.phone }}
      </div>
      {% endif %}

      {% if v.contact %}
      <div class="flex items-start gap-2 text-sm text-[#4A4441]">
        <i class="ph ph-user text-[#B5ADA0] mt-0.5 shrink-0"></i>
        <span>{{ v.contact }}</span>
      </div>
      {% endif %}

      {% if v.social_links %}
      <div class="flex flex-col gap-1.5 pt-1 border-t border-[#EAE5DB]">
        {% for link in v.social_links %}
        <a href="{{ link }}" target="_blank" rel="noopener"
           class="flex items-center gap-2 text-xs text-[#A8512F] hover:underline truncate">
          <i class="ph ph-link text-[#B5ADA0] shrink-0"></i>
          {{ link | replace('https://','') | replace('http://','') | truncate(36) }}
        </a>
        {% endfor %}
      </div>
      {% endif %}

    </div>

    <a href="/{{ city_slug }}"
       class="flex items-center gap-2 text-sm text-[#8C8478] hover:text-[#A8512F] transition-colors px-1">
      <i class="ph ph-buildings"></i> Vissza: {{ city }}
    </a>

  </div>

</div>

{% endblock %}
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/test_venue_person_pages.py -v
```

Expected: `test_venue_detail_page_returns_200` PASS, `test_venue_detail_page_404_redirects` PASS

- [ ] **Step 6: Commit**

```bash
git add scraper/web/app.py scraper/web/templates/public_venue_detail.html tests/test_venue_person_pages.py
git commit -m "feat: venue detail page at /{city}/helyszin/{slug}"
```

---

### Task 3: Person detail route and template

**Files:**
- Create: `scraper/web/templates/public_person_detail.html`
- Modify: `scraper/web/app.py` — insert route before `/{city_slug}/{segment}`
- Test: `tests/test_venue_person_pages.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_venue_person_pages.py`:

```python
def test_person_detail_page_returns_200(tmp_path):
    db = _db(tmp_path)
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/budapest/ember/kovacs-janos")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "Futók" in resp.text
    finally:
        app_state.db_path = old_db


def test_person_detail_merges_multiple_communities(tmp_path):
    db = _db(tmp_path)
    for community in ["Futók", "Kerékpárosok"]:
        p = PersonRecord(
            name="Kovács János", role="leader", city="Budapest", topic="running",
            community_name=community, source_url="https://a.test",
            extracted_at="2026-01-01T00:00:00+00:00",
        )
        upsert_persons(db, [p.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/budapest/ember/kovacs-janos")
        assert resp.status_code == 200
        assert "Futók" in resp.text
        assert "Kerékpárosok" in resp.text
    finally:
        app_state.db_path = old_db


def test_person_detail_404_redirects(tmp_path):
    db = _db(tmp_path)
    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get(
            "/budapest/ember/nem-letezik", follow_redirects=False
        )
        assert resp.status_code == 302
        assert resp.headers["location"] == "/emberek"
    finally:
        app_state.db_path = old_db
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_venue_person_pages.py::test_person_detail_page_returns_200 -v
```

Expected: 302 → /emberek doesn't exist yet or 404

- [ ] **Step 3: Add the route to app.py**

Insert immediately after the venue detail route (before `/{city_slug}/{segment}`):

```python
@_fastapi.get("/{city_slug}/ember/{name_slug}", response_class=HTMLResponse)
async def public_person_detail(request: Request, city_slug: str, name_slug: str):
    city_name = _city_from_slug(city_slug)
    if not city_name or not app_state.db_path:
        return RedirectResponse("/emberek", status_code=302)
    all_persons = get_persons(app_state.db_path, city_name)
    merged = [p for p in all_persons if _slugify(p.get("name", "")) == name_slug]
    if not merged:
        return RedirectResponse("/emberek", status_code=302)
    # Build per-community entries with links
    city_locale = _city_locale(city_name)
    topic_url_slugs = {t.name: _topic_url_slug(t.name, city_locale) for t in (app_state.topics or [])}
    community_entries = []
    seen = set()
    for p in merged:
        key = (p.get("community_name", ""), p.get("topic", ""), p.get("role", ""))
        if key in seen:
            continue
        seen.add(key)
        community_name = p.get("community_name", "")
        topic = p.get("topic", "")
        community_entries.append({
            "name": community_name,
            "url": f"/{city_slug}/{_slugify(community_name)}",
            "role": p.get("role", ""),
            "topic": topic,
            "topic_label": TOPIC_LABELS.get(topic, topic.replace("_", " ").title()),
            "topic_icon": TOPIC_ICONS.get(topic, "circle"),
        })
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
```

- [ ] **Step 4: Create `scraper/web/templates/public_person_detail.html`**

```html
{% extends "public_base.html" %}
{% block title %}{{ person.name }} – közösségek.com{% endblock %}
{% block og_desc %}{{ bio or (person.name + ' közösségi ' + person.role + ' ' + city + 'ban.') }}{% endblock %}
{% block content %}

<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">
      <a href="/{{ city_slug }}" class="hover:text-white transition-colors">{{ city }}</a>
      <span class="mx-1 opacity-60">›</span>
      <a href="/emberek" class="hover:text-white transition-colors">Emberek</a>
    </p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">{{ person.name }}</h1>
    <p class="text-sm text-white/75 mt-1">{{ city }}</p>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-6 grid grid-cols-1 lg:grid-cols-3 gap-6">

  <!-- Main: communities -->
  <div class="lg:col-span-2 flex flex-col gap-4">

    {% if bio %}
    <div class="bg-white rounded-xl border border-[#EAE5DB] p-5">
      <p class="text-sm text-[#6A6259] leading-relaxed">{{ bio }}</p>
    </div>
    {% endif %}

    <div class="bg-white rounded-xl border border-[#EAE5DB] p-5">
      <h2 class="text-xs font-semibold uppercase tracking-[0.08em] text-[#8C8478] mb-3 flex items-center gap-2">
        <i class="ph ph-users-three text-[#C2613A]"></i>
        Közösségek
        <span class="font-normal text-[#B5ADA0]">{{ community_entries | length }}</span>
      </h2>
      <div class="flex flex-col gap-2">
        {% for ce in community_entries %}
        <a href="{{ ce.url }}"
           class="flex items-center gap-3 px-3 py-2.5 rounded-lg border border-[#EAE5DB] hover:border-[#E88E6B] hover:bg-[#FDF0EA]/30 transition-colors">
          <i class="ph ph-{{ ce.topic_icon }} text-[#C2613A] shrink-0" style="font-size:16px"></i>
          <div class="min-w-0 flex-1">
            <p class="text-sm font-medium text-[#1C1917] truncate">{{ ce.name }}</p>
            <p class="text-xs text-[#8C8478]">{{ ce.topic_label }}</p>
          </div>
          {% if ce.role %}
          <span class="shrink-0 text-[10px] font-medium px-1.5 py-0.5 rounded bg-[#F5F2EC] text-[#8C8478] border border-[#EAE5DB]">
            {{ ce.role }}
          </span>
          {% endif %}
        </a>
        {% endfor %}
      </div>
    </div>

  </div>

  <!-- Sidebar -->
  <div class="flex flex-col gap-3">

    {% if website or social_links %}
    <div class="bg-white rounded-xl border border-[#EAE5DB] p-5 flex flex-col gap-3">
      <h2 class="text-xs font-semibold uppercase tracking-[0.08em] text-[#8C8478]">Elérhetőség</h2>

      {% if website %}
      <a href="{{ website }}" target="_blank" rel="noopener"
         class="flex items-center gap-2 text-sm text-[#A8512F] hover:underline truncate">
        <i class="ph ph-arrow-square-out text-[#B5ADA0] shrink-0"></i>
        {{ website | replace('https://','') | replace('http://','') | truncate(36) }}
      </a>
      {% endif %}

      {% for link in social_links %}
      <a href="{{ link }}" target="_blank" rel="noopener"
         class="flex items-center gap-2 text-xs text-[#A8512F] hover:underline truncate">
        <i class="ph ph-link text-[#B5ADA0] shrink-0"></i>
        {{ link | replace('https://','') | replace('http://','') | truncate(36) }}
      </a>
      {% endfor %}
    </div>
    {% endif %}

    <a href="/{{ city_slug }}"
       class="flex items-center gap-2 text-sm text-[#8C8478] hover:text-[#A8512F] transition-colors px-1">
      <i class="ph ph-buildings"></i> Vissza: {{ city }}
    </a>

  </div>

</div>

{% endblock %}
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/test_venue_person_pages.py -v
```

Expected: all 6 tests PASS

- [ ] **Step 6: Commit**

```bash
git add scraper/web/app.py scraper/web/templates/public_person_detail.html tests/test_venue_person_pages.py
git commit -m "feat: person detail page at /{city}/ember/{slug}, merged by name+city"
```

---

### Task 4: Link venue cards in list pages

**Files:**
- Modify: `scraper/web/templates/public_venues.html`
- Modify: `scraper/web/templates/public_explore.html`
- Test: `tests/test_venue_person_pages.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_venue_person_pages.py`:

```python
def test_venues_list_contains_detail_links(tmp_path):
    db = _db(tmp_path)
    v = _venue(name="Müpa Budapest", city="Budapest")
    upsert_venues(db, [v.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/helyszinek")
        assert resp.status_code == 200
        assert "/budapest/helyszin/mupa-budapest" in resp.text
    finally:
        app_state.db_path = old_db
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_venue_person_pages.py::test_venues_list_contains_detail_links -v
```

Expected: FAIL — link not in page

- [ ] **Step 3: Update `public_venues.html` venue card macro**

In `scraper/web/templates/public_venues.html`, the `_venue_card` macro starts at line 6. Wrap the `<h2>` at line 10 in an `<a>` tag:

Replace:
```html
  <div class="flex items-start justify-between gap-2">
    <h2 class="font-semibold text-[#1C1917] text-base leading-snug">{{ v.name }}</h2>
```

With:
```html
  <div class="flex items-start justify-between gap-2">
    <h2 class="font-semibold text-[#1C1917] text-base leading-snug">
      <a href="/{{ v.city | slugify }}/helyszin/{{ v.name | slugify }}"
         class="hover:text-[#A8512F] transition-colors">{{ v.name }}</a>
    </h2>
```

- [ ] **Step 4: Update `public_explore.html` venue card**

In `scraper/web/templates/public_explore.html`, the venue card name is at line 283. Wrap it:

Replace:
```html
          <h3 class="font-semibold text-[#1C1917] text-sm leading-snug">{{ v.name }}</h3>
```

With:
```html
          <h3 class="font-semibold text-[#1C1917] text-sm leading-snug">
            <a href="/{{ v.city | slugify }}/helyszin/{{ v.name | slugify }}"
               class="hover:text-[#A8512F] transition-colors">{{ v.name }}</a>
          </h3>
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/test_venue_person_pages.py -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add scraper/web/templates/public_venues.html scraper/web/templates/public_explore.html tests/test_venue_person_pages.py
git commit -m "feat: link venue cards to detail pages"
```

---

### Task 5: Link leader from community page + populate /emberek list

**Files:**
- Modify: `scraper/web/templates/public_community.html`
- Modify: `scraper/web/templates/public_people.html`
- Modify: `scraper/web/app.py` — update `/emberek` route to pass grouped persons
- Test: `tests/test_venue_person_pages.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_venue_person_pages.py`:

```python
def test_community_page_links_leader(tmp_path):
    db = _db(tmp_path)
    from scraper.store import save_results as sr
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
        leader="Kovács János",
    )
    sr("Budapest", "running", [r], db)

    old_db = app_state.db_path
    old_topics = app_state.topics
    try:
        app_state.db_path = db
        app_state.topics = []
        resp = TestClient(web_app.app).get("/budapest/budapest-futok")
        assert resp.status_code == 200
        assert "/budapest/ember/kovacs-janos" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.topics = old_topics


def test_emberek_page_lists_persons(tmp_path):
    db = _db(tmp_path)
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    old_db = app_state.db_path
    try:
        app_state.db_path = db
        resp = TestClient(web_app.app).get("/emberek")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "/budapest/ember/kovacs-janos" in resp.text
    finally:
        app_state.db_path = old_db
```

- [ ] **Step 2: Run to verify failure**

```bash
pytest tests/test_venue_person_pages.py::test_community_page_links_leader tests/test_venue_person_pages.py::test_emberek_page_lists_persons -v
```

Expected: both FAIL

- [ ] **Step 3: Link leader in `public_community.html`**

In `scraper/web/templates/public_community.html`, find the leader detail row. Look for the line:
```
{{ detail_row('person-simple', 'Vezető',     r.leader) }}
```
(around line 149)

The `detail_row` macro renders the value as plain text. Replace just the leader row with an explicit link. First find the `detail_row` macro definition to understand its signature, then add a conditional override just for the leader:

Replace:
```html
        {{ detail_row('person-simple', 'Vezető',     r.leader) }}
```

With:
```html
        {% if r.leader %}
        <div class="flex items-start gap-3 text-sm">
          <div class="w-7 h-7 rounded-md bg-[#F5F2EC] border border-[#EAE5DB] flex items-center justify-center shrink-0 mt-0.5">
            <i class="ph ph-person-simple text-[#8C8478]" style="font-size:13px"></i>
          </div>
          <div class="min-w-0">
            <div class="text-[10px] font-semibold text-[#B5ADA0] uppercase tracking-[0.08em] mb-0.5">Vezető</div>
            <div class="text-[#4A4441]">
              <a href="/{{ city | slugify }}/ember/{{ r.leader | slugify }}"
                 class="hover:text-[#A8512F] transition-colors">{{ r.leader }}</a>
            </div>
          </div>
        </div>
        {% endif %}
```

- [ ] **Step 4: Update `/emberek` route in app.py**

Find the `/emberek` route (~line 2969). Replace it:

```python
@_fastapi.get("/emberek", response_class=HTMLResponse)
async def public_people(request: Request):
    if not app_state.db_path:
        return templates.TemplateResponse(request, "public_people.html", {
            "city_groups": [], "total_persons": 0, **lang_context(request),
        })
    init_db(app_state.db_path)
    hu_names = _hu_city_names()
    all_persons = get_all_persons(app_state.db_path)
    hu_persons = [p for p in all_persons if p.get("city", "") in hu_names]

    # Deduplicate by name+city slug (merged person identity)
    seen: dict[tuple, dict] = {}
    for p in hu_persons:
        key = (_slugify(p.get("name", "")), _slugify(p.get("city", "")))
        if key not in seen:
            seen[key] = p
    unique = list(seen.values())

    # Group by city
    from collections import defaultdict
    city_map: dict = defaultdict(list)
    for p in unique:
        city_map[p.get("city") or "—"].append(p)
    city_groups = [
        {"name": city, "persons": sorted(persons, key=lambda x: x.get("name", ""))}
        for city, persons in sorted(city_map.items())
    ]
    total = sum(len(g["persons"]) for g in city_groups)
    return templates.TemplateResponse(request, "public_people.html", {
        "city_groups": city_groups,
        "total_persons": total,
        **lang_context(request),
    })
```

Also add `get_all_persons` to the db imports in app.py (add after `get_person_history`):

```python
    get_all_persons,
```

- [ ] **Step 5: Update `public_people.html` to show person list**

Replace the entire `{% block content %}` body with:

```html
{% block content %}

<div class="brand-gradient relative overflow-hidden">
  <div class="absolute -top-6 -right-6 w-28 h-28 rounded-full bg-white opacity-[0.07] pointer-events-none"></div>
  <div class="max-w-6xl mx-auto px-4 sm:px-6 py-5 sm:py-7 relative">
    <p class="text-[10px] font-semibold uppercase tracking-[0.1em] text-white/70 mb-1.5">Közösségi emberek</p>
    <h1 class="text-xl sm:text-2xl font-black tracking-[-0.025em] text-white leading-tight">Közösségi vezetők és oktatók</h1>
    <p class="text-sm text-white/75 mt-1">{{ total_persons }} személy</p>
  </div>
</div>

<div class="max-w-6xl mx-auto px-4 sm:px-6 py-6">

  {% if city_groups %}
  {% for group in city_groups %}
  <div class="mb-8">
    <h2 class="text-xs font-bold uppercase tracking-[0.1em] text-[#8C8478] mb-4 flex items-center gap-2">
      <a href="/{{ group.name | slugify }}" class="hover:text-[#A8512F] transition-colors">{{ group.name }}</a>
      <span class="font-normal text-[#B5ADA0]">{{ group.persons | length }}</span>
    </h2>
    <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
      {% for p in group.persons %}
      <a href="/{{ p.city | slugify }}/ember/{{ p.name | slugify }}"
         class="bg-white rounded-xl border border-[#EAE5DB] p-4 flex flex-col gap-1.5 hover:border-[#E88E6B] hover:bg-[#FDF0EA]/20 transition-colors">
        <div class="flex items-center gap-2">
          <i class="ph ph-user-circle text-[#C2613A]" style="font-size:20px"></i>
          <span class="font-semibold text-[#1C1917] text-sm leading-snug">{{ p.name }}</span>
        </div>
        {% if p.community_name %}
        <p class="text-xs text-[#8C8478] truncate">{{ p.community_name }}</p>
        {% endif %}
        {% if p.role %}
        <span class="text-[10px] font-medium px-1.5 py-0.5 rounded bg-[#F5F2EC] text-[#8C8478] border border-[#EAE5DB] self-start">{{ p.role }}</span>
        {% endif %}
      </a>
      {% endfor %}
    </div>
  </div>
  {% endfor %}
  {% else %}
  <div class="text-center py-20 text-[#B5ADA0]">
    <i class="ph ph-users text-5xl mb-4 block"></i>
    <p class="text-base">Még nincsenek személyek az adatbázisban.</p>
    <a href="/" class="mt-3 inline-block text-sm text-[#A8512F] hover:underline">Közösségek felfedezése →</a>
  </div>
  {% endif %}

</div>

{% endblock %}
```

- [ ] **Step 6: Run all tests**

```bash
pytest tests/test_venue_person_pages.py -v
pytest tests/ -v --tb=short
```

Expected: all 10 new tests PASS; no regressions in other tests

- [ ] **Step 7: Commit**

```bash
git add scraper/web/app.py scraper/web/templates/public_community.html scraper/web/templates/public_people.html tests/test_venue_person_pages.py
git commit -m "feat: link leader to person page, populate /emberek list"
```
