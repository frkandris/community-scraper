# Listing Pages Filters — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add city + role server-side filters and a JS name search to `/emberek`; add a JS name search to `/helyszinek`; fix missing `role_hu` translation on the people listing.

**Architecture:** 2 tasks — (1) extend the `/emberek` route with `city`/`role` query params, rewrite `public_people.html` with filter form + `data-name` attributes + inline JS + `role_hu` fix; (2) add JS name search to `public_venues.html` (template-only, no route change).

**Tech Stack:** FastAPI query params, Jinja2, vanilla JS `input` event listener.

---

### Task 1: `/emberek` route filters + full template update

**Files:**
- Modify: `scraper/web/app.py` — `public_people` route at line 3038
- Modify: `scraper/web/templates/public_people.html`
- Create: `tests/test_listing_filters.py`

**Key facts:**
- `public_people` currently has no query params. FastAPI ignores unknown query params by default, so `?city=Budapest` returns 200 but no filtering — tests for filtering will still fail.
- `get_all_persons(db_path)` returns all person rows (one per community membership). Deduplication by `(_slugify(name), _slugify(city))` is already in the route — keep it. Build `all_cities`/`all_roles` from the full `unique` list before filtering so the dropdowns always show every option.
- `_ROLE_HU` and the `role_hu` filter are already registered in `app.py` (line ~333). The `public_people.html` template just doesn't use it yet.
- `_hu_city_names()` returns the set of Hungarian city names — only persons from these cities are shown.
- Person card is an `<a>` tag in the current template; `data-name` goes on it.

- [ ] **Step 1: Create `tests/test_listing_filters.py`**

```python
from pathlib import Path
from scraper.db import init_db, upsert_persons
from scraper.models import PersonRecord
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _cities():
    return [
        CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[]),
        CityConfig(name="Debrecen", country="Hungary", locale="hu", search_variants=[]),
    ]


def _setup_persons(db):
    upsert_persons(db, [
        PersonRecord(name="Kovács János", role="leader", city="Budapest", topic="running",
                     community_name="Futók", source_url="https://a.test",
                     extracted_at="2026-01-01T00:00:00+00:00").model_dump(),
        PersonRecord(name="Nagy Éva", role="organizer", city="Budapest", topic="cycling",
                     community_name="Kerékpárosok", source_url="https://a.test",
                     extracted_at="2026-01-01T00:00:00+00:00").model_dump(),
        PersonRecord(name="Szabó Péter", role="coach", city="Debrecen", topic="running",
                     community_name="Futók DE", source_url="https://a.test",
                     extracted_at="2026-01-01T00:00:00+00:00").model_dump(),
    ])


def test_people_city_filter(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek?city=Budapest")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "Szabó Péter" not in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_role_filter(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek?role=leader")
        assert resp.status_code == 200
        assert "Kovács János" in resp.text
        assert "Nagy Éva" not in resp.text
        assert "Szabó Péter" not in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_filter_options_from_full_set(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek?city=Budapest")
        # Debrecen must still appear as a filter option even when filtered out
        assert "Debrecen" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_page_search_structure(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek")
        assert "people-search" in resp.text
        assert "data-name=" in resp.text
        assert "data-city-section" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_people_role_displayed_in_hungarian(tmp_path):
    db = _db(tmp_path)
    _setup_persons(db)
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = _cities()
        resp = TestClient(web_app.app).get("/emberek")
        assert "vezető" in resp.text      # "leader" → "vezető"
        assert "szervező" in resp.text    # "organizer" → "szervező"
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
```

- [ ] **Step 2: Run to confirm failures**

```bash
venv/bin/pytest tests/test_listing_filters.py -q
```

Expected: 5 failures (no filtering, no data-name, roles still in English).

- [ ] **Step 3: Extend `public_people` route in `app.py`**

Find the entire `public_people` function (lines 3038–3070) and replace it with:

```python
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
```

- [ ] **Step 4: Rewrite `scraper/web/templates/public_people.html`**

Replace the entire file with:

```html
{% extends "public_base.html" %}
{% block title %}Emberek – közösségek.com{% endblock %}
{% block og_desc %}Aktív tagok és szervezők a magyarországi közösségekből.{% endblock %}
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

  <!-- Filters -->
  <div class="mb-6 flex flex-wrap gap-3 items-end">
    <form method="GET" action="/emberek" class="flex flex-wrap gap-2 items-end">
      <div class="flex flex-col gap-1">
        <label class="text-[10px] font-semibold text-[#8C8478] uppercase tracking-[0.08em]">Város</label>
        <select name="city" onchange="this.form.submit()"
          class="text-sm border border-[#D8D1C4] rounded-lg px-3 py-2 bg-white text-[#1C1917]
                 focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20">
          <option value="">Összes város</option>
          {% for c in all_cities %}
          <option value="{{ c }}" {% if c == selected_city %}selected{% endif %}>{{ c }}</option>
          {% endfor %}
        </select>
      </div>
      <div class="flex flex-col gap-1">
        <label class="text-[10px] font-semibold text-[#8C8478] uppercase tracking-[0.08em]">Szerepkör</label>
        <select name="role" onchange="this.form.submit()"
          class="text-sm border border-[#D8D1C4] rounded-lg px-3 py-2 bg-white text-[#1C1917]
                 focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20">
          <option value="">Összes szerepkör</option>
          {% for r in all_roles %}
          <option value="{{ r }}" {% if r == selected_role %}selected{% endif %}>{{ r | role_hu }}</option>
          {% endfor %}
        </select>
      </div>
      {% if selected_city or selected_role %}
      <a href="/emberek" class="text-xs text-[#A8512F] hover:text-[#8A4226] self-end pb-2.5 transition-colors">
        Szűrők törlése
      </a>
      {% endif %}
    </form>
    <div class="flex flex-col gap-1">
      <label class="text-[10px] font-semibold text-[#8C8478] uppercase tracking-[0.08em]">Keresés</label>
      <input id="people-search" type="search" placeholder="Névre szűrés…"
        class="text-sm border border-[#D8D1C4] rounded-lg px-3 py-2 bg-white text-[#1C1917]
               focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20 w-48">
    </div>
  </div>

  {% if city_groups %}
  {% for group in city_groups %}
  <section data-city-section class="mb-8">
    <h2 class="text-xs font-bold uppercase tracking-[0.1em] text-[#8C8478] mb-4 flex items-center gap-2">
      <a href="/{{ group.name | slugify }}" class="hover:text-[#A8512F] transition-colors">{{ group.name }}</a>
      <span class="font-normal text-[#B5ADA0]">{{ group.persons | length }}</span>
    </h2>
    <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
      {% for p in group.persons %}
      <a href="/{{ p.city | slugify }}/ember/{{ p.name | slugify }}"
         data-name="{{ p.name | lower }}"
         class="bg-white rounded-xl border border-[#EAE5DB] p-4 flex flex-col gap-1.5 hover:border-[#E88E6B] hover:bg-[#FDF0EA]/20 transition-colors">
        <div class="flex items-center gap-2">
          <i class="ph ph-user-circle text-[#C2613A]" style="font-size:20px"></i>
          <span class="font-semibold text-[#1C1917] text-sm leading-snug">{{ p.name }}</span>
        </div>
        {% if p.community_name %}
        <p class="text-xs text-[#8C8478] truncate">{{ p.community_name }}</p>
        {% endif %}
        {% if p.role %}
        <span class="text-[10px] font-medium px-1.5 py-0.5 rounded bg-[#F5F2EC] text-[#8C8478] border border-[#EAE5DB] self-start">{{ p.role | role_hu }}</span>
        {% endif %}
      </a>
      {% endfor %}
    </div>
  </section>
  {% endfor %}
  {% else %}
  <div class="text-center py-20 text-[#B5ADA0]">
    <i class="ph ph-users text-5xl mb-4 block"></i>
    <p class="text-base">Még nincsenek személyek az adatbázisban.</p>
    <a href="/" class="mt-3 inline-block text-sm text-[#A8512F] hover:underline">Közösségek felfedezése →</a>
  </div>
  {% endif %}

</div>

<script>
const ps = document.getElementById('people-search');
if (ps) {
  ps.addEventListener('input', () => {
    const q = ps.value.toLowerCase();
    document.querySelectorAll('[data-city-section]').forEach(section => {
      let visible = 0;
      section.querySelectorAll('[data-name]').forEach(card => {
        const show = card.dataset.name.includes(q);
        card.classList.toggle('hidden', !show);
        if (show) visible++;
      });
      section.classList.toggle('hidden', visible === 0);
    });
  });
}
</script>

{% endblock %}
```

- [ ] **Step 5: Run the new tests**

```bash
venv/bin/pytest tests/test_listing_filters.py -q
```

Expected: 5 passed.

- [ ] **Step 6: Run full suite**

```bash
venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add scraper/web/app.py scraper/web/templates/public_people.html tests/test_listing_filters.py
git commit -m "feat: city + role filters and name search on /emberek"
```

---

### Task 2: JS name search on `/helyszinek`

**Files:**
- Modify: `scraper/web/templates/public_venues.html`
- Modify: `tests/test_listing_filters.py` — append venue tests

**Key facts:**
- No route changes needed — `/helyszinek` already passes `city_sections` (grouped view) and `venues` (flat view when city filter is active).
- `data-name` goes on the root `<div>` of the `_venue_card` macro (line 7 of the template).
- Grouped view: venue cards are inside `<div class="mb-5">` blocks — change these to `<section data-city-section>`.
- Flat view (city filter active): cards are in a bare grid with no section wrapper — JS can still filter individual cards.
- The JS must handle both cases: filter `[data-name]` cards AND hide `[data-city-section]` sections when all their cards are hidden.
- The name search input goes outside the existing `<form>` tag (it's JS-only, not a query param), wrapped together in a flex div.

- [ ] **Step 1: Append venue tests to `tests/test_listing_filters.py`**

Add these two imports at the **top** of `tests/test_listing_filters.py` (alongside the existing imports from Task 1):

```python
from scraper.db import upsert_venues
from scraper.models import VenueRecord
```

Then append this test function at the **bottom** of the file:

```python
def test_venues_page_has_name_search_structure(tmp_path):
    db = _db(tmp_path)
    upsert_venues(db, [
        VenueRecord(name="Müpa Budapest", city="Budapest", locale="hu",
                    source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
                    welcomed_topics=["music"]).model_dump(),
    ])
    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/helyszinek")
        assert resp.status_code == 200
        assert "venue-search" in resp.text
        assert "data-name=" in resp.text
        assert "data-city-section" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
```

- [ ] **Step 2: Run to confirm failure**

```bash
venv/bin/pytest tests/test_listing_filters.py::test_venues_page_has_name_search_structure -q
```

Expected: FAIL — `venue-search` not in response.

- [ ] **Step 3: Edit `scraper/web/templates/public_venues.html`**

**Change 1** — Add `data-name` to the `_venue_card` macro root div (line 7). Find:

```html
<div class="bg-white rounded-xl border border-[#EAE5DB] p-5 flex flex-col gap-3 hover:border-[#E88E6B] hover:bg-[#FDF0EA]/20 transition-colors">
```

Replace with:

```html
<div data-name="{{ v.name | lower }}" class="bg-white rounded-xl border border-[#EAE5DB] p-5 flex flex-col gap-3 hover:border-[#E88E6B] hover:bg-[#FDF0EA]/20 transition-colors">
```

**Change 2** — Wrap the existing `<form>` in a flex container and add the search input. Find:

```html
  <form method="GET" action="/helyszinek" class="mb-6 flex flex-wrap gap-2 items-end">
```

Replace with:

```html
  <div class="mb-6 flex flex-wrap gap-3 items-end">
  <form method="GET" action="/helyszinek" class="flex flex-wrap gap-2 items-end">
```

Find the closing tag of that form (after `{% endif %}` for Szűrők törlése):

```html
  </form>

  {% if venues %}
```

Replace with:

```html
  </form>
  <div class="flex flex-col gap-1">
    <label class="text-[10px] font-semibold text-[#8C8478] uppercase tracking-[0.08em]">Keresés</label>
    <input id="venue-search" type="search" placeholder="Névre szűrés…"
      class="text-sm border border-[#D8D1C4] rounded-lg px-3 py-2 bg-white text-[#1C1917]
             focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20 w-48">
  </div>
  </div>

  {% if venues %}
```

**Change 3** — Wrap city group `<div class="mb-5">` in `<section data-city-section>`. Find:

```html
    {% for city_group in city_sections %}
    <div class="mb-5">
      <h3 class="text-sm font-semibold text-[#4A4441] mb-3 flex items-center gap-2">
        <a href="/{{ city_group.name | slugify }}" class="hover:text-[#A8512F] transition-colors">{{ city_group.name }}</a>
        <span class="text-xs font-normal text-[#B5ADA0]">{{ city_group.venues | length }}</span>
      </h3>
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {% for v in city_group.venues %}
        {{ _venue_card(v) }}
        {% endfor %}
      </div>
    </div>
    {% endfor %}
```

Replace with:

```html
    {% for city_group in city_sections %}
    <section data-city-section class="mb-5">
      <h3 class="text-sm font-semibold text-[#4A4441] mb-3 flex items-center gap-2">
        <a href="/{{ city_group.name | slugify }}" class="hover:text-[#A8512F] transition-colors">{{ city_group.name }}</a>
        <span class="text-xs font-normal text-[#B5ADA0]">{{ city_group.venues | length }}</span>
      </h3>
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {% for v in city_group.venues %}
        {{ _venue_card(v) }}
        {% endfor %}
      </div>
    </section>
    {% endfor %}
```

**Change 4** — Add `<script>` block before `{% endblock %}`. Find:

```html
{% endblock %}
```

Replace with:

```html
<script>
const vs = document.getElementById('venue-search');
if (vs) {
  vs.addEventListener('input', () => {
    const q = vs.value.toLowerCase();
    document.querySelectorAll('[data-name]').forEach(card => {
      card.classList.toggle('hidden', !card.dataset.name.includes(q));
    });
    document.querySelectorAll('[data-city-section]').forEach(section => {
      const allHidden = [...section.querySelectorAll('[data-name]')].every(c => c.classList.contains('hidden'));
      section.classList.toggle('hidden', allHidden);
    });
  });
}
</script>

{% endblock %}
```

- [ ] **Step 4: Run all listing filter tests**

```bash
venv/bin/pytest tests/test_listing_filters.py -q
```

Expected: all 6 tests pass.

- [ ] **Step 5: Run full suite**

```bash
venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add scraper/web/templates/public_venues.html tests/test_listing_filters.py
git commit -m "feat: JS name search on /helyszinek"
```
