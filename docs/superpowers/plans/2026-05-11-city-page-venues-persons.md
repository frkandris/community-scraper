# City Page Venues + Persons — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show all venues and all (deduplicated) persons for the current city on the `/{city_slug}` page, each item linking to its detail page.

**Architecture:** 2 tasks — (1) pass `city_venues` and `city_persons` from `_render_explore` (only when `city` is set and no topic filter); (2) add the two sections to `public_explore.html` between the community grid and the subscribe box.

**Tech Stack:** FastAPI, Jinja2, SQLite (existing `get_venues` and `get_persons` DB functions).

---

### Task 1: Pass `city_venues` and `city_persons` from `_render_explore`

**Files:**
- Modify: `scraper/web/app.py` — `_render_explore` function at line 818

**Key facts about `_render_explore` (read these before editing):**
- Defined at line 818 in `scraper/web/app.py`.
- The `topic_venues` variable is computed at line 960: `topic_venues = get_venues_by_city_topic(...) if city and len(topic)==1`.
- The `return templates.TemplateResponse(...)` block starts at line 967.
- `get_venues(db_path, city)` — already imported — returns all venues for a city.
- `get_persons(db_path, city, topic=None)` — already imported — returns all person rows for a city (one row per community membership, so same person can appear N times).

- [ ] **Step 1: Add `city_venues` and `city_persons` to `_render_explore`**

In `scraper/web/app.py`, read lines 958–986 to find the exact text of the `topic_venues` computation and the `return` call.

Find this block (around line 960):

```python
    topic_venues: list[dict] = []
    if city and len(topic) == 1 and app_state.db_path:
        topic_venues = get_venues_by_city_topic(app_state.db_path, city, topic[0])

    city_locale = _city_locale(city) if city else "en"
```

Replace with:

```python
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
```

- [ ] **Step 2: Add `city_venues` and `city_persons` to the template kwargs**

In the same function, find the `return templates.TemplateResponse(...)` block. Find the line `"topic_venues": topic_venues,` and add the two new keys after it:

```python
        "topic_venues": topic_venues,
        "city_venues": city_venues,
        "city_persons": city_persons,
```

- [ ] **Step 3: Commit**

```bash
git add scraper/web/app.py
git commit -m "feat: pass city_venues and city_persons to city page template context"
```

---

### Task 2: Add venues and persons sections to `public_explore.html`

**Files:**
- Modify: `scraper/web/templates/public_explore.html` — insert two sections after line 313 (after `{% endif %}` that closes `{% if topic_venues %}`) and before `<!-- Subscribe section -->`
- Create: `tests/test_city_page.py`

**Key facts about the template:**
- The `{% if topic_venues %}` block ends at line 313 with `{% endif %}`.
- The subscribe section starts at line 315 with `<!-- Subscribe section -->`.
- `city_venues` and `city_persons` are only non-empty when on a city page with no topic filter (so no `{% if not selected_topics %}` guard needed in the template; the list is already empty otherwise).
- The `city | slugify` and `v.name | slugify` / `p.name | slugify` Jinja2 filters are available.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_city_page.py`:

```python
from pathlib import Path
from scraper.db import init_db, upsert_venues, upsert_persons
from scraper.models import VenueRecord, PersonRecord
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_city_page_shows_venue_link(tmp_path):
    db = _db(tmp_path)
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"],
    )
    upsert_venues(db, [v.model_dump()])

    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/budapest")
        assert resp.status_code == 200
        assert "/budapest/helyszin/mupa-budapest" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_city_page_shows_person_link(tmp_path):
    db = _db(tmp_path)
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/budapest")
        assert resp.status_code == 200
        assert "/budapest/ember/kovacs-janos" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities


def test_city_page_deduplicates_persons(tmp_path):
    db = _db(tmp_path)
    for community in ["Futók", "Kerékpárosok"]:
        p = PersonRecord(
            name="Kovács János", role="leader", city="Budapest", topic="running",
            community_name=community, source_url="https://a.test",
            extracted_at="2026-01-01T00:00:00+00:00",
        )
        upsert_persons(db, [p.model_dump()])

    old_db, old_cities = app_state.db_path, app_state.cities
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        resp = TestClient(web_app.app).get("/budapest")
        assert resp.status_code == 200
        # Person appears once — count occurrences of the detail link
        assert resp.text.count("/budapest/ember/kovacs-janos") == 1
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
```

- [ ] **Step 2: Run to confirm failure**

```bash
venv/bin/pytest tests/test_city_page.py -q
```

Expected: 3 failures — template doesn't render the links yet.

- [ ] **Step 3: Read the template section**

Read `scraper/web/templates/public_explore.html` lines 310–320 to get the exact surrounding text.

- [ ] **Step 4: Insert the venues section**

Find the block:
```html
  {% endif %}

  <!-- Subscribe section -->
```

(the `{% endif %}` that closes `{% if topic_venues %}`, followed by the subscribe comment)

Replace it with:

```html
  {% endif %}

  <!-- City venues section (city page, no topic filter) -->
  {% if city_venues %}
  <div class="mt-8 pt-6 border-t border-[#EAE5DB]">
    <h2 class="text-base font-semibold text-[#1C1917] mb-4 flex items-center gap-2">
      <i class="ph ph-map-pin text-[#C2613A]"></i>
      Helyszínek
      <span class="text-xs text-[#8C8478] font-normal">{{ city_venues | length }}</span>
      <a href="/helyszinek" class="ml-auto text-xs text-[#A8512F] hover:underline font-normal">Összes →</a>
    </h2>
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
      {% for v in city_venues %}
      <div class="bg-white rounded-lg border border-[#EAE5DB] p-4 flex flex-col gap-2">
        <div class="flex items-start justify-between gap-2">
          <h3 class="font-semibold text-[#1C1917] text-sm leading-snug">
            <a href="/{{ city | slugify }}/helyszin/{{ v.name | slugify }}"
               class="hover:text-[#A8512F] transition-colors">{{ v.name }}</a>
          </h3>
          {% if v.venue_type %}
          <span class="shrink-0 text-[10px] font-medium px-1.5 py-0.5 rounded bg-[#F5F2EC] text-[#8C8478] border border-[#EAE5DB] whitespace-nowrap">
            {{ v.venue_type.replace('_',' ') }}
          </span>
          {% endif %}
        </div>
        {% if v.description %}
        <p class="text-xs text-[#6A6259] leading-relaxed line-clamp-3">{{ v.description }}</p>
        {% endif %}
        {% if v.address %}
        <div class="flex items-start gap-1.5 text-xs text-[#8C8478] mt-auto">
          <i class="ph ph-map-pin shrink-0 text-[#B5ADA0] mt-0.5"></i>
          <span>{{ v.address }}</span>
        </div>
        {% endif %}
      </div>
      {% endfor %}
    </div>
  </div>
  {% endif %}

  <!-- City persons section (city page, no topic filter) -->
  {% if city_persons %}
  <div class="mt-8 pt-6 border-t border-[#EAE5DB]">
    <h2 class="text-base font-semibold text-[#1C1917] mb-4 flex items-center gap-2">
      <i class="ph ph-person-simple text-[#C2613A]"></i>
      Emberek
      <span class="text-xs text-[#8C8478] font-normal">{{ city_persons | length }}</span>
      <a href="/emberek" class="ml-auto text-xs text-[#A8512F] hover:underline font-normal">Összes →</a>
    </h2>
    <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-2">
      {% for p in city_persons %}
      <a href="/{{ city | slugify }}/ember/{{ p.name | slugify }}"
         class="flex items-center gap-2 px-3 py-2 rounded-lg bg-white border border-[#EAE5DB] hover:border-[#A8512F] hover:shadow-sm transition-all group">
        <i class="ph ph-person-simple text-[#B5ADA0] group-hover:text-[#A8512F] transition-colors shrink-0"></i>
        <div class="min-w-0">
          <div class="text-sm font-medium text-[#1C1917] group-hover:text-[#A8512F] transition-colors truncate">{{ p.name }}</div>
          {% if p.role %}
          <div class="text-[10px] text-[#8C8478] truncate">{{ p.role }}</div>
          {% endif %}
        </div>
      </a>
      {% endfor %}
    </div>
  </div>
  {% endif %}

  <!-- Subscribe section -->
```

- [ ] **Step 5: Run the city page tests**

```bash
venv/bin/pytest tests/test_city_page.py -q
```

Expected: 3 passed.

- [ ] **Step 6: Run full test suite**

```bash
venv/bin/pytest tests/ -q
```

Expected: all tests pass (66 or more).

- [ ] **Step 7: Commit**

```bash
git add scraper/web/templates/public_explore.html tests/test_city_page.py
git commit -m "feat: city page venues and persons sections"
```
