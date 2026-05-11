# Community Page Venue + Persons Enrichment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a venue card and a persons section to the community detail page (`/{city_slug}/{community_slug}`), each linking to its respective detail page.

**Architecture:** 2 tasks — (1) add `get_venue_for_community` to `db.py` with a test; (2) import both it and the existing `get_persons_for_community` in `app.py`, compute them in `public_city_segment`, and add venue/persons cards to `public_community.html`.

**Tech Stack:** FastAPI, Jinja2, SQLite `json_each` query (same pattern already used in `search_communities_by_tag`).

---

### Task 1: `get_venue_for_community` DB function

**Files:**
- Modify: `scraper/db.py` — insert after `get_communities_for_venue` (after line 810)
- Create: `tests/test_community_enrichment.py`

**Key facts:**
- `get_communities_for_venue` ends at line 810 with `return [json.loads(r[0]) for r in rows]`.
- Venues store `community_ids` as a JSON array in their `data` column. `json_each` iterates it.
- The `json` module is already imported at the top of `db.py`.

- [ ] **Step 1: Write failing test**

Create `tests/test_community_enrichment.py`:

```python
from pathlib import Path
from scraper.db import init_db, upsert_venues, upsert_persons, get_venue_for_community, get_persons_for_community
from scraper.store import save_results
from scraper.models import VenueRecord, PersonRecord, CommunityRecord


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_get_venue_for_community_found(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    cid = r.community_id
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"], community_ids=[cid],
    )
    upsert_venues(db, [v.model_dump()])

    result = get_venue_for_community(db, cid, "Budapest")
    assert result is not None
    assert result["name"] == "Müpa Budapest"


def test_get_venue_for_community_not_found(tmp_path):
    db = _db(tmp_path)
    result = get_venue_for_community(db, "nonexistent-id", "Budapest")
    assert result is None


def test_get_venue_for_community_empty_id(tmp_path):
    db = _db(tmp_path)
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"],
    )
    upsert_venues(db, [v.model_dump()])
    result = get_venue_for_community(db, "", "Budapest")
    assert result is None
```

- [ ] **Step 2: Run to confirm failure**

```bash
venv/bin/pytest tests/test_community_enrichment.py -q
```

Expected: `ImportError` — `get_venue_for_community` not defined yet.

- [ ] **Step 3: Add `get_venue_for_community` to `scraper/db.py`**

Insert after the closing line of `get_communities_for_venue` (after `return [json.loads(r[0]) for r in rows]` on line 810, before `def get_topic_counts`):

```python
def get_venue_for_community(db_path: Path, community_id: str, city: str) -> dict | None:
    """Return the first venue in city whose community_ids list contains community_id."""
    if not db_path.exists() or not community_id:
        return None
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM venues WHERE city=? AND EXISTS ("
            "  SELECT 1 FROM json_each(json_extract(data,'$.community_ids')) WHERE value=?"
            ") LIMIT 1",
            (city, community_id),
        ).fetchone()
    return json.loads(row[0]) if row else None

```

- [ ] **Step 4: Run tests**

```bash
venv/bin/pytest tests/test_community_enrichment.py -q
```

Expected: 3 passed.

- [ ] **Step 5: Run full suite**

```bash
venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add scraper/db.py tests/test_community_enrichment.py
git commit -m "feat: get_venue_for_community DB function"
```

---

### Task 2: Route context, imports, and template sections

**Files:**
- Modify: `scraper/web/app.py` — import two new functions; add venue/persons lookup before the `public_community.html` response
- Modify: `scraper/web/templates/public_community.html` — add venue card and persons section after line 234
- Modify: `tests/test_community_enrichment.py` — add route tests

**Key facts:**
- The `from ..db import (...)` block is at lines 26–74 of `app.py`. Its last line before `)` is `    search_all,`.
- `get_persons_for_community(db_path, community_name, city)` is already in `db.py` (line 1335) but NOT yet imported in `app.py`.
- `get_venue_for_community(db_path, community_id, city)` was just added in Task 1.
- In `public_city_segment` (line 3171), the community detail branch starts at line 3188: `record = _find_community_by_slug(...)`. The `return templates.TemplateResponse(...)` for `public_community.html` is at line 3195.
- `public_community.html` line 234: `</div>` closes the main header card. Line 237: `<!-- Feedback & validity box -->` begins the feedback section. Insert new cards between these.

- [ ] **Step 1: Write failing route tests**

Add these imports to the TOP of `tests/test_community_enrichment.py` (alongside the existing imports):

```python
from scraper.pipeline import CityConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient
```

Then append these test functions to the bottom of the file:


def test_community_page_shows_venue_card(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    cid = r.community_id
    v = VenueRecord(
        name="Müpa Budapest", city="Budapest", locale="hu",
        source_url="https://mupa.hu", extracted_at="2026-01-01T00:00:00+00:00",
        welcomed_topics=["music"], community_ids=[cid],
    )
    upsert_venues(db, [v.model_dump()])

    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        app_state.topics = []
        resp = TestClient(web_app.app).get("/budapest/budapest-futok")
        assert resp.status_code == 200
        assert "/budapest/helyszin/mupa-budapest" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_community_page_shows_person(tmp_path):
    db = _db(tmp_path)
    r = CommunityRecord(
        name="Budapest Futók", topic="running", city="Budapest", locale="hu",
        source_url="https://a.test", extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    p = PersonRecord(
        name="Kovács János", role="leader", city="Budapest", topic="running",
        community_name="Budapest Futók", source_url="https://a.test",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    upsert_persons(db, [p.model_dump()])

    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        app_state.db_path = db
        app_state.cities = [CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[])]
        app_state.topics = []
        resp = TestClient(web_app.app).get("/budapest/budapest-futok")
        assert resp.status_code == 200
        assert "/budapest/ember/kovacs-janos" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics
```

- [ ] **Step 2: Run to confirm failure**

```bash
venv/bin/pytest tests/test_community_enrichment.py::test_community_page_shows_venue_card tests/test_community_enrichment.py::test_community_page_shows_person -q
```

Expected: 2 failures — route doesn't pass venue/persons to template yet.

- [ ] **Step 3: Add imports to `app.py`**

Find the line `    search_all,` near the end of the `from ..db import (...)` block and add after it:

```python
    get_venue_for_community,
    get_persons_for_community,
```

- [ ] **Step 4: Add venue and persons lookup in `public_city_segment`**

In `scraper/web/app.py`, find the community detail branch in `public_city_segment`. Find this block (around line 3190):

```python
    record = _find_community_by_slug(city_name, segment)
    if record:
        schema_json = records_to_jsonld([record])
        history = get_community_history(app_state.db_path, record.get("community_id", ""))
        rec_topic = record.get("topic", "")
        city_locale = _city_locale(city_name)
        topic_url_slugs = {t.name: _topic_url_slug(t.name, city_locale) for t in (app_state.topics or [])}
        return templates.TemplateResponse(request, "public_community.html", {
```

Replace with:

```python
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
```

- [ ] **Step 5: Add `community_venue` and `community_persons` to template kwargs**

In the same `return templates.TemplateResponse(...)` block for `public_community.html`, find `"record_key": _community_record_key(...)` and add the two new keys after it:

```python
            "record_key": _community_record_key(record["name"], city_name, rec_topic),
            "community_venue": community_venue,
            "community_persons": community_persons,
```

- [ ] **Step 6: Add venue card and persons section to `public_community.html`**

Read `scraper/web/templates/public_community.html` lines 232–240 to find the exact text between the main header card `</div>` and `<!-- Feedback & validity box -->`.

Find this exact text:

```html
  </div>

  <!-- Feedback & validity box -->
```

(The `</div>` that closes the main `bg-white rounded-xl` header card, followed by the feedback comment.)

Replace it with:

```html
  </div>

  <!-- Venue card -->
  {% if community_venue %}
  <div class="mt-4 bg-white rounded-xl border border-[#EAE5DB] p-5">
    <p class="text-[10px] font-semibold text-[#B5ADA0] uppercase tracking-[0.08em] mb-3">
      <i class="ph ph-map-pin mr-1"></i>Helyszín
    </p>
    <a href="/{{ city | slugify }}/helyszin/{{ community_venue.name | slugify }}"
       class="flex items-center gap-3 group">
      <div class="w-9 h-9 rounded-lg bg-[#F5F2EC] border border-[#EAE5DB] flex items-center justify-center shrink-0">
        <i class="ph ph-map-pin text-[#A8512F]" style="font-size:16px"></i>
      </div>
      <div class="min-w-0">
        <div class="font-semibold text-[#1C1917] group-hover:text-[#A8512F] transition-colors">{{ community_venue.name }}</div>
        {% if community_venue.address %}
        <div class="text-xs text-[#8C8478] truncate">{{ community_venue.address }}</div>
        {% endif %}
      </div>
      <i class="ph ph-arrow-right text-[#D8D1C4] group-hover:text-[#A8512F] transition-colors ml-auto shrink-0"></i>
    </a>
  </div>
  {% endif %}

  <!-- Community persons -->
  {% if community_persons %}
  <div class="mt-4 bg-white rounded-xl border border-[#EAE5DB] p-5">
    <p class="text-[10px] font-semibold text-[#B5ADA0] uppercase tracking-[0.08em] mb-3">
      <i class="ph ph-person-simple mr-1"></i>Emberek
    </p>
    <div class="space-y-2">
      {% for p in community_persons %}
      <a href="/{{ city | slugify }}/ember/{{ p.name | slugify }}"
         class="flex items-center gap-2.5 group py-1 -mx-1 px-1 rounded-lg hover:bg-[#F5F2EC] transition-colors">
        <i class="ph ph-person-simple text-[#B5ADA0] group-hover:text-[#A8512F] transition-colors shrink-0 text-base"></i>
        <div class="min-w-0 flex items-center gap-2">
          <span class="text-sm font-medium text-[#1C1917] group-hover:text-[#A8512F] transition-colors">{{ p.name }}</span>
          {% if p.role %}
          <span class="text-[10px] text-[#8C8478] bg-[#F5F2EC] border border-[#EAE5DB] px-1.5 py-0.5 rounded shrink-0">{{ p.role }}</span>
          {% endif %}
        </div>
      </a>
      {% endfor %}
    </div>
  </div>
  {% endif %}

  <!-- Feedback & validity box -->
```

- [ ] **Step 7: Run route tests**

```bash
venv/bin/pytest tests/test_community_enrichment.py -q
```

Expected: all 5 tests pass.

- [ ] **Step 8: Run full test suite**

```bash
venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: all tests pass (71 or more).

- [ ] **Step 9: Commit**

```bash
git add scraper/web/app.py scraper/web/templates/public_community.html tests/test_community_enrichment.py
git commit -m "feat: community page venue card and persons section"
```
