# Admin Stats Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `/admin/stats` page showing data quality sarokszámok (total communities, website/contact/description coverage, top 20 cities, topic breakdown).

**Architecture:** New `get_data_quality_stats()` DB function → simple `@admin.get("/stats")` route → server-rendered `stats.html` template. No JS, no async. Nav link added to `base.html`.

**Tech Stack:** Python, SQLite (json_extract), FastAPI, Jinja2, Tailwind CSS

**Spec:** `docs/superpowers/specs/2026-05-16-admin-stats-page.md`

---

### Task 1: Add `get_data_quality_stats()` to db.py

**Files:**
- Modify: `scraper/db.py` (append at end of file, line 2062)
- Test: `tests/test_stats.py` (new file)

- [ ] **Step 1: Write the failing test**

Create `tests/test_stats.py`:

```python
from pathlib import Path
import json

from scraper.db import init_db, bulk_upsert_communities, get_data_quality_stats


def test_get_data_quality_stats_empty_db(tmp_path: Path):
    db_path = tmp_path / "test.db"
    stats = get_data_quality_stats(db_path)
    assert stats["total"] == 0
    assert stats["visible"] == 0
    assert stats["city_rows"] == []
    assert stats["topic_counts"] == {}


def test_get_data_quality_stats(tmp_path: Path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    bulk_upsert_communities(db_path, [
        {
            "name": "Alpha Runners", "city": "Budapest", "topic": "futás",
            "website": "https://alpha.com", "contact": "",
            "description": "x" * 60,
        },
        {
            "name": "Beta Yoga", "city": "Budapest", "topic": "jóga",
            "website": "", "contact": "info@beta.com",
            "description": "rövid",
        },
        {
            "name": "Gamma Club", "city": "Debrecen", "topic": "futás",
            "website": "", "contact": "",
            "description": "",
        },
    ])

    stats = get_data_quality_stats(db_path)

    assert stats["total"] == 3
    assert stats["visible"] == 3
    assert stats["hidden"] == 0
    assert stats["cities"] == 2
    assert stats["topics"] == 2
    assert stats["has_website"] == 1
    assert stats["has_contact"] == 1
    assert stats["has_description"] == 1  # only Alpha (>50 chars)
    assert stats["has_any"] == 2          # Alpha (website) + Beta (contact)

    # city_rows sorted by count desc
    assert len(stats["city_rows"]) == 2
    assert stats["city_rows"][0]["city"] == "Budapest"
    assert stats["city_rows"][0]["cnt"] == 2
    assert stats["city_rows"][0]["w"] == 1   # Alpha has website
    assert stats["city_rows"][0]["c"] == 1   # Beta has contact
    assert stats["city_rows"][1]["city"] == "Debrecen"
    assert stats["city_rows"][1]["cnt"] == 1

    assert stats["topic_counts"]["futás"] == 2
    assert stats["topic_counts"]["jóga"] == 1


def test_get_data_quality_stats_hidden(tmp_path: Path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    bulk_upsert_communities(db_path, [
        {"name": "Visible", "city": "Budapest", "topic": "futás",
         "website": "https://v.com", "contact": "", "description": ""},
    ])
    # Hide it directly
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE communities SET hidden=1 WHERE city='Budapest'")
        conn.commit()

    stats = get_data_quality_stats(db_path)
    assert stats["total"] == 1
    assert stats["visible"] == 0
    assert stats["hidden"] == 1
    assert stats["has_website"] == 0   # hidden records excluded from quality counts
    assert stats["city_rows"] == []
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=. .venv/bin/pytest tests/test_stats.py -v
```

Expected: `ImportError` or `AttributeError: module 'scraper.db' has no attribute 'get_data_quality_stats'`

- [ ] **Step 3: Implement `get_data_quality_stats` in db.py**

Append at the end of `scraper/db.py` (after line 2062):

```python


def get_data_quality_stats(db_path: Path) -> dict:
    empty: dict = {
        "total": 0, "visible": 0, "hidden": 0,
        "cities": 0, "topics": 0,
        "has_website": 0, "has_contact": 0, "has_description": 0, "has_any": 0,
        "city_rows": [], "topic_counts": {},
    }
    if not db_path.exists():
        return empty
    with _connect(db_path) as conn:
        row = conn.execute("""
            SELECT
              COUNT(*) as total,
              SUM(CASE WHEN hidden=0 THEN 1 ELSE 0 END) as visible,
              SUM(CASE WHEN hidden=1 THEN 1 ELSE 0 END) as hidden,
              COUNT(DISTINCT CASE WHEN hidden=0 THEN city END) as cities,
              COUNT(DISTINCT CASE WHEN hidden=0 THEN topic END) as topics,
              SUM(CASE WHEN hidden=0
                   AND json_extract(data,'$.website') IS NOT NULL
                   AND json_extract(data,'$.website') != '' THEN 1 ELSE 0 END) as has_website,
              SUM(CASE WHEN hidden=0
                   AND json_extract(data,'$.contact') IS NOT NULL
                   AND json_extract(data,'$.contact') != '' THEN 1 ELSE 0 END) as has_contact,
              SUM(CASE WHEN hidden=0
                   AND length(COALESCE(json_extract(data,'$.description'),'')) > 50
                   THEN 1 ELSE 0 END) as has_description,
              SUM(CASE WHEN hidden=0 AND (
                   (json_extract(data,'$.website') IS NOT NULL AND json_extract(data,'$.website') != '')
                   OR
                   (json_extract(data,'$.contact') IS NOT NULL AND json_extract(data,'$.contact') != '')
                 ) THEN 1 ELSE 0 END) as has_any
            FROM communities
        """).fetchone()
        city_rows = conn.execute("""
            SELECT city, COUNT(*) as cnt,
              SUM(CASE WHEN json_extract(data,'$.website') IS NOT NULL
                       AND json_extract(data,'$.website') != '' THEN 1 ELSE 0 END) as w,
              SUM(CASE WHEN json_extract(data,'$.contact') IS NOT NULL
                       AND json_extract(data,'$.contact') != '' THEN 1 ELSE 0 END) as c
            FROM communities
            WHERE hidden=0
            GROUP BY city
            ORDER BY cnt DESC
            LIMIT 20
        """).fetchall()
    topic_counts = get_topic_counts(db_path)
    return {
        "total": row[0] or 0,
        "visible": row[1] or 0,
        "hidden": row[2] or 0,
        "cities": row[3] or 0,
        "topics": row[4] or 0,
        "has_website": row[5] or 0,
        "has_contact": row[6] or 0,
        "has_description": row[7] or 0,
        "has_any": row[8] or 0,
        "city_rows": [{"city": r[0], "cnt": r[1], "w": r[2] or 0, "c": r[3] or 0} for r in city_rows],
        "topic_counts": topic_counts,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. .venv/bin/pytest tests/test_stats.py -v
```

Expected: all 3 tests PASS

- [ ] **Step 5: Run full test suite**

```bash
PYTHONPATH=. .venv/bin/pytest --ignore=tests/test_city_page.py -v
```

Expected: all tests PASS (no regressions)

- [ ] **Step 6: Commit**

```bash
git add tests/test_stats.py scraper/db.py
git commit -m "feat: add get_data_quality_stats() to db.py"
```

---

### Task 2: Add route to app.py

**Files:**
- Modify: `scraper/web/app.py` — insert new route after `subscriptions_page` (around line 2466)

- [ ] **Step 1: Insert route after the subscriptions route**

In `scraper/web/app.py`, after the closing `}` of the `subscriptions_page` function (around line 2466), insert:

```python

@admin.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request):
    from ..db import get_data_quality_stats
    stats: dict = {}
    if app_state.db_path and app_state.db_path.exists():
        stats = get_data_quality_stats(app_state.db_path)
    return templates.TemplateResponse(request, "stats.html", {"stats": stats})
```

- [ ] **Step 2: Commit**

```bash
git add scraper/web/app.py
git commit -m "feat: add /admin/stats route"
```

---

### Task 3: Create stats.html template

**Files:**
- Create: `scraper/web/templates/stats.html`

- [ ] **Step 1: Create the template**

Create `scraper/web/templates/stats.html`:

```html
{% extends "base.html" %}
{% block title %}Stats – Community Scraper{% endblock %}
{% block content %}

<div class="flex items-center justify-between mb-6">
  <h1 class="text-2xl font-bold text-gray-900">Stats</h1>
</div>

{% set visible = stats.get('visible', 0) %}

<!-- Sarokszámok -->
<div class="mb-6">
  <h2 class="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Összesítő</h2>
  <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
    {% for label, value in [
      ('Összes közösség', stats.get('total', 0)),
      ('Látható', stats.get('visible', 0)),
      ('Városok', stats.get('cities', 0)),
      ('Topicok', stats.get('topics', 0)),
    ] %}
    <div class="bg-white rounded-xl shadow-sm p-5 border border-gray-100">
      <div class="text-2xl font-bold text-gray-900">{{ value }}</div>
      <div class="text-xs text-gray-500 mt-1">{{ label }}</div>
    </div>
    {% endfor %}
  </div>
</div>

<!-- Adatminőség -->
<div class="mb-6">
  <h2 class="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Adatminőség <span class="normal-case font-normal">(látható közösségek, {{ visible }} db)</span></h2>
  <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
    {% for label, key, color in [
      ('Van website', 'has_website', 'blue'),
      ('Van elérhetőség', 'has_contact', 'green'),
      ('Van leírás (>50 kar.)', 'has_description', 'purple'),
      ('Bármilyen elérhetőség', 'has_any', 'indigo'),
    ] %}
    {% set n = stats.get(key, 0) %}
    {% set pct = ((n / visible * 100) | round | int) if visible else 0 %}
    <div class="bg-white rounded-xl shadow-sm p-5 border border-gray-100">
      <div class="text-2xl font-bold text-gray-900">{{ pct }}%</div>
      <div class="text-xs text-gray-500 mt-1">{{ label }}</div>
      <div class="text-xs text-gray-400 mt-0.5">{{ n }} / {{ visible }}</div>
      <div class="mt-3 h-1.5 rounded-full bg-gray-100 overflow-hidden">
        <div class="h-full rounded-full bg-{{ color }}-400" style="width: {{ pct }}%"></div>
      </div>
    </div>
    {% endfor %}
  </div>
</div>

<!-- Top városok -->
{% if stats.get('city_rows') %}
<div class="mb-6">
  <h2 class="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Top városok (top 20)</h2>
  <div class="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
    <table class="min-w-full text-sm">
      <thead>
        <tr class="border-b border-gray-100">
          <th class="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide">Város</th>
          <th class="px-4 py-3 text-right text-xs font-semibold text-gray-500 uppercase tracking-wide">Közösségek</th>
          <th class="px-4 py-3 text-right text-xs font-semibold text-gray-500 uppercase tracking-wide">Website %</th>
          <th class="px-4 py-3 text-right text-xs font-semibold text-gray-500 uppercase tracking-wide">Elérhetőség %</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-gray-100">
        {% for row in stats['city_rows'] %}
        {% set wpct = ((row.w / row.cnt * 100) | round | int) if row.cnt else 0 %}
        {% set cpct = ((row.c / row.cnt * 100) | round | int) if row.cnt else 0 %}
        <tr class="hover:bg-gray-50">
          <td class="px-4 py-2.5 font-medium text-gray-900">{{ row.city }}</td>
          <td class="px-4 py-2.5 text-right text-gray-700">{{ row.cnt }}</td>
          <td class="px-4 py-2.5 text-right text-gray-700">{{ wpct }}%</td>
          <td class="px-4 py-2.5 text-right text-gray-700">{{ cpct }}%</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

<!-- Topic megoszlás -->
{% if stats.get('topic_counts') %}
<div class="mb-6">
  <h2 class="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Topic megoszlás</h2>
  <div class="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
    <table class="min-w-full text-sm">
      <thead>
        <tr class="border-b border-gray-100">
          <th class="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide">Topic</th>
          <th class="px-4 py-3 text-right text-xs font-semibold text-gray-500 uppercase tracking-wide">Közösségek</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-gray-100">
        {% for topic, cnt in stats['topic_counts'].items() | sort(attribute='1', reverse=True) %}
        <tr class="hover:bg-gray-50">
          <td class="px-4 py-2.5 font-medium text-gray-900">{{ topic }}</td>
          <td class="px-4 py-2.5 text-right text-gray-700">{{ cnt }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</div>
{% endif %}

{% endblock %}
```

- [ ] **Step 2: Commit**

```bash
git add scraper/web/templates/stats.html
git commit -m "feat: add stats.html admin template"
```

---

### Task 4: Add nav link to base.html + final check

**Files:**
- Modify: `scraper/web/templates/base.html` — add "Stats" link in desktop and mobile nav

- [ ] **Step 1: Add desktop nav link**

In `scraper/web/templates/base.html`, find this line (around line 206):

```html
      {{ navlink('/admin/subscriptions',  'Subscribers', _p.startswith('/admin/subscriptions')) }}
```

Replace it with:

```html
      {{ navlink('/admin/stats',          'Stats',        _p == '/admin/stats') }}
      {{ navlink('/admin/subscriptions',  'Subscribers', _p.startswith('/admin/subscriptions')) }}
```

- [ ] **Step 2: Add mobile nav link**

In `scraper/web/templates/base.html`, find this line (around line 257):

```html
      {{ navlink('/admin/subscriptions', 'Subscribers',   _p.startswith('/admin/subscriptions')) }}
```

Replace it with:

```html
      {{ navlink('/admin/stats',         'Stats',          _p == '/admin/stats') }}
      {{ navlink('/admin/subscriptions', 'Subscribers',   _p.startswith('/admin/subscriptions')) }}
```

- [ ] **Step 3: Run full test suite one final time**

```bash
PYTHONPATH=. .venv/bin/pytest --ignore=tests/test_city_page.py -v
```

Expected: all tests PASS

- [ ] **Step 4: Commit and push**

```bash
git add scraper/web/templates/base.html
git commit -m "feat: add Stats nav link to admin"
git push
```
