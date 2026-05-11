# Community Submission & Re-AI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let users submit missing communities via a public form; let admins review submissions and trigger immediate background scraping; let admins re-run AI extraction on any existing community.

**Architecture:** Four independent layers built bottom-up: DB layer first (no deps), then public form (depends on DB), then admin submissions UI with pipeline scraping (depends on DB + pipeline), then Re-AI button (depends on existing `find_community_by_id` + pipeline). Each task is self-contained and tested before the next begins.

**Tech Stack:** FastAPI (routes + BackgroundTasks), Jinja2 (templates), SQLite via `scraper/db.py`, `FallbackExtractor` from `scraper/extract.py`, `fetch_and_clean` from `scraper/fetch.py`, `save_results` from `scraper/store.py`, `load_cache_page` from `scraper/db.py`.

---

## File Map

| Action | File |
|--------|------|
| Modify | `scraper/db.py` — add `community_submissions` table + 3 CRUD functions |
| Modify | `scraper/pipeline.py` — add `scrape_submitted_url` + `reextract_community` |
| Modify | `scraper/web/app.py` — add 6 routes + extend DB imports + extend pipeline imports + sitemap entry |
| Create | `scraper/web/templates/public_submit_community.html` |
| Create | `scraper/web/templates/submissions.html` |
| Modify | `scraper/web/templates/public_base.html` — footer link |
| Modify | `scraper/web/templates/public_explore.html` — city+topic CTA |
| Modify | `scraper/web/templates/result_detail.html` — Re-AI button per community card |
| Create | `tests/test_community_submission_db.py` |
| Create | `tests/test_community_submission_form.py` |
| Create | `tests/test_community_submission_admin.py` |
| Create | `tests/test_reai.py` |

---

## Task 1: DB Layer

**Files:**
- Modify: `scraper/db.py` (add table near line 308, add functions after line 1747)
- Create: `tests/test_community_submission_db.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_community_submission_db.py`:

```python
from pathlib import Path
from scraper.db import (
    init_db,
    save_community_submission,
    get_community_submissions,
    resolve_community_submission,
)


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def test_save_community_submission_returns_id(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(
        db, "Budapest Futók", "Budapest", "running",
        "https://example.com/futok", "user@example.com"
    )
    assert row_id > 0


def test_get_community_submissions_pending(tmp_path):
    db = _db(tmp_path)
    save_community_submission(db, "Budapest Futók", "Budapest", "running",
                              "https://example.com/futok", None)
    rows = get_community_submissions(db, status="pending")
    assert len(rows) == 1
    r = rows[0]
    assert r["name"] == "Budapest Futók"
    assert r["city"] == "Budapest"
    assert r["topic"] == "running"
    assert r["source_url"] == "https://example.com/futok"
    assert r["submitter_email"] is None
    assert r["status"] == "pending"
    assert r["submitted_at"]


def test_get_community_submissions_filters_by_status(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Klub", "Debrecen", "chess",
                                       "https://example.com/klub", None)
    resolve_community_submission(db, row_id, "approved")
    assert len(get_community_submissions(db, status="pending")) == 0
    approved = get_community_submissions(db, status="approved")
    assert len(approved) == 1
    assert approved[0]["name"] == "Klub"


def test_resolve_community_submission(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Test", "Győr", "yoga",
                                       "https://example.com/test", "a@b.com")
    resolve_community_submission(db, row_id, "rejected")
    rows = get_community_submissions(db, status="rejected")
    assert len(rows) == 1
    assert rows[0]["id"] == row_id
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_community_submission_db.py -v
```

Expected: FAIL with `ImportError: cannot import name 'save_community_submission'`

- [ ] **Step 3: Add `community_submissions` table to `init_db()`**

In `scraper/db.py`, find the block ending with `conn.commit()` that follows the `edit_requests` table (around line 308). Insert before `conn.commit()`:

```python
        conn.execute("""
            CREATE TABLE IF NOT EXISTS community_submissions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL,
                city            TEXT NOT NULL,
                topic           TEXT NOT NULL,
                source_url      TEXT NOT NULL,
                submitter_email TEXT,
                submitted_at    TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending'
            )
        """)
```

- [ ] **Step 4: Add the 3 CRUD functions**

Append to the end of `scraper/db.py`:

```python


# ── Community Submissions ─────────────────────────────────────────────────────

def save_community_submission(
    db_path: Path,
    name: str,
    city: str,
    topic: str,
    source_url: str,
    submitter_email: str | None,
) -> int:
    from datetime import datetime, timezone
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO community_submissions (name, city, topic, source_url, submitter_email, submitted_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            (name, city, topic, source_url, submitter_email,
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return cur.lastrowid


def get_community_submissions(db_path: Path, status: str = "pending") -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, name, city, topic, source_url, submitter_email, submitted_at, status "
            "FROM community_submissions WHERE status=? ORDER BY submitted_at DESC",
            (status,),
        ).fetchall()
    cols = ("id", "name", "city", "topic", "source_url", "submitter_email", "submitted_at", "status")
    return [dict(zip(cols, row)) for row in rows]


def resolve_community_submission(db_path: Path, sub_id: int, status: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE community_submissions SET status=? WHERE id=?",
            (status, sub_id),
        )
        conn.commit()
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_community_submission_db.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add scraper/db.py tests/test_community_submission_db.py
git commit -m "feat: community_submissions table and CRUD functions"
```

---

## Task 2: Public Form + Footer Link + CTA + Sitemap

**Files:**
- Modify: `scraper/web/app.py` (add imports, 2 routes, sitemap entry)
- Create: `scraper/web/templates/public_submit_community.html`
- Modify: `scraper/web/templates/public_base.html` (footer link)
- Modify: `scraper/web/templates/public_explore.html` (CTA)
- Create: `tests/test_community_submission_form.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_community_submission_form.py`:

```python
from pathlib import Path
from scraper.db import init_db, get_community_submissions
from scraper.web import app as web_app
from scraper.web.state import app_state
from scraper.pipeline import CityConfig, TopicConfig
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _setup_state(db):
    app_state.db_path = db
    app_state.cities = [
        CityConfig(name="Budapest", country="Hungary", locale="hu", search_variants=[]),
    ]
    app_state.topics = [
        TopicConfig(name="running", search_terms={}),
        TopicConfig(name="yoga", search_terms={}),
    ]


def test_get_form_returns_200(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).get("/kozosseg-bekuldes")
        assert resp.status_code == 200
        assert "form" in resp.text.lower()
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_get_form_prefills_city_and_topic(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).get("/kozosseg-bekuldes?city=Budapest&topic=running")
        assert resp.status_code == 200
        assert "Budapest" in resp.text
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_post_form_saves_submission_and_redirects(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app, follow_redirects=False).post(
            "/kozosseg-bekuldes",
            data={
                "name": "Budapest Futók",
                "city": "Budapest",
                "topic": "running",
                "source_url": "https://example.com/futok",
                "submitter_email": "",
            },
        )
        assert resp.status_code == 302
        assert "submitted=1" in resp.headers["location"]
        rows = get_community_submissions(db, status="pending")
        assert len(rows) == 1
        assert rows[0]["name"] == "Budapest Futók"
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_post_form_missing_required_field_returns_400(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).post(
            "/kozosseg-bekuldes",
            data={"name": "", "city": "Budapest", "topic": "running", "source_url": "https://x.com"},
        )
        assert resp.status_code == 400
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics


def test_get_form_submitted_shows_thank_you(tmp_path):
    db = _db(tmp_path)
    old_db, old_cities, old_topics = app_state.db_path, app_state.cities, app_state.topics
    try:
        _setup_state(db)
        resp = TestClient(web_app.app).get("/kozosseg-bekuldes?submitted=1")
        assert resp.status_code == 200
        assert "köszön" in resp.text.lower()
    finally:
        app_state.db_path = old_db
        app_state.cities = old_cities
        app_state.topics = old_topics
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_community_submission_form.py -v
```

Expected: FAIL with 404 (routes don't exist yet).

- [ ] **Step 3: Extend the DB imports in `app.py`**

In `scraper/web/app.py`, find the `from ..db import (` block (around line 26). Add these three names to the import list (anywhere in the list):

```python
    save_community_submission,
    get_community_submissions,
    resolve_community_submission,
```

- [ ] **Step 4: Add the two public routes to `app.py`**

Find the sitemap route (`@_fastapi.get("/sitemap.xml")`, around line 1350). Insert the two routes immediately BEFORE the sitemap route:

```python
@_fastapi.get("/kozosseg-bekuldes", response_class=HTMLResponse)
async def submit_community_get(request: Request, city: str = "", topic: str = ""):
    init_db(_db())
    submitted = request.query_params.get("submitted") == "1"
    all_cities = sorted(c.name for c in (app_state.cities or []))
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

```

- [ ] **Step 5: Add `/kozosseg-bekuldes` to the sitemap**

In the sitemap route (around line 1356), find the `locs` list and add the entry after `/emberek`:

```python
        base + "/emberek",
        base + "/kozosseg-bekuldes",
```

- [ ] **Step 6: Create `public_submit_community.html`**

Create `scraper/web/templates/public_submit_community.html`:

```html
{% extends "public_base.html" %}
{% block title %}Közösség beküldése – közösségek.com{% endblock %}
{% block content %}

<div class="max-w-2xl mx-auto px-4 sm:px-6 py-10">

  <!-- Header -->
  <div class="rounded-2xl bg-gradient-to-br from-[#A8512F] to-[#7A3A1E] px-8 py-10 mb-8 text-white">
    <h1 class="text-2xl font-bold mb-1">Közösség beküldése</h1>
    <p class="text-white/80 text-sm">Ismersz egy közösséget, ami még nem szerepel nálunk? Küldd be és mi hozzáadjuk!</p>
  </div>

  {% if submitted %}
  <!-- Thank-you state -->
  <div class="bg-white rounded-xl border border-[#EAE5DB] p-10 text-center">
    <i class="ph ph-check-circle text-5xl text-[#A8512F] mb-4 block"></i>
    <h2 class="text-lg font-bold text-[#1C1917] mb-2">Köszönjük a beküldést!</h2>
    <p class="text-sm text-[#8C8478] mb-6">Hamarosan átnézzük és hozzáadjuk az adatbázishoz.</p>
    <a href="/" class="inline-flex items-center gap-2 px-5 py-2.5 bg-[#A8512F] text-white text-sm font-semibold rounded-lg hover:bg-[#8A4226] transition-colors">
      <i class="ph ph-house"></i> Vissza a főoldalra
    </a>
  </div>

  {% else %}
  <!-- Form -->
  <div class="bg-white rounded-xl border border-[#EAE5DB] p-6 sm:p-8">
    <form method="POST" action="/kozosseg-bekuldes" class="space-y-5">

      <div>
        <label class="block text-sm font-semibold text-[#1C1917] mb-1.5">Közösség neve <span class="text-red-500">*</span></label>
        <input type="text" name="name" required
          class="w-full px-4 py-2.5 border border-[#D8D1C4] rounded-lg text-sm focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20 transition-colors placeholder-[#B5ADA0]"
          placeholder="pl. Budapest Futók">
      </div>

      <div>
        <label class="block text-sm font-semibold text-[#1C1917] mb-1.5">Város <span class="text-red-500">*</span></label>
        <select name="city" required
          class="w-full px-4 py-2.5 border border-[#D8D1C4] rounded-lg text-sm focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20 bg-white transition-colors">
          <option value="">— Válassz várost —</option>
          {% for c in all_cities %}
          <option value="{{ c }}" {% if c == city %}selected{% endif %}>{{ c }}</option>
          {% endfor %}
        </select>
      </div>

      <div>
        <label class="block text-sm font-semibold text-[#1C1917] mb-1.5">Témakör <span class="text-red-500">*</span></label>
        <select name="topic" required
          class="w-full px-4 py-2.5 border border-[#D8D1C4] rounded-lg text-sm focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20 bg-white transition-colors">
          <option value="">— Válassz témakört —</option>
          {% for tp in all_topics %}
          <option value="{{ tp.name }}" {% if tp.name == topic %}selected{% endif %}>{{ tp.label }}</option>
          {% endfor %}
        </select>
      </div>

      <div>
        <label class="block text-sm font-semibold text-[#1C1917] mb-1.5">Weboldal / közösségi oldal link <span class="text-red-500">*</span></label>
        <input type="url" name="source_url" required
          class="w-full px-4 py-2.5 border border-[#D8D1C4] rounded-lg text-sm focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20 transition-colors placeholder-[#B5ADA0]"
          placeholder="https://...">
      </div>

      <div>
        <label class="block text-sm font-semibold text-[#1C1917] mb-1.5">Kapcsolattartó email <span class="text-[#B5ADA0] font-normal">(opcionális)</span></label>
        <input type="email" name="submitter_email"
          class="w-full px-4 py-2.5 border border-[#D8D1C4] rounded-lg text-sm focus:outline-none focus:border-[#C2613A] focus:ring-2 focus:ring-[#C2613A]/20 transition-colors placeholder-[#B5ADA0]"
          placeholder="your@email.com">
      </div>

      <div class="pt-2">
        <button type="submit"
          class="w-full sm:w-auto px-8 py-3 bg-[#A8512F] text-white text-sm font-semibold rounded-lg hover:bg-[#8A4226] transition-colors">
          Beküldés
        </button>
      </div>

    </form>
  </div>
  {% endif %}

</div>
{% endblock %}
```

- [ ] **Step 7: Add footer link in `public_base.html`**

In `scraper/web/templates/public_base.html`, find the footer `<div class="flex items-center gap-4">` (around line 128). Add the link inside that div, after the Admin link:

```html
      <a href="/rolunk" class="text-xs text-[#8C8478] hover:text-[#4A4441] transition-colors">{{ t('nav_about') }}</a>
      <a href="/terkep" class="text-xs text-[#8C8478] hover:text-[#4A4441] transition-colors">{{ t('nav_map') }}</a>
      <a href="/kozosseg-bekuldes" class="text-xs text-[#8C8478] hover:text-[#4A4441] transition-colors">Közösség beküldése</a>
      <a href="/admin" class="text-xs text-[#D8D1C4] hover:text-[#8C8478] transition-colors">Admin</a>
```

- [ ] **Step 8: Add city+topic CTA in `public_explore.html`**

In `scraper/web/templates/public_explore.html`, find the `<!-- Subscribe section -->` comment (around line 379). Insert immediately before it:

```html
  {% if city and selected_topics | length == 1 %}
  <div class="mt-6 text-center">
    <a href="/kozosseg-bekuldes?city={{ city | urlencode }}&topic={{ selected_topics[0] | urlencode }}"
       class="inline-flex items-center gap-2 text-sm text-[#A8512F] hover:text-[#8A4226] hover:underline transition-colors">
      <i class="ph ph-plus-circle"></i> Hiányzik egy közösség? Küldd be →
    </a>
  </div>
  {% endif %}

```

- [ ] **Step 9: Run tests to verify they pass**

```bash
pytest tests/test_community_submission_form.py -v
```

Expected: 5 tests PASS.

- [ ] **Step 10: Commit**

```bash
git add scraper/web/app.py scraper/web/templates/public_submit_community.html \
        scraper/web/templates/public_base.html scraper/web/templates/public_explore.html \
        tests/test_community_submission_form.py
git commit -m "feat: public community submission form, footer link, CTA, sitemap"
```

---

## Task 3: Admin Submissions UI + Pipeline Scraping Function

**Files:**
- Modify: `scraper/pipeline.py` (add `scrape_submitted_url` async function)
- Modify: `scraper/web/app.py` (add 3 admin routes + pipeline import)
- Create: `scraper/web/templates/submissions.html`
- Create: `tests/test_community_submission_admin.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_community_submission_admin.py`:

```python
from pathlib import Path
from unittest.mock import AsyncMock, patch
from scraper.db import init_db, save_community_submission, get_community_submissions
from scraper.pipeline import PipelineConfig
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _cfg(db: Path) -> PipelineConfig:
    return PipelineConfig(
        searxng_url="http://localhost:8888",
        ollama_url="http://localhost:11434",
        ollama_model="llama3",
        ollama_temperature=0.1,
        ollama_timeout=30,
        ollama_max_text_chars=6000,
        search_results_per_query=5,
        search_max_pages=2,
        search_rate_limit=1.0,
        fetch_timeout=15,
        fetch_min_text_length=100,
        fetch_max_concurrent=3,
        fetch_blocked_domains=[],
        db_path=db,
    )


def test_reject_submission(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Club A", "Budapest", "running",
                                       "https://a.example.com", None)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        resp = TestClient(web_app.app).post(
            f"/admin/submissions/{row_id}/reject",
            headers={"Authorization": "Basic YWRtaW46"},
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        rows = get_community_submissions(db, status="rejected")
        assert len(rows) == 1
        assert rows[0]["name"] == "Club A"
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg


def test_approve_submission_enqueues_scrape(tmp_path):
    db = _db(tmp_path)
    row_id = save_community_submission(db, "Club B", "Budapest", "yoga",
                                       "https://b.example.com", None)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        with patch("scraper.web.app.scrape_submitted_url", new_callable=AsyncMock) as mock_scrape:
            resp = TestClient(web_app.app).post(
                f"/admin/submissions/{row_id}/approve",
                headers={"Authorization": "Basic YWRtaW46"},
            )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        rows = get_community_submissions(db, status="approved")
        assert len(rows) == 1
        assert rows[0]["name"] == "Club B"
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg
```

Note: `YWRtaW46` is the base64 encoding of `admin:` (admin with empty password — matches `ADMIN_PASSWORD=""` default in tests).

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_community_submission_admin.py -v
```

Expected: FAIL with 404 (routes don't exist) or ImportError.

- [ ] **Step 3: Add `scrape_submitted_url` to `pipeline.py`**

First, add `load_cache_page` to the existing `from .db import` line (around line 15 in `pipeline.py`):

```python
from .db import get_search_cache, save_search_cache, upsert_venues, upsert_persons, delete_leader_persons_for_community, load_cache_page
```

Then append the following function at the end of `scraper/pipeline.py`:

```python


async def scrape_submitted_url(
    db_path: Path,
    config: "PipelineConfig",
    city: str,
    topic: str,
    url: str,
) -> bool:
    ollama = OllamaExtractor(
        base_url=config.ollama_url,
        model=config.ollama_model,
        temperature=config.ollama_temperature,
        timeout_seconds=config.ollama_timeout,
        max_text_chars=config.ollama_max_text_chars,
    )
    primaries = []
    if config.deepseek_api_key:
        primaries.append(DeepSeekExtractor(
            api_key=config.deepseek_api_key,
            model=config.deepseek_model,
            temperature=config.deepseek_temperature,
            timeout_seconds=config.deepseek_timeout,
            max_text_chars=config.deepseek_max_text_chars,
            rate_limit_seconds=config.deepseek_rate_limit_seconds,
        ))
    if config.groq_api_key:
        primaries.append(GroqExtractor(
            api_key=config.groq_api_key,
            model=config.groq_model,
            temperature=config.groq_temperature,
            timeout_seconds=config.groq_timeout,
            max_text_chars=config.groq_max_text_chars,
            rate_limit_seconds=config.groq_rate_limit_seconds,
        ))
    extractor: OllamaExtractor | FallbackExtractor = (
        FallbackExtractor(primaries=primaries, fallback=ollama) if primaries else ollama
    )

    text = await fetch_and_clean(url, blocked_domains=[], timeout_seconds=15)
    if not text:
        log.warning("scrape_submitted_url_no_text", url=url)
        return False

    records = await extractor.extract(
        text=text, city=city, topic=topic, locale="hu", source_url=url,
    )
    save_results(city, topic, records, db_path)
    log.info("scrape_submitted_url_done", city=city, topic=topic, url=url, found=len(records))
    return True
```

- [ ] **Step 4: Add `scrape_submitted_url` to the pipeline import in `app.py`**

In `scraper/web/app.py`, find the pipeline import line (around line 89):

```python
from ..pipeline import _enrich_record, _needs_enrichment, run_pipeline
```

Change it to:

```python
from ..pipeline import _enrich_record, _needs_enrichment, run_pipeline, scrape_submitted_url
```

- [ ] **Step 5: Add the 3 admin routes to `app.py`**

Find `@admin.get("/revalidate/start")` or any nearby admin route (around line 1800). Insert these 3 routes BEFORE it:

```python
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

```

- [ ] **Step 6: Create `submissions.html`**

Create `scraper/web/templates/submissions.html`:

```html
{% extends "base.html" %}
{% block title %}Beküldött közösségek – Admin{% endblock %}
{% block content %}

<div class="flex items-center justify-between mb-6">
  <div>
    <h1 class="text-xl font-bold text-gray-900">Beküldött közösségek</h1>
    <p class="text-sm text-gray-500 mt-0.5">{{ submissions | length }} függőben</p>
  </div>
</div>

{% if not submissions %}
<div class="bg-white rounded-lg border border-gray-200 p-12 text-center text-gray-400">
  <i class="ph ph-check-circle text-5xl mb-3 block"></i>
  <p>Nincs függőben lévő beküldés.</p>
</div>
{% else %}
<div class="bg-white rounded-xl border border-gray-200 overflow-hidden">
  <table class="w-full text-sm">
    <thead class="bg-gray-50 border-b border-gray-200">
      <tr>
        <th class="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase">Közösség</th>
        <th class="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase">Város</th>
        <th class="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase">Témakör</th>
        <th class="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase">URL</th>
        <th class="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase">Email</th>
        <th class="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase">Beküldve</th>
        <th class="px-4 py-3 w-44"></th>
      </tr>
    </thead>
    <tbody class="divide-y divide-gray-100">
      {% for s in submissions %}
      <tr class="hover:bg-gray-50 transition-colors" id="row-{{ s.id }}">
        <td class="px-4 py-3 font-medium text-gray-900">{{ s.name }}</td>
        <td class="px-4 py-3 text-gray-600">{{ s.city }}</td>
        <td class="px-4 py-3 text-gray-600">{{ s.topic.replace('_', ' ') }}</td>
        <td class="px-4 py-3 text-xs">
          <a href="{{ s.source_url }}" target="_blank" class="text-blue-600 hover:underline truncate block max-w-xs">{{ s.source_url }}</a>
        </td>
        <td class="px-4 py-3 text-xs text-gray-500">{{ s.submitter_email or '—' }}</td>
        <td class="px-4 py-3 text-xs text-gray-400 whitespace-nowrap">{{ s.submitted_at[:10] }}</td>
        <td class="px-4 py-3 text-right">
          <div class="flex items-center justify-end gap-2">
            <button onclick="approve({{ s.id }})"
              class="px-3 py-1.5 text-xs font-semibold rounded-lg bg-green-50 text-green-700 border border-green-200 hover:bg-green-100 transition-colors">
              <i class="ph ph-check"></i> Jóváhagy
            </button>
            <button onclick="reject({{ s.id }})"
              class="px-3 py-1.5 text-xs font-semibold rounded-lg bg-gray-50 text-gray-500 border border-gray-200 hover:bg-gray-100 transition-colors">
              Elutasít
            </button>
          </div>
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

<script>
async function approve(id) {
  const row = document.getElementById('row-' + id);
  const res = await fetch('/admin/submissions/' + id + '/approve', { method: 'POST' });
  const d = await res.json();
  if (d.ok) { row.style.opacity = '0.4'; setTimeout(() => row.remove(), 350); }
  else alert('Error: ' + (d.error || 'unknown'));
}

async function reject(id) {
  const row = document.getElementById('row-' + id);
  const res = await fetch('/admin/submissions/' + id + '/reject', { method: 'POST' });
  const d = await res.json();
  if (d.ok) { row.style.opacity = '0.4'; setTimeout(() => row.remove(), 350); }
  else alert('Error: ' + (d.error || 'unknown'));
}
</script>

{% endblock %}
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
pytest tests/test_community_submission_admin.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 8: Commit**

```bash
git add scraper/pipeline.py scraper/web/app.py \
        scraper/web/templates/submissions.html \
        tests/test_community_submission_admin.py
git commit -m "feat: admin submissions UI and scrape_submitted_url pipeline function"
```

---

## Task 4: Re-AI Admin UI + Pipeline Function

**Files:**
- Modify: `scraper/pipeline.py` (add `reextract_community` function)
- Modify: `scraper/web/app.py` (add `/admin/communities/{community_id}/reai` route + import)
- Modify: `scraper/web/templates/result_detail.html` (Re-AI button per community card)
- Create: `tests/test_reai.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_reai.py`:

```python
import hashlib
from pathlib import Path
from unittest.mock import AsyncMock, patch
from scraper.db import init_db, save_cache_page
from scraper.models import CommunityRecord
from scraper.pipeline import PipelineConfig
from scraper.store import save_results
from scraper.web import app as web_app
from scraper.web.state import app_state
from fastapi.testclient import TestClient


def _db(tmp_path: Path) -> Path:
    p = tmp_path / "scraper.db"
    init_db(p)
    return p


def _cfg(db: Path) -> PipelineConfig:
    return PipelineConfig(
        searxng_url="http://localhost:8888",
        ollama_url="http://localhost:11434",
        ollama_model="llama3",
        ollama_temperature=0.1,
        ollama_timeout=30,
        ollama_max_text_chars=6000,
        search_results_per_query=5,
        search_max_pages=2,
        search_rate_limit=1.0,
        fetch_timeout=15,
        fetch_min_text_length=100,
        fetch_max_concurrent=3,
        fetch_blocked_domains=[],
        db_path=db,
    )


def _community(db: Path, name="Budapest Futók") -> CommunityRecord:
    r = CommunityRecord(
        name=name, topic="running", city="Budapest", locale="hu",
        source_url="https://example.com/futok",
        extracted_at="2026-01-01T00:00:00+00:00",
    )
    save_results("Budapest", "running", [r], db)
    return r


def test_reai_endpoint_not_found_returns_ok_false(tmp_path):
    db = _db(tmp_path)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        resp = TestClient(web_app.app).post(
            "/admin/communities/nonexistentid/reai",
            headers={"Authorization": "Basic YWRtaW46"},
        )
        assert resp.status_code == 200
        assert resp.json()["ok"] is False
        assert resp.json()["error"] == "not_found"
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg


def test_reai_endpoint_found_returns_ok_true(tmp_path):
    db = _db(tmp_path)
    r = _community(db)
    old_db, old_cfg = app_state.db_path, app_state.pipeline_cfg
    try:
        app_state.db_path = db
        app_state.pipeline_cfg = _cfg(db)
        with patch("scraper.web.app.reextract_community", new_callable=AsyncMock):
            resp = TestClient(web_app.app).post(
                f"/admin/communities/{r.community_id}/reai",
                headers={"Authorization": "Basic YWRtaW46"},
            )
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
    finally:
        app_state.db_path = old_db
        app_state.pipeline_cfg = old_cfg


def test_reextract_community_uses_cached_text(tmp_path):
    import asyncio
    from scraper.pipeline import reextract_community

    db = _db(tmp_path)
    r = _community(db)
    url_hash = hashlib.sha256(r.source_url.encode()).hexdigest()[:16]
    save_cache_page(db, {"url": r.source_url, "url_hash": url_hash, "raw_text": "Futó klub szöveg"})

    cfg = _cfg(db)
    with patch("scraper.pipeline.FallbackExtractor") as MockFallback, \
         patch("scraper.pipeline.OllamaExtractor") as MockOllama:
        mock_extractor = AsyncMock()
        mock_extractor.extract = AsyncMock(return_value=[])
        MockOllama.return_value = mock_extractor
        result = asyncio.run(reextract_community(db, cfg, r.community_id))

    assert result is True
    mock_extractor.extract.assert_called_once()
    call_kwargs = mock_extractor.extract.call_args
    assert "Futó klub szöveg" in str(call_kwargs)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_reai.py -v
```

Expected: FAIL with ImportError or 404.

- [ ] **Step 3: Add `reextract_community` to `pipeline.py`**

Append at the end of `scraper/pipeline.py` (after `scrape_submitted_url`):

```python


async def reextract_community(
    db_path: Path,
    config: "PipelineConfig",
    community_id: str,
) -> bool:
    import hashlib as _hashlib
    from .db import find_community_by_id

    record = find_community_by_id(db_path, community_id)
    if not record:
        log.warning("reextract_community_not_found", community_id=community_id)
        return False

    source_url = record.get("source_url", "")
    city = record.get("city", "")
    topic = record.get("topic", "")

    url_hash = _hashlib.sha256(source_url.encode()).hexdigest()[:16]
    cached = load_cache_page(db_path, url_hash)
    text = cached.get("raw_text") if cached else None

    if not text:
        text = await fetch_and_clean(source_url, blocked_domains=[], timeout_seconds=15)
    if not text:
        log.warning("reextract_community_no_text", community_id=community_id, url=source_url)
        return False

    ollama = OllamaExtractor(
        base_url=config.ollama_url,
        model=config.ollama_model,
        temperature=config.ollama_temperature,
        timeout_seconds=config.ollama_timeout,
        max_text_chars=config.ollama_max_text_chars,
    )
    primaries = []
    if config.deepseek_api_key:
        primaries.append(DeepSeekExtractor(
            api_key=config.deepseek_api_key,
            model=config.deepseek_model,
            temperature=config.deepseek_temperature,
            timeout_seconds=config.deepseek_timeout,
            max_text_chars=config.deepseek_max_text_chars,
            rate_limit_seconds=config.deepseek_rate_limit_seconds,
        ))
    if config.groq_api_key:
        primaries.append(GroqExtractor(
            api_key=config.groq_api_key,
            model=config.groq_model,
            temperature=config.groq_temperature,
            timeout_seconds=config.groq_timeout,
            max_text_chars=config.groq_max_text_chars,
            rate_limit_seconds=config.groq_rate_limit_seconds,
        ))
    extractor: OllamaExtractor | FallbackExtractor = (
        FallbackExtractor(primaries=primaries, fallback=ollama) if primaries else ollama
    )

    records = await extractor.extract(
        text=text, city=city, topic=topic, locale="hu", source_url=source_url,
    )
    save_results(city, topic, records, db_path)
    log.info("reextract_community_done", community_id=community_id, found=len(records))
    return True
```

- [ ] **Step 4: Add `reextract_community` to the pipeline import in `app.py`**

Update the pipeline import line (already modified in Task 3):

```python
from ..pipeline import _enrich_record, _needs_enrichment, run_pipeline, scrape_submitted_url, reextract_community
```

- [ ] **Step 5: Add the `/admin/communities/{community_id}/reai` route to `app.py`**

Find the 3 submissions routes added in Task 3 (`@admin.post("/submissions/{sub_id}/reject")`). Insert the new route immediately AFTER them:

```python
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

```

Note: `find_community_by_id` is already imported in `app.py` (line 28).

- [ ] **Step 6: Add Re-AI button to `result_detail.html`**

In `scraper/web/templates/result_detail.html`, find the action row at the bottom of each community card (around line 104). The row ends with:

```html
      <form method="POST" action="/admin/false-positive/add" class="ml-auto">
        ...
        <button type="submit" ...>
          <i class="ph ph-flag" style="font-size:13px"></i> Nem közösség
        </button>
      </form>
```

Change the entire action row (the `<div class="mt-3 pt-3 border-t border-gray-50 ...">`) to:

```html
    <div class="mt-3 pt-3 border-t border-gray-50 text-xs text-gray-400 flex items-center gap-4 flex-wrap">
      <span>Forrás: <a href="{{ r.source_url }}" target="_blank" class="hover:underline truncate">{{ r.source_url }}</a></span>
      <span>{{ r.extracted_at[:10] }}</span>
      {% if url_hashes.get(r.source_url) %}
      <a href="/admin/progress/{{ url_hashes[r.source_url] }}"
         class="text-blue-500 hover:text-blue-700 font-medium whitespace-nowrap">
        Pipeline →
      </a>
      {% endif %}
      <button onclick="reai('{{ r.community_id }}')"
        id="reai-{{ r.community_id }}"
        class="text-indigo-400 hover:text-indigo-700 transition-colors flex items-center gap-1">
        <i class="ph ph-robot" style="font-size:13px"></i> Re-AI
      </button>
      <form method="POST" action="/admin/false-positive/add" class="ml-auto">
        <input type="hidden" name="name" value="{{ r.name }}">
        <input type="hidden" name="city" value="{{ city }}">
        <input type="hidden" name="topic" value="{{ topic }}">
        <input type="hidden" name="reason" value="Flagged from result detail view">
        <input type="hidden" name="source_url" value="{{ r.source_url }}">
        <input type="hidden" name="fp_type" value="extraction">
        <button type="submit"
          class="flex items-center gap-1 text-red-400 hover:text-red-700 transition-colors"
          title="Mark as not a community (adds to false positive list)">
          <i class="ph ph-flag" style="font-size:13px"></i> Nem közösség
        </button>
      </form>
    </div>
```

Then add the `<script>` block right before `{% endblock %}` at the bottom of the file:

```html
<script>
async function reai(communityId) {
  const btn = document.getElementById('reai-' + communityId);
  btn.innerHTML = '<i class="ph ph-spinner ph-spin" style="font-size:13px"></i> Folyamatban…';
  btn.disabled = true;
  const res = await fetch('/admin/communities/' + communityId + '/reai', { method: 'POST' });
  const d = await res.json();
  if (d.ok) {
    btn.innerHTML = '<i class="ph ph-check" style="font-size:13px"></i> Kész';
  } else {
    btn.innerHTML = '<i class="ph ph-warning" style="font-size:13px"></i> Hiba';
    btn.disabled = false;
  }
}
</script>
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
pytest tests/test_reai.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 8: Run the full test suite**

```bash
pytest -v
```

Expected: All tests PASS (no regressions).

- [ ] **Step 9: Commit**

```bash
git add scraper/pipeline.py scraper/web/app.py \
        scraper/web/templates/result_detail.html \
        tests/test_reai.py
git commit -m "feat: Re-AI button and reextract_community pipeline function"
```
