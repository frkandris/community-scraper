# Community Submission & Re-AI — Design Spec

**Goal:** Let users submit missing communities via a public form; let admins review submissions and trigger immediate scraping; let admins re-run AI extraction on any existing community.

---

## Architecture overview

4 independent layers, 4 implementation tasks:

1. **DB layer** — new `community_submissions` table + 3 CRUD functions
2. **Public form** — `/kozosseg-bekuldes` GET/POST + footer link + city+topic CTA + sitemap entry
3. **Admin submissions UI** — `/admin/submissions` + approve (triggers scraping) + reject
4. **Re-AI admin UI** — Re-AI button on `/admin/results/{city}/{topic}` + endpoint + pipeline function

---

## Task 1: DB layer

### Table: `community_submissions`

Added to `init_db()` in `scraper/db.py` via `CREATE TABLE IF NOT EXISTS`:

```sql
CREATE TABLE IF NOT EXISTS community_submissions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    topic       TEXT NOT NULL,
    source_url  TEXT NOT NULL,
    submitter_email TEXT,
    submitted_at TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'
)
```

Status values: `pending`, `approved`, `rejected`.

### Functions

**`save_community_submission(db_path, name, city, topic, source_url, submitter_email) -> int`**
Inserts a row with `status='pending'`, `submitted_at=utcnow().isoformat()`. Returns the new row `id`.

**`get_community_submissions(db_path, status="pending") -> list[dict]`**
Returns rows matching `status`, ordered by `submitted_at DESC`. Each row is a plain dict.

**`resolve_community_submission(db_path, sub_id: int, status: str)`**
Updates `status` for the given `id`. Valid values: `approved`, `rejected`.

---

## Task 2: Public form + footer + CTA + sitemap

### Route: `GET /kozosseg-bekuldes`

Renders `public_submit_community.html`. Prefills city and topic from `?city=` and `?topic=` query params (passed straight to the template). Passes `all_cities` (sorted list from `app_state.cities`) and `all_topics` (sorted list from `app_state.topics` with labels).

Shows a success message when `?submitted=1` is in the URL (redirect after POST).

### Route: `POST /kozosseg-bekuldes`

Accepts form fields: `name`, `city`, `topic`, `source_url`, `submitter_email` (optional). Validates that `name`, `city`, `topic`, `source_url` are non-empty; returns 400 JSON if any is missing. On success: calls `save_community_submission`, redirects to `/kozosseg-bekuldes?submitted=1`.

### Template: `public_submit_community.html`

Extends `public_base.html`. Brand-gradient header. Form with:
- Text input: közösség neve (required)
- Select: város — options from `all_cities`, pre-selected if `?city=` param set
- Select: témakör — options from `all_topics` (label shown, name value), pre-selected if `?topic=` param set
- URL input: weboldal / közösségi oldal link (required)
- Email input: kapcsolattartó email (optional)
- Submit gomb

On `?submitted=1`: shows a thank-you message instead of the form.

### Footer nav (`public_base.html`)

Add link alongside the existing Rólunk/Térkép/Admin links:

```html
<a href="/kozosseg-bekuldes" class="text-xs text-[#8C8478] hover:text-[#4A4441] transition-colors">Közösség beküldése</a>
```

### City+topic CTA (`public_explore.html`)

When both `city` and the first selected topic are set (i.e. we're on a `/{city_slug}/{topic_slug}` view), add a small prompt at the bottom of the community list, before the subscribe section:

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

### Sitemap

Add `/kozosseg-bekuldes` to the existing sitemap route in `app.py` (alongside `/kereses`, `/helyszinek`, etc.).

---

## Task 3: Admin submissions UI + pipeline scraping function

### Pipeline function: `scrape_submitted_url`

Added to `scraper/pipeline.py`:

```python
async def scrape_submitted_url(
    db_path: Path,
    config: PipelineConfig,
    city: str,
    topic: str,
    url: str,
) -> bool:
```

- Builds `FallbackExtractor` from `config` (same pattern as `run_pipeline`).
- Calls `fetch_and_clean(url, blocked_domains=[], timeout_seconds=15)`.
- If no text returned: logs warning, returns `False`.
- Calls `extractor.extract(text, city, topic)` to get `list[CommunityRecord]`.
- Calls `save_results(city, topic, records, db_path)`.
- Returns `True`.

### Admin routes

**`GET /admin/submissions`** — renders `admin_submissions.html` with pending submissions from `get_community_submissions(_db(), status="pending")`.

**`POST /admin/submissions/{sub_id}/approve`** — calls `resolve_community_submission(_db(), sub_id, "approved")`, then enqueues `scrape_submitted_url` as a FastAPI `BackgroundTasks` task using `app_state.pipeline_cfg`. Returns `{"ok": True}`.

**`POST /admin/submissions/{sub_id}/reject`** — calls `resolve_community_submission(_db(), sub_id, "rejected")`. Returns `{"ok": True}`.

### Template: `admin_submissions.html`

Extends `base.html`. Table with columns: submitted_at, name, city, topic, source_url (clickable), submitter_email. Two buttons per row: "Jóváhagy" (POST approve, JS fetch) and "Elutasít" (POST reject, JS fetch). Row fades out on success.

---

## Task 4: Re-AI admin UI + pipeline function

### Pipeline function: `reextract_community`

Added to `scraper/pipeline.py`:

```python
async def reextract_community(
    db_path: Path,
    config: PipelineConfig,
    community_id: str,
) -> bool:
```

- Loads the community record from DB (`get_communities_for_city` filtered by `community_id`... actually use a direct `get_community_by_id` helper or query directly).
- Gets `source_url` from the record.
- Computes `url_hash = hashlib.sha256(source_url.encode()).hexdigest()[:16]`.
- Loads cached page via `load_cache_page(db_path, url_hash)`.
- If no cache entry or no `page_text` field: fetches fresh with `fetch_and_clean`.
- Builds `FallbackExtractor`, re-extracts, calls `save_results`.
- Returns `True` on success, `False` if community or page text not found.

### DB helper: `get_community_by_id`

Added to `scraper/db.py`:

```python
def get_community_by_id(db_path: Path, community_id: str) -> dict | None:
```

Queries `communities` table: `SELECT data FROM communities WHERE json_extract(data,'$.community_id')=?`.

### Admin route

**`POST /admin/communities/{community_id}/reai`** — looks up community, enqueues `reextract_community` as BackgroundTask. Returns `{"ok": True}` immediately. If community not found: `{"ok": False, "error": "not_found"}`.

### Template update: `result_detail.html`

Add a "Re-AI" button to each community card's action row (alongside existing delete/false-positive actions). Button POSTs to `/admin/communities/{r.community_id}/reai` via JS fetch, shows spinner while pending, shows "✓" on success.

---

## Out of scope

- Email notification to submitter on approval/rejection.
- Deduplication check before saving a submission.
- Re-AI triggering a fresh URL fetch if no cache exists for `reextract_community` — it falls back to fetching, so it works either way.
- Pagination on admin submissions list.
