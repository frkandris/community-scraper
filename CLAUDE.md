# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run tests
PYTHONPATH=. .venv/bin/pytest --ignore=tests/test_city_page.py

# Run a single test file
PYTHONPATH=. .venv/bin/pytest tests/test_store.py

# Note: tests/test_city_page.py has a pre-existing failure unrelated to any changes — always ignore it

# Lint
ruff check scraper/

# Install dev deps
pip install -e ".[dev]"
```

**No local dev environment.** The app runs on Hetzner, managed by Coolify. Do not attempt to start the server locally for verification — read templates and code directly instead. The app is a FastAPI server (uvicorn) on port 8000 in production. `ADMIN_PASSWORD` env var must be set or the admin UI is inaccessible.

## Architecture

The scraper discovers community groups for each `(city, topic)` pair:

1. **Search** (`search.py`): DataForSEO only (`DataForSEOClient`, live or standard mode). Quota errors (`SearchQuotaError`) permanently skip the provider for the run via `FallbackSearchClient` (kept as a single-provider wrapper so a fallback can be re-added with one line).
2. **Fetch** (`fetch.py`): `httpx` + `trafilatura` to extract clean page text. Blocked domains (Facebook, Instagram, TikTok, LinkedIn, YouTube, Reddit, Twitter/X) return `None` immediately. `playwright_domains` in `settings.yaml` controls Playwright-fetched domains (currently empty — the Playwright fetcher is dormant).
3. **Extract** (`extract.py`): DeepSeek only (`DeepSeekExtractor` on the `_ApiExtractor` base). `FallbackExtractor` wraps it with `_exhausted` / `_blocked_until` state; a second provider can be re-added as another primary.
4. **Store** (`store.py` → `db.py`): Upsert to SQLite `communities` table, merging `source_urls` on conflict.

The full run is orchestrated by `pipeline.py:run_pipeline()`. Modes:
- `full`: search → fetch → extract → enrich (default; labelled "Smart" in the UI)
- `ai_only`: re-extract from cached page texts, no web requests
- `revalidate`: re-validates communities whose `revalidate_fingerprint` is stale (separate flow via `_run_revalidate`, not `run_pipeline`)

**Pipeline city priority**: `main.py` runs three sequential `run_pipeline()` calls — Hungary → Sweden → rest of world. Sweden is second due to its 290-municipality city list. This split is in `main.py`, not `pipeline.py`.

**Done-pair pre-filter**: `run_pipeline()` calls `get_fully_processed_pairs(db_path, current_fp)` (one SQL query) before inner loops and passes the complement as `pairs_filter`. Pairs with a `search_cache` entry AND all `cache_pages` at the current `extract_fingerprint` are skipped entirely — no loop iteration, no log entry. Fully-covered cities should not appear in the log.

**Scheduler**: cron is opt-in — `schedule.cron_enabled: false` by default (no scheduled runs). When enabled, `schedule.cron` fires `_scheduled_run` (preset `35 16 * * *` UTC = DeepSeek off-peak window start). Pipeline otherwise starts via manual button presses or `auto_run_on_startup`.

**Topic tiering**: cities with `topic_tier: core` in `cities.yaml` (currently the 260 smallest Swedish kommuner) only run `pipeline.core_topics` from `settings.yaml`; tiered-out pairs are fully frozen (no search, no re-extraction). `_tier_allows()` in `pipeline.py` is the single gate, also used by `/admin/api/search/jobs`.

**Search cost rules**: every *successful* search is saved to `search_cache` — even empty results and Full Refresh runs (an unsaved empty search is re-paid every run). Provider failures raise (`SearchQuotaError` / `ExtractorUnavailableError`) and are NOT cached: the pair/page is skipped with a `search_failed`/`extract_failed` pair-log counter and retried next run. Never convert these errors to empty results — caching an empty extraction under the current fingerprint is permanent silent data loss. `FallbackSearchClient.search_all(stop_after=…)` stops issuing paid queries once enough unique URLs are collected. `search.dataforseo_mode: standard` (opt-in) uses the 70%-cheaper task queue with minutes of latency.

**Cache**: everything goes through `cache.py` (a thin facade over `db.py`). Each scraped URL gets a row in `cache_pages`. The extraction cache is fingerprint-keyed: SHA-256[:12] of `SYSTEM_PROMPT + model_name`. Changing either invalidates all cached extractions automatically.

**Web app** (`web/app.py`): single FastAPI app serving two domains from one container. Public router (`_fastapi`) and admin router (`admin`, gated by `_BasicAuth` ASGI middleware). `_detect_site(request)` reads the `Host` header and returns `"meetapedia"` or `"kozossegek"`. `lang_context(request)` injects site-aware variables (`site`, `site_name`, `site_url`, `lang`, `locale`, `map_url`, `about_url`, `explore_url`, `submit_url`, `map_center`) into every public template. `_site_cities(request)` filters cities by domain (HU-only vs. all). Shared runtime state lives in `web/state.py:app_state` singleton.

`app_state.cities` and `app_state.topics` are **dataclass objects** — always use `city.name`, `city.country` (NOT `city["name"]`). Dict-style access causes a 500 on any route that touches them.

`app_state.current_city` / `current_topic` are set by the `on_pair_start` callback during pipeline runs (cleared in `finally`). Consumed by `/admin/api/coverage/current` for the live jump-to-active feature.

## Key Files

| File | Purpose |
|---|---|
| `scraper/main.py` | Entry point: scheduler + uvicorn |
| `scraper/pipeline.py` | Run orchestration |
| `scraper/extract.py` | LLM prompts, schemas, extractors |
| `scraper/db.py` | All SQLite access; `init_db()` is safe to call repeatedly |
| `scraper/models.py` | `CommunityRecord` pydantic model with auto-cleanup validator |
| `scraper/web/app.py` | All HTTP routes (~3700 lines) |
| `scraper/duplicates.py` | Duplicate detection; admin UI at `/admin/duplicates` |
| `scraper/playwright_fetch.py` | Playwright-based page fetcher; `playwright_domains` in `settings.yaml` is currently empty (social domains are blocked, not Playwright-fetched) |
| `scraper/false_positives.py` | CRUD + prompt injection for false positive rules |
| `scraper/web/schema.py` | JSON-LD schema generation for public pages |
| `scraper/web/i18n.py` | Translations; `lang_context(request)` injects `t`, `lang`, `topic_labels` etc. into every public template |
| `config/cities.yaml` | City list: `name`, `country`, `locale`, `search_variants` |
| `config/topics.yaml` | Topic list: `name`, per-locale `search_terms` |
| `config/settings.yaml` | Model/API/cache/schedule config |
| `scraper/web/templates/coverage.html` | City×topic matrix; JS class toggle (`.active-row`, `.active-topic`) drives live cell states — use CSS `<style>` block, not Tailwind, for JS-dynamic styles |
| `docs/wiki/` | LLM wiki (Karpathy pattern): hacks, post-mortems, decisions, architecture. Ingest a source doc → Claude creates/updates pages + updates `index.md` + appends `log.md` |

## Important Patterns

**i18n**: all public templates receive `t('key')` via `lang_context(request)`. Translation keys live in `i18n.py` in two dicts (English base, then per-language overrides merged on top). New keys need both English (required) and Hungarian (primary market). Missing keys fall back to English silently.

**Database init**: `db.py:init_db()` uses `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ... ADD COLUMN` guards everywhere. It is safe to call on every request. Call it at the start of any route that touches a table that might not exist on older production DBs.

**Jinja2 macros**: macros must be defined **before** they are called in templates. Jinja2 does not hoist macro definitions. Defining a macro after its call site causes `UndefinedError` at render time — silently if the calling branch is never reached (e.g., inside `{% if records %}`).

**Topic labels in templates**: `topic_labels` dict comes from `lang_context` (i18n-aware). `TOPIC_LABELS` in `app.py` is the English fallback. Both are compatible; `lang_context` overrides the explicit kwarg if passed last via `**lang_context(request)`.

**Extraction prompt overrides**: `extract.py:get_prompt(key)` checks `_PROMPT_OVERRIDES` first. Admins can edit prompts live from `/admin/prompts`. The fingerprint system means any prompt change triggers re-extraction on next run.

**False positives**: stored in `false_positives` table. `build_prompt_section(all_fps, city, topic)` appends them to the extraction system prompt. Call `get_false_positives(_db())` to load them.

**Community identity**: `community_id` = SHA-256[:12] of `name.lower()|city.lower()`. Stable across re-runs. `record_key` = `norm(name)|norm(city)|norm(topic)` (unique DB key).

**Tailwind CDN scanning**: the CDN JIT scans the full initial DOM before the page becomes visible. Never server-render large lists in admin templates — load them via a JSON endpoint + `DocumentFragment` client-side. The `logs.html` → `/admin/api/logs/history` pattern is the reference.

**Stop/cancel pattern**: long-running routes (pipeline, revalidate) must use `asyncio.create_task()` and store the task in `app_state._run_task`. `BackgroundTasks` (FastAPI) cannot be cancelled. `asyncio.CancelledError` is a `BaseException` in Python 3.8+, so `except Exception` will NOT catch it — always use `finally` for cleanup.

**CSS build**: `scraper/web/static/css/app.css` is gitignored. Docker builds it from `input.css` via `pytailwindcss` at image build time. For local dev, maintain `app.css` manually. Committing `input.css` changes is sufficient for production.

**Playwright vs. blocked ordering**: `fetch_and_clean()` checks `playwright_fetcher.matches(url)` *before* `_is_blocked()`. A domain in both lists gets fetched by Playwright, not blocked. Keep social-media domains out of `playwright_domains` entirely.

**Person + venue extraction skip**: in `_run_full` and `_run_ai_only`, both the person AND venue cache lookups and AI calls are skipped entirely when `community_names` is empty for a URL. No communities → no persons/venues to extract (the majority of URLs yield 0 communities). Venue/person cache read/write uses `canonical_venue_fingerprint` / `canonical_person_fingerprint` (always primaries[0]) so fallback-provider extractions don't re-run when DeepSeek recovers.

**settings.yaml schedule flags**: `schedule.auto_run_on_startup: true/false` controls whether the pipeline runs automatically on deploy/restart (read at startup, not hot-reloaded). The cron job slot exists but is intentionally empty — automatic scheduled runs are disabled.

**Two-domain nav active-state**: nav links in `public_base.html` use `or` prefix checks for both HU and EN paths (e.g. `_p.startswith('/terkep') or _p.startswith('/map')`). Add both prefixes when introducing a new route that exists on both domains.

## Adding Things

**New city**: add to `config/cities.yaml` (name, country, locale, search_variants). Also add coordinates to `CITY_COORDS` dict in `app.py` for the map page.

**New topic**: add to `config/topics.yaml` (name, per-locale search_terms). Add to `TOPIC_ICONS` and `TOPIC_LABELS` dicts in `app.py`. Add label to `get_topic_labels()` in `i18n.py` for each supported language.

**New i18n key**: add to the English dict first, then add Hungarian translation. Other languages fall back to English automatically.

**New DB column**: add `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...` guard inside `init_db()` in `db.py`. The guard makes it safe to deploy without manual migration.

## Deployment

Runs on Coolify (Hetzner) via Docker. Persist only `/app/data` (SQLite) and `/app/config` (YAML edits). Do not mount a volume over the entire `/app/` tree. Required env vars: `ADMIN_PASSWORD`. Optional API keys: `DEEPSEEK_API_KEY`, `DATAFORSEO_LOGIN`, `DATAFORSEO_PASSWORD`. Email notifications (`/subscribe`, `/report-not-community`, `/suggest-edit`, `/claim-community`): `RESEND_API_KEY`, `FEEDBACK_EMAIL` (recipient), `RESEND_FROM` (sender, e.g. `noreply@kozossegek.com`). All optional — missing = silent no-op.
