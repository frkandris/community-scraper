# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run tests (the whole suite passes — nothing to ignore)
PYTHONPATH=. .venv/bin/pytest

# Run a single test file
PYTHONPATH=. .venv/bin/pytest tests/test_store.py

# Lint
.venv/bin/ruff check scraper/ tests/

# Install dev deps
pip install -e ".[dev]"

# Keep AGENTS.md in sync after editing this file (a test enforces it)
PYTHONPATH=. .venv/bin/python scripts/sync_agents_md.py
```

**No local dev environment.** The app runs on Hetzner, managed by Coolify. Do not attempt to start the server locally for verification — read templates and code directly instead. The app is a FastAPI server (uvicorn) on port 8000 in production. `ADMIN_PASSWORD` env var must be set or the admin UI is inaccessible.

## Architecture

The scraper discovers community groups for each `(city, topic)` pair:

1. **Search** (`search.py`): DataForSEO only (`DataForSEOClient`, live or standard mode). Quota errors (`SearchQuotaError`) permanently skip the provider for the run via `FallbackSearchClient` (kept as a single-provider wrapper so a fallback can be re-added with one line).
2. **Fetch** (`fetch.py`): `httpx` + `trafilatura` to extract clean page text. Blocked domains (Facebook, Instagram, TikTok, LinkedIn, YouTube, Reddit, Twitter/X) return `None` immediately. `playwright_domains` in `settings.yaml` controls Playwright-fetched domains (currently empty — the Playwright fetcher is dormant).
3. **Extract** (`extract.py`): `FallbackExtractor` is the single failure path (typed errors, circuit breaker, retry). Its `primaries` list comes from `pipeline.build_extractor()` — **the one place a chain is assembled**. With `router.enabled` in `config/providers.yaml` that is the free-tier fleet ordered best-quality-first and vetoed per provider by the persisted quota ledger; otherwise it is the single `DeepSeekExtractor`.
4. **Store** (`store.py` → `db.py`): Upsert to SQLite `communities` table, merging `source_urls` on conflict.

The full run is orchestrated by `pipeline.py:run_pipeline()`. Modes:
- `full`: search → fetch → extract → enrich (default; labelled "Smart" in the UI)
- `ai_only`: re-extract from cached page texts, no web requests
- `search_only`: search + fetch + cache raw text, zero LLM calls (the saver collector)

(The former `revalidate`, `recategorize`, and description-maintenance admin flows were deleted 2026-07-23 — the focus is low-cost world indexing. Historical `revalidate` rows may still appear in `runs`.)

**Pipeline city priority**: `_saver_city_groups(cities, priority)` in `main.py` (not `pipeline.py`) returns one group per named country plus a trailing "everything else" group. The order comes from `pipeline.country_priority` in `settings.yaml` — **Hungary → Germany → Indonesia → Sweden → rest** since 2026-08-16, when the 1000+ inhabitant import left Hungary with 973 unprocessed settlements on the primary market. Startup crash-recovery uses the same ordering. The window is a hard time box, so a country behind a large backlog may not be reached at all: this list *is* the expansion priority.

**Done-pair pre-filter**: `run_pipeline()` calls `get_fully_processed_pairs(db_path, current_fp)` (one SQL query) before inner loops and passes the complement as `pairs_filter`. Pairs with a `search_cache` entry AND all `cache_pages` at the current `extract_fingerprint` are skipped entirely — no loop iteration, no log entry. Fully-covered cities should not appear in the log.

**Scheduler**: the twin schedule is ON (`schedule.saver_enabled: true`). **Reordered 2026-08-16**: the `ai_only` extractor runs **00:30→10:00 UTC**, immediately after the free-tier quotas reset at 00:00 UTC, and the `search_only` collector (DataForSEO, zero LLM) takes **10:30→23:50**. The old order existed for DeepSeek's off-peak discount; extraction now runs on the free fleet, so starting 16 h after the budget refills wasted it. The 9.5 h extract window is sized from the fleet's ~16.5K daily calls at a serial ~2 s/call ≈ 9.2 h. Runs are boxed by `run_pipeline(stop_at=…)` and stop gracefully; unfinished pairs carry over. `ai_only` loads raw pages one pair at a time; never restore the old whole-cache materialization because it can OOM at production scale. **Description enrichment** (`schedule.enrich_enabled`, `_enrich_run`) moved with it (`enrich_cron` 00:35 → `enrich_until` 10:00), shares the same provider budget via the quota ledger, is bounded and idempotent/resumable (the `long_description` marker), resumes on startup mid-window, and deliberately does NOT reserve the run coordinator (coexists with `ai_only`).

**Topic tiering**: cities with `topic_tier: core` in `cities.yaml` (260 small Swedish kommuner, ~1,976 German Städte under 100k, and 968 Hungarian settlements under 10k added 2026-08-16) only run `pipeline.core_topics` from `settings.yaml`; tiered-out pairs are fully frozen (no search, no re-extraction). `_tier_allows()` in `pipeline.py` is the single gate, also used by `/admin/api/search/jobs`.

**Adding a country**: use `scripts/import_cities.py <country> --min-pop N --apply --write-coords`. It is additive (never rewrites existing entries), resolves accent-fold slug collisions with a `(Region)` suffix, and tiers small settlements. A new locale needs three things or it degrades silently: `search_terms` in `topics.yaml`, an entry in `LOCALE_TO_DATAFORSEO_LOCATION` (`search.py`), and i18n labels.

**Model routing**: `config/providers.yaml` holds the free-tier fleet (Groq, Cerebras, Gemini, Mistral, OpenRouter, GitHub Models) with per-model quality scores; DeepSeek is paid and parked behind `router.allow_paid`. Routing happens **before** generation, never as a cheap-first cascade. **Every model in the fleet shares one `fingerprint_model`** — the fingerprint keys the extraction cache, so letting it vary per model would invalidate ~74K cached extractions. Which model actually ran lives in `cache_pages.extract_model`/`extract_quality`, outside every key; read it with `pipeline._served_by()`, never `extractor.model` (that names the head of the chain and lies after failover). `QuotaLedger` persists per-day spend in `provider_usage` and lowers a provider's ceiling when a 429 proves the published limit wrong. Admin view: `/admin/providers`.

**External LLM gateway**: `/v1/chat/completions` + `/v1/models` + `/v1/quota` (`scraper/web/api.py`) expose the router to other software with the OpenAI wire format. Bearer auth from comma-separated `ROUTER_API_KEY`; **unset = the gateway 401s everything**, never open. Deliberately general purpose — no project prompt or schema is injected. Gateway calls spend the same daily ledger as the crawler.

**Search cost rules**: every *successful* search is saved to `search_cache` — even empty results and Full Refresh runs (an unsaved empty search is re-paid every run). Provider failures raise (`SearchQuotaError` / `ExtractorUnavailableError`) and are NOT cached: the pair/page is skipped with a `search_failed`/`extract_failed` pair-log counter and retried next run. Never convert these errors to empty results — caching an empty extraction under the current fingerprint is permanent silent data loss. `FallbackSearchClient.search_all(stop_after=…)` stops issuing paid queries once enough unique URLs are collected. Production uses `dataforseo_mode: standard` with `standard_priority: 2` (~$1.2/1K, normally ≤1 minute); normal priority may legally exceed the client's 5-minute polling window and must not be used with the current sequential collector.

**Extractor failure rules**: `run_pipeline()` calls `extractor.preflight()` — one live mini-extraction — before any pair loop, so a broken model name or revoked key fails the run immediately instead of one skipped page at a time (`search_only` skips it: no LLM). During a run, `FallbackExtractor` opens a circuit breaker after `_FAILURE_THRESHOLD` (20) *consecutive* failed calls; one success resets the counter. `providers_down` (providers configured but all dead) aborts the run with the reason in the pair log's `extract_error` → run detail banner + daily email. `exhausted` alone must NOT abort — it is also true when no API key is set, which is a deliberate no-LLM run.

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
| `scraper/wrong_city.py` | Wrong-city detection (text mentions another known city); admin UI at `/admin/wrong-city`; both under the "Data quality" nav group |
| `scraper/playwright_fetch.py` | Playwright-based page fetcher; `playwright_domains` in `settings.yaml` is currently empty (social domains are blocked, not Playwright-fetched) |
| `scraper/false_positives.py` | CRUD + prompt injection for false positive rules |
| `scraper/providers.py` | Free-tier LLM provider catalogue loader + generic OpenAI-compatible extractor |
| `scraper/router.py` | Quota-aware model router: `QuotaLedger` (persisted per-day budget) + `ModelRouter` (pre-generation selection) |
| `scraper/web/api.py` | Public OpenAI-compatible gateway at `/v1/*` (Bearer auth via `ROUTER_API_KEY`) |
| `scraper/web/static/js/listing.js` | Shared public-site widgets: accent-insensitive autocomplete + A-Z/free-text list filter |
| `scraper/web/schema.py` | JSON-LD schema generation for public pages |
| `scraper/web/i18n.py` | Translations; `lang_context(request)` injects `t`, `lang`, `topic_labels` etc. into every public template |
| `config/cities.yaml` | City list: `name`, `country`, `locale`, `search_variants` |
| `config/topics.yaml` | Topic list: `name`, per-locale `search_terms` |
| `config/settings.yaml` | Model/API/cache/schedule config; `pipeline.country_priority` orders the saver windows |
| `config/providers.yaml` | Free-tier provider catalogue: models, quality scores, rate limits, router policy |
| `scraper/web/templates/_listing_filter.html` | `filter_bar()` macro — import it and call it above any listing; listing.js auto-wires it |
| `scraper/web/templates/coverage.html` | City×topic matrix; JS class toggle (`.active-row`, `.active-topic`) drives live cell states — use CSS `<style>` block, not Tailwind, for JS-dynamic styles |
| `docs/wiki/` | LLM wiki (Karpathy + OKF + llm-wiki-seed): hacks, post-mortems, decisions, integrations, runbooks |

## LLM Wiki

`docs/wiki/` is the persistent knowledge base. Before non-trivial work, skim
`docs/wiki/index.md` for relevant pages (plus `glossary.md`/`faq.md`). The maintenance
rules — when to capture what, page format, same-commit discipline — live in
`docs/wiki/CLAUDE.md` and `docs/wiki/SCHEMA.md`. Wiki updates land in the **same
commit** as the code change that triggered them; validate with
`PYTHONPATH=. .venv/bin/python scripts/lint_wiki.py` before committing.

## Important Patterns

**i18n**: all public templates receive `t('key')` via `lang_context(request)`. Translation keys live in `i18n.py` in two dicts (English base, then per-language overrides merged on top). New keys need both English (required) and Hungarian (primary market). Missing keys fall back to English silently.

**Database init**: `db.py:init_db()` uses `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ... ADD COLUMN` guards everywhere. It is safe to call on every request. Call it at the start of any route that touches a table that might not exist on older production DBs.

**Jinja2 macros**: macros must be defined **before** they are called in templates. Jinja2 does not hoist macro definitions. Defining a macro after its call site causes `UndefinedError` at render time — silently if the calling branch is never reached (e.g., inside `{% if records %}`).

**Topic labels in templates**: `topic_labels` dict comes from `lang_context` (i18n-aware). `TOPIC_LABELS` in `app.py` is the English fallback. Both are compatible; `lang_context` overrides the explicit kwarg if passed last via `**lang_context(request)`.

**Extraction prompt overrides**: `extract.py:get_prompt(key)` checks `_PROMPT_OVERRIDES` first. Admins can edit prompts live from `/admin/prompts`. The fingerprint system means any prompt change triggers re-extraction on next run.

**False positives**: stored in `false_positives` table. `build_prompt_section(all_fps, city, topic)` appends them to the extraction system prompt. Call `get_false_positives(_db())` to load them.

**Community identity**: `community_id` = SHA-256[:12] of `name.lower()|city.lower()`. Stable across re-runs. `record_key` = `norm(name)|norm(city)|norm(topic)` (unique DB key).

**Tailwind CDN scanning**: the CDN JIT scans the full initial DOM before the page becomes visible. Never server-render large lists in admin templates — load them via a JSON endpoint + `DocumentFragment` client-side. The `logs.html` → `/admin/api/logs/history` pattern is the reference.

**Public listing UX**: every listing page (cities, venues, people, explore) sorts alphabetically server-side and imports `_listing_filter.html` for the shared A-Z bar + free-text filter. `/static/js/listing.js` auto-attaches to each `[data-mp-filter]` block, so listing pages carry no JS of their own; items opt in with `data-name="Real Name"` (the raw name — `MpText.norm()` folds accents and the A-Z bar needs the original first letter). Two traps: `hidden` alone loses to any Tailwind `display` utility, so `public_base.html` declares `[hidden] { display: none !important; }`; and `listing.js` is `defer`red, so page scripts using `MpAutocomplete` must run inside `DOMContentLoaded`. Never use `<datalist>` for suggestions — iOS Safari ignores it entirely.

**Stop/cancel pattern**: long-running routes (pipeline runs) must use `asyncio.create_task()` and store the task in `app_state._run_task`. `BackgroundTasks` (FastAPI) cannot be cancelled. `asyncio.CancelledError` is a `BaseException` in Python 3.8+, so `except Exception` will NOT catch it — always use `finally` for cleanup.

**CSS build**: `scraper/web/static/css/app.css` is gitignored. Docker builds it from `input.css` via `pytailwindcss` at image build time. For local dev, maintain `app.css` manually. Committing `input.css` changes is sufficient for production.

**Playwright vs. blocked ordering**: `fetch_and_clean()` checks `playwright_fetcher.matches(url)` *before* `_is_blocked()`. A domain in both lists gets fetched by Playwright, not blocked. Keep social-media domains out of `playwright_domains` entirely.

**Person + venue extraction skip**: in `_run_full` and `_run_ai_only`, both the person AND venue cache lookups and AI calls are skipped entirely when `community_names` is empty for a URL. No communities → no persons/venues to extract (the majority of URLs yield 0 communities). Venue/person cache read/write uses `canonical_venue_fingerprint` / `canonical_person_fingerprint` (always primaries[0]) so fallback-provider extractions don't re-run when DeepSeek recovers.

**settings.yaml schedule flags**: `schedule.auto_run_on_startup` (read at startup, not hot-reloaded) is **on** — but startup is only a *crash-recovery net*, not a driver. `_startup_plan()` in `main.py` decides: under the saver schedule, a mid-window deploy/restart that interrupted a `search_only`/`ai_only` run resumes that same mode **boxed to its window** (`search_until`/`extract_until`); a clean boot (last run succeeded) does **nothing** — the twin crons drive the day and startup must never launch a `full` (LLM) run outside the twin-window split. When `saver_enabled` is off, the legacy escalation (`ai_only → full`, unbounded) is preserved. This exists because deploys during the long collector window were silently truncating that day's collection (2026-07-24/25). The legacy combined cron slot (`cron_enabled`) is still off.

**Two-domain nav active-state**: nav links in `public_base.html` use `or` prefix checks for both HU and EN paths (e.g. `_p.startswith('/terkep') or _p.startswith('/map')`). Add both prefixes when introducing a new route that exists on both domains.

## Adding Things

**New city**: add to `config/cities.yaml` (name, country, locale, search_variants). Also add coordinates to `CITY_COORDS` dict in `app.py` for the map page.

**New topic**: add to `config/topics.yaml` (name, per-locale search_terms). Add to `TOPIC_ICONS` and `TOPIC_LABELS` dicts in `app.py`. Add label to `get_topic_labels()` in `i18n.py` for each supported language.

**New i18n key**: add to the English dict first, then add Hungarian translation. Other languages fall back to English automatically.

**New DB column**: add `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...` guard inside `init_db()` in `db.py`. The guard makes it safe to deploy without manual migration.

## Deployment

Runs on Coolify (Hetzner) via Docker. Persist only `/app/data` (SQLite) and `/app/config` (YAML edits). Do not mount a volume over the entire `/app/` tree. Required env vars: `ADMIN_PASSWORD`. Optional API keys: `DEEPSEEK_API_KEY`, `DATAFORSEO_LOGIN`, `DATAFORSEO_PASSWORD`. Email notifications (`/subscribe`, `/report-not-community`, `/suggest-edit`, `/claim-community`): `RESEND_API_KEY`, `FEEDBACK_EMAIL` (recipient), `RESEND_FROM` (sender, e.g. `noreply@kozossegek.com`). All optional — missing = silent no-op.
