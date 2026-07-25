# Meetapedia — Project Reference (ARCHIVED, May 2026)

> **⚠️ OUT OF DATE — DO NOT TRUST THIS FILE.** It describes the system as of May 2026
> and was never updated. Providers listed below (Serper, Brave, SearXNG, Groq, Ollama)
> were all removed in July 2026; the run modes, scheduling and admin flows described
> here no longer exist.
>
> **Current sources of truth:** [`README.md`](README.md) (what the project is),
> [`CLAUDE.md`](CLAUDE.md) (architecture + patterns for anyone changing code), and
> [`docs/wiki/`](docs/wiki/index.md) (the maintained knowledge base).
>
> Kept only for historical context; safe to delete.

---

## 1. What This Project Does

**Community Scraper** is a self-hosted web scraper that automatically discovers local
community groups (running clubs, choirs, board-game nights, yoga circles, etc.) in cities
around the world. For each `(city, topic)` pair it:

1. Generates search queries in the city's language.
2. Searches the web (Serper → Brave → SearXNG fallback chain).
3. Fetches and cleans web pages (trafilatura).
4. Runs an LLM extraction pass (DeepSeek → Groq → Ollama fallback chain).
5. Saves structured community records to a SQLite database.
6. Serves the data through a bilingual public website and a password-protected admin UI.

The entire stack is self-hosted and runs on Coolify (Hetzner). No paid cloud required
except optional API keys for Serper/Brave/DeepSeek/Groq (all have free tiers).

---

## 2. Repository Layout

```
community-scraper/
├── scraper/
│   ├── __init__.py
│   ├── main.py           # Entry point: scheduler + uvicorn server
│   ├── pipeline.py       # Orchestrates one full scrape run
│   ├── search.py         # Search clients (Serper, Brave, SearXNG) + FallbackSearchClient
│   ├── fetch.py          # HTTP fetch + trafilatura text extraction
│   ├── extract.py        # LLM extractors (Ollama, DeepSeek, Groq) + FallbackExtractor
│   ├── cache.py          # CacheManager: scrape + extract cache over SQLite
│   ├── db.py             # All SQLite access (init, CRUD for every table)
│   ├── store.py          # Merge + upsert community records to the communities table
│   ├── models.py         # Pydantic models: CommunityRecord, SearchResult, RunMetadata
│   ├── false_positives.py # False-positive management (mark wrong results, inject into prompt)
│   ├── vcs.py            # Optional git auto-commit after each run
│   ├── migrate_json.py   # One-off migration from old JSON files to SQLite
│   └── web/
│       ├── __init__.py
│       ├── app.py        # FastAPI app: all HTTP routes (public + admin)
│       ├── i18n.py       # Translations: EN / HU / DE
│       ├── log_stream.py # In-memory log broadcaster for SSE log tail
│       ├── schema.py     # JSON-LD schema.org generation
│       ├── state.py      # app_state singleton (shared runtime state)
│       ├── static/
│       │   └── css/input.css   # Tailwind source (compiled externally)
│       └── templates/          # Jinja2 HTML templates
│           ├── public_home.html
│           ├── public_explore.html
│           ├── public_community.html   # Community detail page
│           ├── public_about.html
│           ├── public_map.html
│           ├── public_base.html
│           ├── public_unsubscribe.html
│           ├── base.html               # Admin base
│           ├── dashboard.html
│           ├── results.html
│           ├── result_detail.html
│           ├── cache.html
│           ├── cache_detail.html
│           ├── config.html
│           ├── logs.html
│           ├── history.html
│           ├── history_detail.html
│           ├── run_detail.html
│           ├── prompts.html
│           └── subscriptions.html
├── config/
│   ├── cities.yaml       # City list with locale + search variants
│   ├── topics.yaml       # Topic list with multilingual search terms
│   └── settings.yaml     # Runtime config (models, timeouts, schedule, cache)
├── data/                 # Created at runtime; contains scraper.db
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── tailwind.config.js
```

---

## 3. Data Model — `CommunityRecord`

Defined in `scraper/models.py`. This is the core entity.

```python
class CommunityRecord(BaseModel):
    # Required
    name: str
    topic: str
    city: str
    locale: str              # city's language code, e.g. "hu", "en", "de"
    source_url: str          # page where this community was found
    extracted_at: str        # ISO-8601 timestamp

    # Identity
    community_id: str = ""   # SHA-256[:12] of "name.lower()|city.lower()"
    source_urls: list[str]   # all pages that mention this community (auto-merged)

    # Core info
    description: str | None = None
    meeting_schedule: str | None = None
    location: str | None = None
    website: str | None = None
    social_links: list[str] = []
    contact: str | None = None
    email: str | None = None
    phone: str | None = None

    # Quality
    confidence: float | None = None   # 0–1 LLM self-score
    joinable: bool = True             # pipeline filters out joinable=False records

    # Extended profile
    founding_year: int | None = None
    member_count: str | None = None
    fee: str | None = None
    age_range: str | None = None
    skill_level: str | None = None
    join_process: str | None = None
    leader: str | None = None
    tags: list[str] = []              # max 8, deduplicated
    language: str | None = None       # spoken language of the group
    history: str | None = None        # 1–3 sentence backstory
    frequency: str | None = None      # meeting regularity (e.g. "Heti", "Weekly")
```

### Model Validator (auto-cleanup)

The `_clean_and_generate_id` validator runs after every instantiation:

- Nulls any text field that contains a "null-ish" string:
  `"nincs megadva"`, `"n/a"`, `"nem ismert"`, `"unknown"`, `"none"`,
  `"not provided"`, `"not available"`, `"-"`, `"–"`, `"na"`, `"ismeretlen"`,
  `"keine angabe"`, `"unbekannt"`.
  Applied to: `phone, contact, location, meeting_schedule, description, fee,
  history, frequency, leader, join_process, skill_level, age_range, language`.
- Normalises `website`: prepends `https://` if no scheme.
- Filters `social_links` to only actual `http://` or `https://` URLs.
- Nulls `email` if it contains no `@`.
- Nulls `phone` if it contains no digit (prevents `tel:Nincs megadva` links).
- Deduplicates and caps `tags` at 8.
- Ensures `source_url` is the first entry of `source_urls`.
- Generates `community_id` if empty.

---

## 4. Configuration Files

### `config/cities.yaml`

Each city has:
- `name`: canonical English name used as the database key.
- `country`: human-readable country name.
- `locale`: 2-letter language code (`hu`, `en`, `de`, `fr`, `es`, …). Controls which
  search terms are used and which language SearXNG / Serper searches in.
- `search_variants`: list of name variants to use in queries (handles accented chars,
  common abbreviations, country suffix for disambiguation).

Currently ~130 cities across Europe, Americas, Asia, Africa, Oceania.

**Test mode**: set `pipeline.test_mode: true` in `settings.yaml` and list cities under
`pipeline.test_cities` to restrict runs to a small subset.

### `config/topics.yaml`

Each topic has:
- `name`: snake_case identifier (e.g. `board_games`, `language_exchange`).
- `search_terms`: dict mapping locale → list of search terms.

The pipeline picks terms for `city.locale`; falls back to `en` if the locale is missing.

Currently 27 topics: running, board_games, choir, dance, cycling, hiking, yoga,
photography, book_club, chess, cooking, theater, music, martial_arts, gaming,
volunteering, language_exchange, art, meditation, swimming, community_general,
gardening, film_club, trivia, sustainability, crafts, fitness.

### `config/settings.yaml`

```yaml
ollama:
  model: gemma3:4b          # local fallback model
  temperature: 0.1
  timeout_seconds: 180
  max_text_chars: 6000       # page text truncation before sending to LLM

deepseek:
  model: deepseek-chat
  temperature: 0.1
  timeout_seconds: 60
  max_text_chars: 8000
  rate_limit_seconds: 1.0

groq:
  model: llama-3.3-70b-versatile
  temperature: 0.1
  timeout_seconds: 60
  max_text_chars: 3000
  rate_limit_seconds: 7.0

search:
  results_per_query: 10
  max_pages_per_topic: 5    # max URLs to fetch per (city, topic) pair
  rate_limit_seconds: 1.5

fetch:
  timeout_seconds: 15
  min_text_length: 100      # discard pages with fewer characters
  max_concurrent: 3
  blocked_domains:          # never fetched (no useful text)
    - facebook.com
    - instagram.com
    - twitter.com
    - x.com
    - tiktok.com
    - youtube.com
    - linkedin.com

pipeline:
  commit_after_run: false
  push_after_commit: false
  test_mode: false
  test_cities: [Szentendre, Budapest, London]
  enrich_communities: true  # run enrichment pass for records without contact info

cache:
  skip_scraped: true        # reuse cached page text instead of re-fetching
  skip_extracted: true      # reuse cached extraction results (fingerprint-checked)
  search_ttl_days: 7        # how long search URL lists are cached per city+topic

schedule:
  cron: "*/15 * * * *"      # overridden by SCHEDULE_CRON env var
```

---

## 5. Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `SEARXNG_URL` | SearXNG base URL | `http://localhost:8080` |
| `OLLAMA_URL` | Ollama base URL | `http://localhost:11434` |
| `SERPER_DEV_API_KEY` | Serper.dev API key (primary search) | *(empty = disabled)* |
| `BRAVE_API_KEY` | Brave Search API key (secondary search) | *(empty = disabled)* |
| `DEEPSEEK_API_KEY` | DeepSeek API key (primary extractor) | *(empty = disabled)* |
| `GROQ_API_KEY` | Groq API key (secondary extractor) | *(empty = disabled)* |
| `SCHEDULE_CRON` | Cron expression for scheduled runs | from `settings.yaml` |
| `ADMIN_USER` | Basic-auth username for `/admin/*` | `admin` |
| `ADMIN_PASSWORD` | Basic-auth password for `/admin/*` | Required |
| `FEEDBACK_EMAIL` | Email address for feedback notifications | *(empty = disabled)* |
| `RESEND_API_KEY` | Resend API key for feedback emails | *(empty = disabled)* |
| `RESEND_FROM` | From address for Resend | `onboarding@resend.dev` |
| `GIT_USER_NAME` | Git author name for auto-commits | — |
| `GIT_USER_EMAIL` | Git author email for auto-commits | — |
| `GIT_TOKEN` | GitHub PAT for optional push | — |

---

## 6. SQLite Database Schema (`data/scraper.db`)

Initialised by `scraper/db.py:init_db()`. All tables use `ALTER TABLE … ADD COLUMN`
guards so re-running init on an existing DB is safe.

### `communities`
One row per unique `(name, city, topic)` combination (normalised: lowercase, non-alphanums
replaced by `_`).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `record_key` | TEXT UNIQUE | `norm(name)|norm(city)|norm(topic)` |
| `community_id` | TEXT | SHA-256[:12] of `name.lower()|city.lower()` |
| `city` | TEXT | |
| `topic` | TEXT | |
| `data` | TEXT | Full `CommunityRecord` serialised as JSON |
| `updated_at` | TEXT | ISO-8601 |

Indices: `(city, topic)`, `community_id`.

When updating an existing record, `source_urls` are merged (new + old, deduped).

### `cache_pages`
One row per scraped URL. Stores both the raw text and the extracted records JSON.

| Column | Type | Notes |
|---|---|---|
| `url_hash` | TEXT PK | SHA-256[:16] of the URL |
| `url` | TEXT | |
| `city` | TEXT | |
| `topic` | TEXT | |
| `domain` | TEXT | `urlparse(url).netloc` |
| `scraped_at` | TEXT | ISO-8601, set when raw text saved |
| `extracted_at` | TEXT | ISO-8601, set when extraction done |
| `extract_fingerprint` | TEXT | SHA-256[:12] of `SYSTEM_PROMPT + model` |
| `data` | TEXT | Full cache entry JSON (includes `raw_text`, `records`, timings, …) |

The JSON stored in `data` has additional dynamic keys:
- `raw_text`: cleaned page text
- `records`: list of `CommunityRecord.model_dump()` dicts
- `scrape_duration_s`, `extract_duration_s`
- `enrich_scraped_at`, `enrich_scrape_duration_s`
- `enrich_extracted_at`, `enrich_extract_duration_s`, `enrich_count`
- `enrich_log`: per-record enrichment details
- `source_queries`: which queries led to this URL
- `extract_model`, `enrich_model`

### `runs`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `started_at` | TEXT | |
| `finished_at` | TEXT | |
| `run_mode` | TEXT | `"full"` or `"ai_only"` |
| `success` | INTEGER | 0 or 1 |
| `search_log` | TEXT | JSON array of `pair_log` dicts per (city, topic) |

### `subscriptions`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `email` | TEXT | Lowercased |
| `city` | TEXT | |
| `topic` | TEXT | |
| `token` | TEXT UNIQUE | UUID4 for unsubscribe link |
| `created_at` | TEXT | |

Unique index on `(email, city, topic)`.

### `false_positives`
| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `name` | TEXT | Community name |
| `city` | TEXT | |
| `topic` | TEXT | |
| `reason` | TEXT | Human note |
| `source_url` | TEXT | |
| `fp_type` | TEXT | `"extraction"` or `"enrichment"` |
| `marked_at` | TEXT | |

Unique on `(name, city, topic, fp_type)`. False positives are injected at the end of
the extraction/enrichment system prompt as a "do not extract these" list.

### `prompt_history`
Snapshots of the full system prompt (including FP section) each time the FP list changes.
Used to render a diff view in the admin `/prompts` page.

### `search_cache`
| Column | Type | Notes |
|---|---|---|
| `city` | TEXT | |
| `topic` | TEXT | |
| `urls` | TEXT | JSON array of URLs |
| `queries` | TEXT | JSON array of queries that produced this |
| `cached_at` | TEXT | ISO-8601 |

Primary key `(city, topic)`. TTL enforced at read time: `cached_at >= now - ttl_days`.
Used by Smart runs to skip the search API entirely.

---

## 7. Search Layer — `scraper/search.py`

### Provider Chain

```
SerperSearchClient  →  BraveSearchClient  →  SearXNGClient
```

- `FallbackSearchClient` wraps a `primaries: list` and a `fallback`.
- Each primary has a `_exhausted: list[bool]` flag per index.
- On `SearchQuotaError`, the provider is permanently marked exhausted for the lifetime
  of the `FallbackSearchClient` instance.
- `SearXNGClient` is always the final fallback (self-hosted, no quota).

### `SearchQuotaError`

Raised by Brave and Serper on:
- HTTP 402 or 429.
- HTTP 400 with "credit" or "quota" in the response body (Serper-specific quirk).

### Query Building — `build_queries()`

For a city with `search_variants = ["Kecskemét", "Kecskemet"]` and topic terms
`["futás", "futóklub"]`:

```python
queries = [
    "futás Kecskemét",     # first term × primary variant
    "futóklub Kecskemét",  # second term × primary variant
    "futás Kecskemet",     # first term × second variant
]
```

Maximum 3 queries per (city, topic) pair.

### Provider Details

**SerperSearchClient** (`https://google.serper.dev/search`)
- POST with JSON body `{q, num, gl, hl}`.
- `gl`/`hl` derived from `LOCALE_TO_SERPER` dict (17 locales).
- Header: `X-API-KEY`.
- Returns `organic[].{link, title, snippet}`.

**BraveSearchClient** (`https://api.search.brave.com/res/v1/web/search`)
- GET with params `{q, count, country, search_lang}`.
- Country from `LOCALE_TO_BRAVE_COUNTRY` dict.
- Header: `X-Subscription-Token`.

**SearXNGClient**
- GET `{base_url}/search?q=...&format=json&language={locale}`.
- `language` from `LOCALE_TO_LANGUAGE` dict (e.g. `hu` → `hu-HU`).

### Search Cache (`search_cache` table)

When `skip_scraped=True` (Smart or Re-AI mode) and `search_ttl_days > 0`:
1. Check `search_cache` for `(city, topic)` within TTL.
2. If hit → reuse URLs; skip the API call entirely.
3. If miss → call search APIs → save result to `search_cache`.

Full Refresh mode (`skip_scraped=False`) always bypasses the cache.

---

## 8. Fetch Layer — `scraper/fetch.py`

`fetch_and_clean(url, blocked_domains, timeout, min_text_length, semaphore) → str | None`

- Checks `blocked_domains` before fetching (returns `None` immediately for Facebook etc.).
- Uses `httpx.AsyncClient` with a 15 s timeout and a realistic User-Agent header.
- `asyncio.Semaphore` limits concurrent fetches (default: 3).
- Extracts clean text with `trafilatura.extract()`.
- Falls back to `html2text` if trafilatura returns nothing.
- Returns `None` if text is shorter than `min_text_length` (default: 100 chars).

---

## 9. Extraction Layer — `scraper/extract.py`

### LLM Chain

```
DeepSeekExtractor  →  GroqExtractor  →  OllamaExtractor
```

`FallbackExtractor` wraps `primaries: list` and a `fallback: OllamaExtractor`.

**Per-primary state:**
- `_exhausted[i]: bool` — permanent; set on `ExtractorQuotaError` (HTTP 402).
- `_blocked_until[i]: float` — temporary; set on `ExtractorRateLimitError` (HTTP 429,
  respects `Retry-After` header, defaults to 60 s).

### `OllamaExtractor`

- POST `{ollama_url}/api/chat` with structured output via `"format": EXTRACTION_SCHEMA`.
- Ollama enforces the JSON schema grammar-level; output is always valid JSON.
- `model_fingerprint`: SHA-256[:12] of `SYSTEM_PROMPT + model_name`.
  Used as cache invalidation key — if the prompt or model changes, old cached
  extractions are re-run automatically.

### `_ApiExtractor` (base for DeepSeek and Groq)

- POST `{BASE_URL}/chat/completions` (OpenAI-compatible).
- `response_format: {"type": "json_object"}` — model is asked to return JSON.
- Response parsed from `choices[0].message.content`.
- Rate limiting: `asyncio.sleep()` between calls using `_last_request_time`.
- `ExtractorQuotaError` on HTTP 402.
- `ExtractorRateLimitError` on HTTP 429.
- Other 4xx/5xx → logs warning, returns `{}` (graceful degradation).

### System Prompt

`SYSTEM_PROMPT` instructs the model to:
- Extract only genuine ongoing community groups (not events, businesses, or venues).
- Set `joinable=false` for professional/competitive groups, paid courses, or one-time
  events.
- Rate confidence 0.1–0.9.
- Output field values in the **original language of the page** (not translated).
- Extract 20 fields total (see `CommunityRecord` above).

At the end of the system prompt, any false positives for the current (city, topic) pair
are appended as: "Do not extract these — they have been verified as non-joinable: …"

### Enrichment Pass

After initial extraction, records that have neither `website`, `social_links`, nor
`contact` are enriched:

1. Search for `"«name»" «city»` (3 results).
2. Fetch the top 2 results.
3. Send each page to `extractor.enrich(record, page_text)`.
4. `ENRICH_SYSTEM_PROMPT` asks for: `website, contact, social_links, email, phone`.
5. If any new field is found, update the record (`model_copy(update=…)`).
6. Enrichment is skipped if `enrich_communities: false` in settings.

---

## 10. Pipeline — `scraper/pipeline.py`

### `run_pipeline(cities, topics, config, cache, run_mode, skip_scraped, skip_extracted)`

Two modes:

**`run_mode="full"` (`_run_full`)**

For every `(city, topic)`:
1. Build queries from `topic.search_terms[city.locale]`.
2. Check search cache (if `skip_scraped=True` and `search_ttl_days > 0`).
3. Search (Serper → Brave → SearXNG).
4. For each URL:
   a. Check scrape cache (`cache.get_scraped(url)` if `skip_scraped`).
   b. `fetch_and_clean(url, …)` if not cached.
   c. `cache.save_scraped(url, …)`.
5. For each (url, text):
   a. Check extraction cache (`cache.get_extracted(url, fingerprint=…)` if `skip_extracted`).
   b. `extractor.extract(text, city, topic, locale, source_url)` if not cached.
   c. Filter to `joinable=True` records.
   d. Enrichment pass (if `enrich_communities=True`).
   e. `cache.save_extracted(url, records, …)`.
   f. `save_results(city, topic, records, db_path)`.
6. Logs aggregate run completion metadata.

**`run_mode="ai_only"` (`_run_ai_only`)**

Re-processes all pages already in the scrape cache without any web requests:
- Loads all `(url, raw_text, city, topic)` from cache.
- Runs extraction (respects extraction cache if `skip_extracted=True`).
- No search, no fetching, no enrichment.

### `PipelineConfig`

All tunable parameters as a frozen dataclass:

```
searxng_url, ollama_url, ollama_model, ollama_temperature, ollama_timeout,
ollama_max_text_chars,
search_results_per_query, search_max_pages, search_rate_limit,
fetch_timeout, fetch_min_text_length, fetch_max_concurrent, fetch_blocked_domains,
db_path,
brave_api_key, serper_api_key,
deepseek_api_key, deepseek_model, deepseek_temperature, deepseek_timeout,
  deepseek_max_text_chars, deepseek_rate_limit_seconds,
groq_api_key, groq_model, groq_temperature, groq_timeout,
  groq_max_text_chars, groq_rate_limit_seconds,
cache_skip_scraped, cache_skip_extracted, search_cache_ttl_days,
enrich_communities
```

---

## 11. Cache Layer — `scraper/cache.py`

`CacheManager` is a thin facade over `scraper/db.py` cache_pages functions.

### Scrape cache

- `get_scraped(url) → str | None` — returns `raw_text` from `cache_pages`.
- `save_scraped(url, text, city, topic, duration_s, source_queries)`.

### Extraction cache

- `get_extracted(url, fingerprint) → list[CommunityRecord] | None`
  Returns `None` if: no entry, no `extracted_at`, missing `records`, or fingerprint mismatch.
  Fingerprint mismatch triggers re-extraction (prompt or model changed).
- `save_extracted(url, records, duration_s, fingerprint, model)`.

### Enrichment markers (timing + audit)

- `mark_enrich_scraped(url, duration_s)` — records when the enrichment search was done.
- `mark_enrich_extracted(url, count, duration_s, model)` — records when enrichment extraction finished.
- `save_enrich_log(url, enrich_log)` — per-record audit of what was found.

### URL hash

`SHA-256(url)[:16]` — consistent lookup key across all cache operations.

---

## 12. Store — `scraper/store.py`

`save_results(city, topic, records, db_path) → int`

Calls `db.bulk_upsert_communities()`. On conflict (`record_key` unique), merges
`source_urls` lists (union of new + old, deduped) and updates the data JSON.

Runtime totals are now read from SQLite directly by the dashboard instead of a legacy
`metadata.json` file.

---

## 13. Web Application — `scraper/web/app.py`

### Auth

`_BasicAuth` is a pure ASGI middleware (not a FastAPI dependency) that gates all
`/admin/*` paths with HTTP Basic auth. It deliberately does NOT buffer SSE responses.

Credentials: `ADMIN_USER` / `ADMIN_PASSWORD` env vars. `ADMIN_USER` defaults to
`admin`; `ADMIN_PASSWORD` must be set before the admin UI can be used.

### Public Routes

| Path | Description |
|---|---|
| `GET /` | Home page with city selector and topic stats |
| `GET /explore?city=&topic[]=` | Explore by city / topic filter |
| `GET /{city_slug}` | City overview page (all topics) |
| `GET /{city_slug}/{topic}` | City + topic listing |
| `GET /{city_slug}/{community_slug}` | Community detail page |
| `GET /community/{community_id}` | Legacy ID-based URL → 301 redirect |
| `GET /map` | World map with city counts (Leaflet.js) |
| `GET /about` | Project info page |
| `POST /subscribe` | Email subscription |
| `GET /unsubscribe?token=` | Unsubscribe |
| `POST /feedback` | Community feedback form (sends email via Resend) |
| `GET /api/city-topics?city=` | JSON: per-topic counts for a city |
| `GET /set-lang?lang=&next=` | Set language cookie (`lang`) |

**URL routing logic** in `public_city_segment`:
1. If `segment` is a known topic name → render topic listing.
2. Otherwise → treat as community slug → `_find_community_by_slug`.
3. If nothing found → redirect to city page.

**Slug generation** (`_slugify`): NFKD normalise → ASCII encode → lowercase → replace
non-alphanums with `-`. E.g. `"Futóklub Pécs"` → `"futokub-pecs"`.

### Jinja2 Filters

| Filter | Purpose |
|---|---|
| `slugify` | URL-safe ASCII slug |
| `urlencode` | percent-encode |
| `fmt_dur` | float seconds → `"1m 23s"` |
| `valid_url(url)` | Returns True if URL has http(s) scheme, a dot in the host, no spaces or %20 |
| `link_meta(url)` | Returns `{label, icon, color}` dict for known platforms |

**`link_meta` platforms**: Facebook, Instagram, X, YouTube, LinkedIn, Meetup, Telegram,
Discord, WhatsApp, TikTok, GitHub, Linktree. Falls back to domain name + globe icon.

**`valid_url` purpose**: prevents AI-hallucinated URLs (like `xn--benk-dombra%20futk-t1bn`)
from appearing as clickable links on the public site.

### Admin Routes (`/admin/*`)

| Path | Description |
|---|---|
| `GET /admin/` | Dashboard: stats, run trigger, history |
| `GET /admin/results` | Per-(city, topic) community counts |
| `GET /admin/results/{city}/{topic}` | Community list with false-positive controls |
| `GET /admin/cache` | Scraped page cache index |
| `GET /admin/cache/{url_hash}` | Cache entry detail with prompts, records, enrich log |
| `POST /admin/cache/{url_hash}/run-scrape` | Re-scrape one URL (enqueued) |
| `POST /admin/cache/{url_hash}/run-extract` | Re-extract one URL (enqueued) |
| `POST /admin/cache/{url_hash}/run-enrich` | Re-enrich one URL (enqueued) |
| `POST /admin/cache/{url_hash}/delete-scraped` | Clear raw text only |
| `POST /admin/cache/{url_hash}/delete-extracted` | Clear extraction only |
| `POST /admin/cache/{url_hash}/delete` | Delete entire entry |
| `POST /admin/cache/clear-all` | Wipe all cache + all communities |
| `POST /admin/cache/queue-extract-all` | Bulk enqueue extraction for unextracted pages |
| `POST /admin/api/run` | Trigger a pipeline run |
| `POST /admin/api/stop` | Cancel running task |
| `GET /admin/api/status` | `{is_running, last_run_at}` JSON |
| `GET /admin/api/queue` | Running + pending queue items JSON |
| `GET /admin/api/progress` | Current phase + URL |
| `GET /admin/api/logs/stream` | SSE log tail |
| `GET /admin/api/cache-entries` | Fresh cache index JSON |
| `GET /admin/api/test-searxng` | Test SearXNG connectivity |
| `GET /admin/config` | Edit cities.yaml / topics.yaml / settings.yaml |
| `POST /admin/config/cities` | Save cities.yaml |
| `POST /admin/config/topics` | Save topics.yaml |
| `POST /admin/config/settings` | Save settings.yaml |
| `GET /admin/prompts` | System prompt view with FP diff history |
| `POST /admin/false-positive/add` | Mark a community as false positive |
| `POST /admin/false-positive/remove` | Remove FP marking |
| `GET /admin/subscriptions` | List email subscribers |
| `GET /admin/logs` | Live log tail |
| `GET /admin/runs/{run_id}` | Per-run detail with pair logs |

### Dashboard Run Modes

The dashboard presents 3 preset cards:

| Card | `run_mode` | `skip_scraped` | `skip_extracted` | Effect |
|---|---|---|---|---|
| **Smart** | `full` | `on` | `on` | Uses all caches; only fetches/extracts new content |
| **Full Refresh** | `full` | off | off | Ignores all caches; complete rescrape + re-extract |
| **Re-AI** | `ai_only` | — | off | Re-runs LLM on cached texts; no web requests |

### Queue System

Admin operations that involve I/O (scrape, extract, enrich) go through a simple
in-process queue:

- `app_state.queue_items: list[dict]` — status: `pending | running | done | error`.
- `app_state._queue_fns: dict[str, coroutine]` — maps item ID to the async function.
- `_queue_worker()` — a long-running task that pulls items sequentially.
- `_enqueue(op, url_hash, url, city, topic, fn, priority=False)` — adds to queue.
  `priority=True` inserts right after the currently running item (position 1).
- Manual buttons (Scrape/Extract/Enrich on cache detail) use `priority=True`.
- "Bulk extract all" uses normal priority (appended to end).

### Active Providers Strip

Dashboard shows which search and AI providers are active:
```python
active_providers = {
    "search": (["Serper"] if serper_key else []) + (["Brave"] if brave_key else []) + ["SearXNG"],
    "ai":     (["DeepSeek"] if deepseek_key else []) + (["Groq"] if groq_key else []) + ["Ollama"],
}
```
Rendered as coloured badge chips in the dashboard template.

---

## 14. State — `scraper/web/state.py`

`app_state` is a module-level singleton shared across all requests:

```python
app_state.cities: list[CityConfig]
app_state.topics: list[TopicConfig]
app_state.pipeline_cfg: PipelineConfig
app_state.cache_manager: CacheManager
app_state.db_path: Path
app_state.is_running: bool
app_state.last_run_at: datetime | None
app_state.current_phase: str | None     # "scrape"|"extract"|"enrich_scrape"|...
app_state.current_url: str | None
app_state.scheduler: AsyncIOScheduler | None
app_state.version: str                  # from VERSION file
app_state.queue_items: list[dict]
app_state._queue_fns: dict[str, callable]
app_state._queue_worker_task: asyncio.Task | None
app_state._run_task: asyncio.Task | None
```

---

## 15. i18n — `scraper/web/i18n.py`

Translations for EN / HU / DE. Language is selected from:
1. `lang` cookie (set by `GET /set-lang`).
2. `Accept-Language` header.
3. Falls back to `"en"`.

`lang_context(request)` returns `{"lang": "hu", "t": <translate function>}` injected
into every public template.

`get_topic_labels(lang)` returns localised topic names (e.g. `running` → `"Futás"` in HU).

---

## 16. Log Streaming — `scraper/web/log_stream.py`

`broadcaster` is a module-level in-memory ring buffer (last 500 log lines).
Each log line is a dict assigned a monotonically increasing `seq` number.

`broadcast_processor` is a structlog processor that forwards every log event to the
broadcaster before the console renderer.

The SSE endpoint (`GET /admin/api/logs/stream`) polls `get_lines_after(last_seq)` every
0.5 seconds, sends new lines as `data: {json}\n\n`, and sends a keepalive comment
every 15 seconds.

---

## 17. Schema.org — `scraper/web/schema.py`

`records_to_jsonld(records) → str` generates a `SportsClub` / `Organization` JSON-LD
block for community records. Injected into `<head>` of public pages via
`<script type="application/ld+json">`.

---

## 18. Scheduler — `scraper/main.py`

- `AsyncIOScheduler` from APScheduler runs `_scheduled_run()` on the cron schedule.
- `SCHEDULE_CRON` env var overrides `settings.yaml`'s `schedule.cron`.
- Default: `"*/15 * * * *"` (every 15 minutes — useful for testing; production should
  be `"0 3 * * *"` or similar).
- Misfire grace: 900 s. If the server was down, the run is skipped rather than run late.
- Scheduled runs always use the default `skip_scraped` / `skip_extracted` from config.

---

## 19. Key Design Decisions and Invariants

**Extraction cache fingerprint**: any change to `SYSTEM_PROMPT` or the model name
automatically invalidates all cached extractions. Old cached results are re-extracted
on the next run. The `model_fingerprint` property on all extractor classes computes this.

**`joinable` filter**: the pipeline keeps only `joinable=True` records. Records where
the LLM set `joinable=False` (professional ensembles, paid courses, venues) are
extracted but immediately discarded. This is the primary quality gate.

**False positives**: admins can mark any community on the results page as a false
positive (type `extraction` or `enrichment`). These are injected at the end of the
system prompt for the relevant (city, topic) pair. A prompt history snapshot is saved
each time the FP list changes.

**Blocked domains**: Facebook, Instagram, Twitter, etc. are in the fetch blocklist
because they require auth and return useless login-wall HTML. They are still allowed as
`social_links` values in extracted records.

**Phone validation**: `phone` is nulled if it contains no digit. This catches AI
hallucinations like `"Nincs megadva"` ("not provided") being interpreted as a phone
number and rendered as `<a href="tel:Nincs megadva">`.

**URL validation** (`valid_url` filter): rejects URLs that:
- Don't start with `http://` or `https://`.
- Have no dot in the hostname (e.g. bare domain name without TLD).
- Contain spaces or `%20` (URL-encoded space = hallucinated community name used as URL).

**Source URL merging**: when the same community is found on multiple pages, the
`source_urls` list accumulates all of them. The community detail page shows all source
URLs in the attribution section.

**`community_id` stability**: once generated, `community_id` is stable across re-runs
because it's derived only from `name.lower() + city.lower()`. URL structure (`/{city}/{community_slug}`) is stable for the same reason.

**Test mode**: set `pipeline.test_mode: true` and `pipeline.test_cities: [Budapest]`
to run quickly on a single city. The admin config page reloads runtime scraper config
after saving city/topic/settings YAML.

---

## 20. Public Community Detail Page — Template Fields

`public_community.html` displays the following fields (using the `detail_row` macro):

| Icon | Label | Field |
|---|---|---|
| `map-pin` | Location | `r.location` |
| `calendar` | Schedule | `r.meeting_schedule` |
| `person-simple` | Vezető | `r.leader` |
| `users-three` | Tagok | `r.member_count` |
| `currency-circle-dollar` | Részvételi díj | `r.fee` |
| `star` | Szint | `r.skill_level` |
| `trend-up` | Csatlakozás | `r.join_process` |
| `translate` | Nyelv | `r.language` |
| `baby` | Korosztály | `r.age_range` |
| `repeat` | Rendszeresség | `r.frequency` |
| `envelope` | Contact | `r.email / r.phone / r.contact` |
| `flag` | Alapítva | `r.founding_year` |
| `tag` | Témák | `r.tags` (chips) |
| `book-open` | Történet | `r.history` |

Links section: `r.website` + `r.social_links` rendered as a vertical list with
platform icons (`link_meta` filter) and `valid_url` guard.

---

## 21. Deployment (Coolify / Docker)

### Dockerfile
```dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
COPY scraper/ ./scraper/
COPY config/   ./config/
RUN pip install --no-cache-dir .
CMD ["python", "-m", "scraper.main"]
```

### Services
- **scraper-app** (this app, port 8000)
- **searxng** (port 8080)
- **ollama** (port 11434)

### Volume strategy
Persist only the runtime directories, not the whole `/app` tree:
- `/app/data` preserves `scraper.db` (all communities + cache).
- `/app/config` preserves config edits made through the admin UI.

Do not mount a volume over the entire `/app` directory, because that can hide updated
application code from newer Docker images.

### Health check
Coolify checks `GET /` (FastAPI serves the public home page).

### First deploy
1. Set required env vars (SEARXNG_URL, OLLAMA_URL, ADMIN_PASSWORD at minimum).
2. Pull Ollama model: `docker exec <ollama-container> ollama pull gemma3:4b`.
3. Trigger a manual run from the admin dashboard.

---

## 22. Common Tasks

**Add a new city**: edit `config/cities.yaml` via admin config page or directly. Add
`name`, `country`, `locale`, `search_variants`. If the locale is new, add search terms
for it in `topics.yaml`. Also add coordinates to `CITY_COORDS` in `app.py` for the map.

**Add a new topic**: edit `config/topics.yaml`. Add the name and search terms for each
supported locale. Add an entry to `TOPIC_ICONS` and `TOPIC_LABELS` in `app.py`. Add a
label to each language in `i18n.py`'s `get_topic_labels()`.

**Change the extraction model**: update `settings.yaml` (ollama.model or deepseek.model).
The fingerprint change will invalidate all extraction caches and trigger re-extraction
on the next run. To avoid re-extracting everything immediately, use Smart mode.

**Mark a false positive**: on the admin results page (`/admin/results/{city}/{topic}`),
click the "FP" button next to a community. Provide a reason. The record stays in the
DB but won't be re-extracted from the same source; and the LLM will be told not to
extract it again.

**Manually re-process one URL**: go to `/admin/cache`, find the URL, click the detail
page, then use the Scrape / Extract / Enrich buttons. These are high-priority queue ops.

**Reset everything**: "Clear All Cache" button on `/admin/cache` wipes both the cache
and all community records. Requires a full run to rebuild.
