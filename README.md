# közösségek.com — Community Directory Scraper

A self-hosted scraper and public directory of local community groups in Hungary.
The app continuously discovers running clubs, choirs, board-game nights, yoga circles, etc.
by city and interest topic, then serves them at **közösségek.com**.

## What it does

For each `(city, topic)` pair it:

1. Generates search queries in Hungarian/English.
2. Searches the web — Serper → Brave → SearXNG fallback chain.
3. Fetches and cleans pages (`httpx` + `trafilatura`; Playwright for JS-heavy sites like Reddit).
4. Runs LLM extraction — DeepSeek → Groq → Ollama fallback chain.
5. Saves structured community, venue, and person records to SQLite.
6. Serves the data through a bilingual public website (HU/EN) and a password-protected admin UI.

The scheduled **Smart run** works in two phases, both city-ordered (largest cities first):
1. **Re-AI phase** — re-extracts all cached pages where the extraction fingerprint is stale (prompt or model changed).
2. **Search phase** — searches for new `(city, topic)` pairs not yet covered.

## Architecture

```
pipeline.py
  └── search.py      (Serper / Brave / SearXNG)
  └── fetch.py       (httpx + trafilatura / Playwright)
  └── extract.py     (DeepSeek / Groq / Ollama)
  └── store.py       (SQLite via db.py)

web/app.py           (FastAPI — public site + /admin)
main.py              (APScheduler + uvicorn)
```

## Public site

| Page | URL |
|------|-----|
| Home | `/` |
| City directory | `/:city` |
| Explore by tag | `/felfedezes` |
| Community detail | `/:city/:slug` |
| Venue detail | `/:city/helyszinek/:slug` |
| Person detail | `/:city/emberek/:slug` |

## Admin UI (`/admin`)

| Section | Purpose |
|---------|---------|
| Dashboard | Run controls, stats, Hungary/Global scope toggle |
| Results → Communities / Venues / People | Browse and edit scraped records |
| Moderation → Duplicates | Review and merge duplicate communities |
| Moderation → Edit requests | User-submitted corrections |
| Moderation → Not community | Mark false positives |
| Moderation → Beküldések | User submissions |
| Moderation → Recategorize | AI re-categorizes "other"-topic communities |
| System → Progress | Per city/topic scrape progress |
| System → Logs | Real-time log stream (SSE) |
| System → Config | Edit YAML config in-browser |
| System → Prompts | Edit LLM prompts live |
| Subscribers | Newsletter subscriber list |

## Configuration

| File | Purpose |
|------|---------|
| `config/cities.yaml` | City list: name, country, locale, search variants |
| `config/topics.yaml` | Topic list: per-locale search terms |
| `config/settings.yaml` | Model/API/cache/schedule config |

## Environment variables

| Variable | Description |
|----------|-------------|
| `ADMIN_PASSWORD` | Required — gates the entire `/admin` UI |
| `SEARXNG_URL` | SearXNG base URL |
| `OLLAMA_URL` | Ollama base URL |
| `DEEPSEEK_API_KEY` | Optional — faster/better extraction |
| `GROQ_API_KEY` | Optional — fallback extraction |
| `SERPER_DEV_API_KEY` | Optional — primary search |
| `BRAVE_API_KEY` | Optional — secondary search |

## Deployment

Runs on Coolify (Hetzner) via Docker. Persist `/app/data` (SQLite) and `/app/config` (YAML edits).

## Development

```bash
pip install -e ".[dev]"
pytest                   # run tests
ruff check scraper/      # lint
```

No local server needed — the app runs on Hetzner. Read templates and code directly for verification.
