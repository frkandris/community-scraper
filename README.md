# Community Scraper

Searches the web for active community groups based on city lists and community topics, interprets results with a local LLM, and stores structured JSON data with full git history.

## Architecture

```
[scraper app :8001] → [SearXNG :8080]   # web search
[scraper app :8001] → [Ollama  :11434]  # local LLM
```

## Components

- **SearXNG** – self-hosted meta-search engine
- **Ollama** – local LLM inference (llama3.2:3b)
- **FastAPI** – web admin interface (`:8001`)
- **APScheduler** – scheduled runs (default: daily at 03:00 UTC)

## Web Interface

| Page | URL | Description |
|------|-----|-------------|
| Dashboard | `/` | Status, run controls, software info |
| Results | `/results` | Browse records by city and topic |
| Cache | `/cache` | Manage the two-level page cache |
| Config | `/config` | Edit YAML config in-browser |
| Logs | `/logs` | Real-time log stream (SSE) |
| History | `/history` | Git commit history |

## Two-Level Cache

Each scraped URL is stored in `data/cache/pages/` with two independent cache layers:

- **Scrape cache** – raw page text fetched from the web. Skip re-fetching known URLs.
- **Extract cache** – AI-interpreted community records. Skip re-running LLM on already-processed pages.

**Run modes:**
- **Full run** – search → fetch (respects scrape cache) → AI (respects extract cache)
- **AI only** – skip search and fetching entirely; re-run AI on cached pages only (useful when switching models)

Per-run overrides for both cache layers are available in the Dashboard.

## Configuration

`config/cities.yaml` – cities with their locale  
`config/topics.yaml` – topics with locale-specific search terms  
`config/settings.yaml` – Ollama / SearXNG / fetch / cache parameters

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SEARXNG_URL` | `http://localhost:8080` | SearXNG base URL |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama base URL |
| `SCHEDULE_CRON` | `0 3 * * *` | Run schedule (cron expression) |
| `GIT_USER_NAME` | – | Git commit author name |
| `GIT_USER_EMAIL` | – | Git commit author email |
| `GIT_TOKEN` | – | GitHub PAT (optional, for push) |

## Data Storage

Results are stored in `data/<city>/<topic>/communities.json`. Every run creates an auto-commit so the full history of community data is browsable via git.
