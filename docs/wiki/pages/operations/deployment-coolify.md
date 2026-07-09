---
type: Runbook
title: Deployment (Coolify / Hetzner)
description: Docker on Coolify; persist only /app/data and /app/config; required and optional env vars.
tags: [operations, deployment, coolify, docker, env]
timestamp: 2026-07-09
resource: Dockerfile
---

# Deployment (Coolify / Hetzner)

*The app runs on Coolify (Hetzner) via Docker as a FastAPI/uvicorn server on port 8000. There is no local dev server — read code/templates directly for verification.*

## Volumes

Persist only the runtime dirs, never the whole `/app` tree (that would hide updated code from new images):

- `/app/data` — `scraper.db` (all communities + cache).
- `/app/config` — YAML edits made through the admin UI.

## Environment variables

- **Required:** `ADMIN_PASSWORD` (gates the entire `/admin` UI; unset → 503). `ADMIN_USER` defaults to `admin`.
- **Search:** `DATAFORSEO_LOGIN` / `DATAFORSEO_PASSWORD`, `SERPER_DEV_API_KEY`. All optional — missing just drops that provider (Google Playwright is free and always primary).
- **Extraction:** `DEEPSEEK_API_KEY`, `GROQ_API_KEY`. Optional; missing drops that provider.
- **Email (Resend):** `RESEND_API_KEY`, `FEEDBACK_EMAIL`, `RESEND_FROM`. Optional — missing = silent no-op on `/subscribe`, `/report-not-community`, `/suggest-edit`, `/claim-community`.

## CSS build

`scraper/web/static/css/app.css` is gitignored; Docker builds it from `input.css` via `pytailwindcss` at image build time. For local edits, maintain `app.css` by hand; committing `input.css` changes suffices for production. Note the runtime pages also load the Tailwind **CDN JIT** — see [[tailwind-cdn-jit-large-lists]].

## Tests / lint

```
PYTHONPATH=. .venv/bin/pytest --ignore=tests/test_city_page.py   # test_city_page has a known unrelated failure
ruff check scraper/
```
The repo requires Python ≥ 3.12.
