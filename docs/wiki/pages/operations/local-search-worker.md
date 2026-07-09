---
type: Runbook
title: Local Browser-Driven Search Worker
description: Offload Google search to your own machine's browser (residential IP) via Playwright, feeding results into production's search_cache through an admin ingest API.
tags: [operations, search, playwright, worker, captcha, dataforseo]
timestamp: 2026-07-09
resource: scripts/local_search_worker.py
---

# Local Browser-Driven Search Worker

*Runs Google searches from your own machine (residential IP, far less CAPTCHA than the datacenter) and posts the URL lists into production's `search_cache`. The pipeline then finds the search pre-populated and skips its own search step.*

Motivation: the server-side [[search-layer|GooglePlaywrightSearchClient]] gets CAPTCHA'd on Hetzner's datacenter IP and falls back to paid DataForSEO. Doing the search from a residential IP avoids both.

## Topology

```
your Mac                              production (Hetzner)
──────────                            ────────────────────
local_search_worker.py                GET  /admin/api/search/jobs   → pairs + prebuilt queries
  ├ GET jobs ──────────────────────▶  (skips pairs already in search_cache within TTL)
  ├ Playwright Google search (delays)
  └ POST ingest ───────────────────▶  POST /admin/api/search/ingest → writes search_cache
                                       next pipeline run skips search for those pairs
```

Only the **search** step moves to the laptop; fetch, extract, and the DB stay in production.

## Server endpoints (`app.py`)

- `GET /admin/api/search/jobs?limit=&country=` — returns `{jobs: [{city, topic, locale, queries}]}` for pairs with no fresh `search_cache` entry; queries are built server-side via `build_queries` so the worker needs no config.
- `POST /admin/api/search/ingest` — body `{city, topic, queries, urls}` → `save_search_cache` (URLs validated to http(s), unknown city/topic rejected).

**Auth:** both are under `/admin`, so Basic auth applies. The POST also needs a matching `X-Worker-Token` header — this bypasses the same-origin CSRF check ([[web-app]]) for the machine-to-machine call. Set `SEARCH_WORKER_TOKEN` on the server (empty = ingest disabled).

## Running the worker

```bash
# one-time on your machine
pip install -e ".[dev]" && playwright install chromium

PYTHONPATH=. python scripts/local_search_worker.py \
  --base-url https://kozossegek.com \
  --admin-user admin --admin-password "$ADMIN_PASSWORD" \
  --worker-token "$SEARCH_WORKER_TOKEN" \
  --country Hungary --headful
```

Key flags: `--headful` (visible browser — lets you solve a CAPTCHA by hand), `--country`, `--batch` (jobs per API pull), `--max-jobs`, `--min-delay`/`--max-delay` (jittered spacing on top of the client's 8 s), `--captcha-cooldown` (headless back-off), `--once`. Env fallbacks: `WORKER_BASE_URL`, `ADMIN_USER`, `ADMIN_PASSWORD`, `SEARCH_WORKER_TOKEN`.

The worker reuses `GooglePlaywrightSearchClient`, so consent handling, CAPTCHA detection, snippet scraping, and rate limiting come for free. On a CAPTCHA it pauses for manual solve (`--headful` in an interactive terminal) or cools down.

## Beating Google's automation CAPTCHA

Vanilla Playwright Chromium gets CAPTCHA'd by Google on the **first query**, even from a residential IP — Google detects the automation fingerprint, not just the IP. The client therefore supports:

- **Persistent profile** (`--user-data-dir`, default `~/.cs_search_profile`): `launch_persistent_context` keeps consent/login cookies and history across runs, so the profile looks human. This is the main defense.
- **Stealth**: `--disable-blink-features=AutomationControlled` + an init script masking `navigator.webdriver`/`languages`/`plugins`/`window.chrome`, a macOS UA, and `--browser-locale` matched to the search country (`sv-SE` for Sweden).

**One-time warm-up** (do this in your own terminal before unattended runs):

```bash
PYTHONPATH=. python scripts/local_search_worker.py --warmup --browser-locale sv-SE
```

It opens the persistent profile on google.com; accept the cookie consent, run a search or two, and — most effective — **sign into a Google account**. A logged-in session almost never triggers search CAPTCHAs. Subsequent unattended runs reuse the warmed profile.

## Status / findings (2026-07-09)

The infrastructure works end-to-end (auth, token, `/jobs` → search → `/ingest` → `search_cache`), but **reliably scraping search results from a driven browser did not pan out**:

- **Google** CAPTCHAs the automated browser on the *first* query — with every mitigation tried: residential IP, headful real Chromium, a **logged-in Google account** in a persistent profile, stealth (webdriver mask + `AutomationControlled` off + matched locale + no `num=`), and **patchright** (patched Playwright that hides CDP automation signals). Google's detection goes beyond what these defeat.
- **DuckDuckGo** `html.`/`lite.` endpoints hard-return **HTTP 403/202** to scrapers; the SPA served the `static-pages/418.html` bot-block with vanilla Playwright and a **202 shell (no rendered results)** with patchright — patchright measurably helped the fingerprint (418 → 202) but results still didn't render/scrape, and the result XHR (`links.duckduckgo.com/d.js`) didn't fire.
- **Bing** returns a 200 result-less shell (challenge) to the driven browser.
- Rapid testing likely temporarily rate-limited the residential IP across engines, which would need a cooldown before any fair retry.

**Conclusion:** `patchright` is integrated and does hide the Playwright/CDP fingerprint (worth keeping), but the search engines' additional server-side/heuristic defenses block reliable result scraping. **Keep DataForSEO/Serper server-side as the actual search path.** The worker infra (endpoints, `scripts/local_search_worker.py`, engine flag, DDG client, persistent profile) remains for future refinement — e.g. a proper DDG `vqd`/XHR extractor, much slower pacing (minutes between queries), or a residential-proxy pool.

## Setup for the browser engines

`--engine duckduckgo` (default) or `--engine google`. Both route through **patchright** when installed (`pip install patchright && patchright install chromium`), falling back to vanilla playwright otherwise.

## Notes

- The worker does **not** fetch or extract — it only fills `search_cache`. Run a normal pipeline afterward to fetch+extract the new URLs.
- Idempotent: re-running skips pairs already cached within the TTL ([[search-ttl-3650-days]]).
- Keep DataForSEO/Serper configured as a server-side fallback for pairs the worker hasn't reached.
