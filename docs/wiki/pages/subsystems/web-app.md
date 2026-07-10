---
type: Subsystem
title: Web App (routes, auth, state)
description: One FastAPI app with a public router and an /admin router gated by pure-ASGI Basic auth; Hungarian paths are canonical and English paths redirect.
tags: [web, fastapi, routing, auth, app-state, admin]
timestamp: 2026-07-10
resource: scraper/web/app.py
---

# Web App

*A single `_fastapi` app serves both domains; `app = _BasicAuth(_fastapi)` wraps it so `/admin/*` is gated. See [[two-domain-single-container]] and [[i18n-and-site-detection]].*

## Auth: pure-ASGI `_BasicAuth`

`_BasicAuth` is pure ASGI (not Starlette `BaseHTTPMiddleware`) specifically so it does **not buffer SSE** (the admin log stream). It guards only `/admin`-prefixed paths; everything else passes through. Missing `ADMIN_PASSWORD` → 503. Credentials compared with `hmac.compare_digest` (constant-time). Writes additionally require same-origin (`_same_origin_admin_write`: `Origin`/`Referer` netloc must equal `Host`) — a CSRF guard on top of auth.

## Routing: Hungarian canonical, English alias

The Hungarian path is the canonical handler; the English path 301/302-redirects to it (`/explore`→`/felfedezes`, `/map`→`/terkep`, `/about`→`/rolunk`, `/venues`→`/helyszinek`, `/people`→`/emberek`, `/submit-community`→`/kozosseg-bekuldes`). `_render_explore` is the shared renderer.

**Route ordering is load-bearing.** The two greedy catch-alls `/{city_slug}/{segment}` and `/{city_slug}` are registered **last**, after all literal/deeper routes (`/felfedezes/{topic_slug}`, `/{city_slug}/helyszin/…`, `/{city_slug}/ember/…`). Starlette matches in registration order, so literals win. `public_city_segment` disambiguates a topic slug from a community-name slug by trying `_topic_from_url_slug` first, then `_find_community_by_slug`, else 302 to the city page. When adding a route that exists on both domains, add both prefixes to nav active-state checks.

## `app_state` singleton

Module-global dataclass. **`cities` and `topics` are lists of config objects, not dicts** — always `c.name` / `c.country` / `c.locale`; dict-style access 500s any route touching them (see [[2026-05-coverage-page-500]]). Pipeline callbacks mutate progress fields read by coverage, while `RunCoordinator` exclusively owns `is_running`, `_run_task`, and `current_run_mode`. `_home_stats_cache` (keyed by site) is invalidated after every run.

## Coverage page

City × topic matrix from `get_city_topic_states` + `get_fully_processed_pairs` at the current fingerprint. Five cell states: green (has communities), blue ✓ (done, 0 results), amber ~ (searched but stale fingerprint), gray · (never searched), pulsing ▶ (actively processing). JS polls `/admin/api/coverage/current` every 3 s to move the highlight and refreshes the previous cell via `/admin/api/coverage/cell`. `POST /admin/api/restamp-fingerprints` bulk-updates stale fingerprints to current without reprocessing (turns amber → green). See [[2026-06-coverage-amber-cells]]. Note `_COVERAGE_PAGE_SIZE = 2` looks like a leftover debug constant.

## Queue and runs

Admin I/O ops (scrape/extract/enrich) go through an in-process queue (`queue_items` + `_queue_fns` + a worker task); manual cache-detail buttons use `priority=True`. Pipeline, scheduled, startup, and revalidate runs share one coordinator-owned slot and task-identity cleanup. See [[shared-run-task-slot]] and [[asyncio-task-cancellation]].

## Tailwind CDN JIT

Both admin and public load the Tailwind CDN JIT runtime (not a build). All utility classes must appear as **complete literal strings** at load; JS that rebuilds cells hard-codes full class strings (`bg-green-100 text-green-700`) rather than templating them, because a computed `bg-${c}-100` would never generate. See [[tailwind-cdn-jit-large-lists]].

## Flags

`stats_clicks.html` exists but no route renders it (dead template after the outclick-tracking revert). `request_city_en` ignores its form and just 301s to `/varosok` (stub). `_render_explore` does per-topic/per-city DB calls in loops (N+1 risk on large data), mitigated on home by `_home_stats_cache`.
