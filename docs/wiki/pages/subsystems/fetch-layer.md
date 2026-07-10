---
type: Subsystem
title: Fetch Layer
description: An SSRF-safe httpx/Playwright fetcher validates public DNS and redirects before trafilatura/html2text turns HTML into clean text.
tags: [fetch, trafilatura, httpx, playwright, blocked-domains]
timestamp: 2026-07-10
resource: scraper/fetch.py
---

# Fetch Layer

*`fetch_and_clean(url, …)` returns clean page text or `None`; `fetch_many` runs it concurrently under a semaphore, capped at `max_pages`.*

See [[search-layer]] for where URLs come from, [[extraction-layer]] for what consumes the text.

## Extraction pipeline

Two-tier: `trafilatura.extract(include_comments=False, include_tables=False)` first; if the result is missing or `< min_text_length` (100), fall back to `html2text` (links + images ignored). Returns `None` if the fallback is also too short, if HTTP status ≥ 400, if `content-type` lacks `text/html`, or if URL safety rejects the initial/redirect target.

## Safety gates and ordering

Before either httpx or Playwright runs, `fetch_and_clean` applies [[server-side-url-safety]] and the configured blocked-domain list. A domain present in both `playwright_domains` and `blocked_domains` is blocked; Playwright can no longer bypass the policy. Every HTTP redirect and Playwright request is checked again.

Blocked domains (`twitter, x, facebook, instagram, tiktok, linkedin, youtube, reddit`) are login-walled/bot-hostile and return no useful text. They are still valid as `social_links` values on extracted records. Matching uses exact host/subdomain boundaries through `host_matches_domain`.

Blocked URLs are filtered **twice** (pipeline pre-filter + `_is_blocked` inside `fetch_and_clean`) — belt-and-suspenders so cached-URL paths can't slip a blocked URL through.

## Concurrency

`fetch_many` builds one `asyncio.Semaphore(max_concurrent)` (default 3), wraps each fetch, and truncates to `urls[:max_pages]` (default 5). Only URLs yielding non-empty text return as `(url, text)`.

## Playwright fetcher (`playwright_fetch.py`)

Dormant by default — `playwright_domains: []`, so `pw_fetcher` stays `None`. History lesson (see CHANGELOG 2026-05-15): social domains were moved *out* of `playwright_domains` into `blocked_domains` because launching Chromium for login-walled sites caused 91% CPU / 43 GB disk I/O per run.

When enabled: detects login walls via `_LOGIN_MARKERS` (Facebook/Instagram/Reddit strings) and returns `None` (a rendered login wall has no useful content). Waits 3.0 s for `reddit.com`/SPAs vs 1.5 s otherwise. Reuses `fetch._extract_text` via a late import (avoids a circular dependency) and creates a fresh browser context per URL to isolate cookies.

## Shared User-Agent

The same Chrome 124 UA string is hardcoded in `fetch._HEADERS` and the Playwright fetcher context. Updating it means editing both files — there is no single source of truth.
