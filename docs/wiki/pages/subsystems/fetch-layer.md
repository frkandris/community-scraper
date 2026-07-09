---
type: Subsystem
title: Fetch Layer
description: httpx + trafilatura (html2text fallback) turns URLs into clean text; blocked domains and an optional Playwright fetcher gate what actually gets fetched.
tags: [fetch, trafilatura, httpx, playwright, blocked-domains]
timestamp: 2026-07-09
resource: scraper/fetch.py
---

# Fetch Layer

*`fetch_and_clean(url, …)` returns clean page text or `None`; `fetch_many` runs it concurrently under a semaphore, capped at `max_pages`.*

See [[search-layer]] for where URLs come from, [[extraction-layer]] for what consumes the text.

## Extraction pipeline

Two-tier: `trafilatura.extract(include_comments=False, include_tables=False)` first; if the result is missing or `< min_text_length` (100), fall back to `html2text` (links + images ignored). Returns `None` if the fallback is also too short, if HTTP status ≥ 400, or if `content-type` lacks `text/html`. trafilatura gives clean article text; html2text is the cruder safety net.

## Check ordering is load-bearing

In `fetch_and_clean`, `playwright_fetcher.matches(url)` is checked **before** `_is_blocked(url)`. A domain in both lists gets Playwright-fetched, bypassing the block — so social-media domains must stay out of `playwright_domains` entirely. See [[playwright-vs-blocked-domain-ordering]].

Blocked domains (`twitter, x, facebook, instagram, tiktok, linkedin, youtube, reddit`) are login-walled/bot-hostile and return no useful text. They are still valid as `social_links` values on extracted records. Matching is naive **substring-in-host** (`any(domain in host …)`) — shared by the fetch check, the pipeline pre-filter, and `PlaywrightFetcher.matches`; susceptible to false matches on any host merely containing the string.

Blocked URLs are filtered **twice** (pipeline pre-filter + `_is_blocked` inside `fetch_and_clean`) — belt-and-suspenders so cached-URL paths can't slip a blocked URL through.

## Concurrency

`fetch_many` builds one `asyncio.Semaphore(max_concurrent)` (default 3), wraps each fetch, and truncates to `urls[:max_pages]` (default 5). Only URLs yielding non-empty text return as `(url, text)`.

## Playwright fetcher (`playwright_fetch.py`)

Dormant by default — `playwright_domains: []`, so `pw_fetcher` stays `None`. History lesson (see CHANGELOG 2026-05-15): social domains were moved *out* of `playwright_domains` into `blocked_domains` because launching Chromium for login-walled sites caused 91% CPU / 43 GB disk I/O per run.

When enabled: detects login walls via `_LOGIN_MARKERS` (Facebook/Instagram/Reddit strings) and returns `None` (a rendered login wall has no useful content). Waits 3.0 s for `reddit.com`/SPAs vs 1.5 s otherwise. Reuses `fetch._extract_text` via a late import (avoids a circular dependency). Creates a fresh browser context per URL (isolates cookies), unlike the search client which reuses one context to preserve accepted-consent state.

## Shared User-Agent

The same Chrome 124 UA string is hardcoded in three places (`fetch._HEADERS`, the Google search context, the Playwright fetcher context). Updating it means editing three files — no single source of truth.
