---
type: Runbook
title: Local Browser-Driven Search Worker (removed)
description: REMOVED 2026-07-09 — browser-driven search never beat engine bot detection; kept as post-mortem. Code in git history.
tags: [operations, search, playwright, worker, captcha, dataforseo]
timestamp: 2026-07-10
---

# Local Browser-Driven Search Worker (removed)

*A residential-IP worker once filled production `search_cache`; the script, API endpoints, token plumbing, and browser search clients were removed after automation remained unreliable.*

## Former topology

The local script pulled unsearched city/topic jobs from `/admin/api/search/jobs`, searched in a persistent Playwright profile, then posted URL lists to `/admin/api/search/ingest`. Fetching, extraction, and SQLite stayed on the server. Basic auth plus `X-Worker-Token` protected ingestion.

## Why it was removed

- Google challenged the first automated query despite headful Chromium, a logged-in persistent profile, locale/UA matching, webdriver masking, and patchright.
- DuckDuckGo returned 403/202 bot shells; its result XHR never produced usable results.
- Bing returned a result-less challenge shell.

The experiment showed that a residential IP alone does not defeat browser-automation heuristics. Keeping the code added operational and security surface without a reliable quota saving, so commits `627a337..9c5aba7` removed it. DataForSEO is now the sole server-side provider; see [[search-provider-fallback-chain]], [[search-layer]], and [[deployment-coolify]].
