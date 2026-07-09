---
type: Hack
title: Tailwind CDN JIT: Never Server-Render Large Lists
description: The CDN JIT scans the full initial DOM before paint; load big admin lists via JSON + DocumentFragment.
tags: [tailwind, cdn, performance, admin]
timestamp: 2026-07-09
resource: scraper/web/templates
---

# Tailwind CDN JIT: Never Server-Render Large Lists

*The Tailwind CDN JIT scanner runs over the initial DOM before the page becomes visible — server-rendering thousands of rows causes a multi-second freeze.*

## The problem

Tailwind CDN scans the full DOM to detect which utility classes to compile. On an admin page with 500+ community rows rendered server-side, this scan takes 3-8 seconds and blocks page render.

## The fix

Load large lists via a JSON endpoint + client-side `DocumentFragment` insertion. The Tailwind scanner only sees the empty container; the actual rows are added after the JIT pass completes.

## Reference implementation

`logs.html` + `/admin/api/logs/history`: the template renders an empty `<div id="log-container">`, JS fetches `/admin/api/logs/history`, builds a `DocumentFragment`, and appends it — no Tailwind freeze.

## When this matters

Any admin page with:
- Community lists (500+ rows in large databases)
- Log viewers
- Search result tables

Public pages are not affected because they use server-side pagination and rarely exceed 50 rows per page.

## Related

- [[admin-json-endpoint-pattern]]
