---
type: Concept
title: Server-side URL Safety
description: Every server-side fetch validates HTTP(S) syntax, public DNS answers, blocked domains, and each redirect target before connecting.
tags: [security, ssrf, fetch, dns, redirects]
timestamp: 2026-07-10
resource: scraper/url_safety.py
---

# Server-side URL Safety

*User, search-provider, and LLM-supplied URLs are untrusted input even when an admin clicks approve.*

## Validation layers

`scraper.url_safety` provides a shared policy for `fetch_and_clean`, submission approval, re-extraction, enrichment, and optional Playwright fetching:

1. Only `http` and `https`; no credentials, control characters, invalid ports, or overlong URLs.
2. Reject localhost, `.local`, `.internal`, single-label hosts, and non-global literal IPs.
3. Resolve DNS off the event loop and require **every** IPv4/IPv6 answer to be globally routable.
4. Disable automatic httpx redirects; resolve and validate each `Location` target before the next request.
5. Re-check the configured blocked-domain list after every redirect.
6. Playwright routes every HTTP(S) request through the same public-address guard.

Public community submissions get syntax validation immediately. Admin approval performs the DNS check before changing the row to `approved`; an unsafe URL remains pending and no background fetch is queued.

## Related

- [[fetch-layer]]
- [[playwright-vs-blocked-domain-ordering]]
- [[web-app]]
