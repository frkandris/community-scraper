---
type: Decision
title: Cross-Domain Canonical for Hungarian Pages
description: HU-city pages on meetapedia.com canonicalize to kozossegek.com so Google stops consolidating the duplicate toward the traffic-less domain.
tags: [seo, canonical, duplicate-content, hungary]
timestamp: 2026-07-09
resource: scraper/web/app.py
---

# Cross-Domain Canonical for Hungarian Pages

*`_canonical_base(request, city_name)` returns `https://kozossegek.com` if the request is on kozossegek **or** the city is Hungarian; otherwise `https://meetapedia.com`.*

## Why

Hungarian-city pages are served with **identical paths on both domains** (`/szombathely/ezustkor-filmklub` exists on both), with self-canonicals and no hreflang. Google treated them as duplicates and consolidated toward meetapedia.com — which gets almost no Hungarian traffic (avg position ~31). Result: the traffic-earning domain (kozossegek) was being deindexed in favor of its twin. See the [[2026-06-seo-traffic-collapse]] post-mortem.

The fix makes **all** HU-city pages point their canonical at kozossegek.com regardless of which domain served them, so Google consolidates toward the right home. Everything else self-canonicalizes.

## Where it is wired

`canonical_base` is passed from the city-scoped renderers: `_render_explore` (only when `city` is set), venue detail, person detail, and the community page in `public_city_segment`. The template falls back to `site_url` when `canonical_base` is absent (non-city pages self-canonicalize). It composes with the [[indexing-strategy]] sitemap rule (meetapedia sitemap omits HU cities) — canonical and sitemap must agree, or Google gets mixed signals.

## Verification

```
curl -s https://meetapedia.com/szombathely/ezustkor-filmklub | grep canonical
# → <link rel="canonical" href="https://kozossegek.com/szombathely/ezustkor-filmklub">
curl -s https://meetapedia.com/sitemap.xml | grep -c szombathely   # → 0
```
