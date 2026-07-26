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

## 2026-07-26: upgraded from canonical hint to hard 301

GSC (28 days to 2026-07-23) proved the canonical was **not enough**: Google ignored
it and kept meetapedia as the HU winner — meetapedia earned 551 HU impressions to
kozossegek's 33, and only 43 of kozossegek's 27K pages got any impression at all
(≈deindexed). A `rel=canonical` is a hint; a 301 is a command.

`_hu_redirect(request, city_name)` (`web/app.py`) now returns a **301** to
`https://kozossegek.com{path}?{query}` whenever `_detect_site == "meetapedia"` and
the city is Hungarian. It is called at the top of every city-scoped route
(`_render_explore`, `public_city_segment`, `public_city`, `public_venue_detail`,
`public_person_detail`). Non-HU cities (meetapedia's own market) render normally;
kozossegek never redirects its own HU pages. The canonical tag stays as a
belt-and-braces signal, and the sitemap already omits HU cities on meetapedia — all
three now agree. See [[2026-06-seo-traffic-collapse]] and [[indexing-strategy]].

## Verification

```
curl -sI https://meetapedia.com/szombathely/ezustkor-filmklub | grep -i location
# → location: https://kozossegek.com/szombathely/ezustkor-filmklub   (301)
curl -s https://meetapedia.com/sitemap.xml | grep -c szombathely   # → 0
```
