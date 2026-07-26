---
type: Architecture
title: SEO Indexing Strategy
description: Canonical tags, thin-page noindex, domain-scoped sitemaps, and robots rules that keep the two-domain directory from cannibalizing its own search rankings.
tags: [seo, canonical, sitemap, robots, noindex, jsonld]
timestamp: 2026-07-09
resource: scraper/web/app.py
---

# SEO Indexing Strategy

*Two domains serve overlapping content, so the indexing strategy centers on telling Google which copy is canonical and keeping thin/duplicate pages out of the index. See [[seo-cross-domain-canonical]] and the [[2026-06-seo-traffic-collapse]] post-mortem.*

## Canonical tags

`public_base.html` emits `<link rel="canonical" href="{{ canonical_base or site_url }}{{ path }}">`. `canonical_base` is set only for city-scoped pages (via `_canonical_base`); other pages self-canonicalize to their own `site_url`. The cross-domain rule for Hungarian pages is the load-bearing part — see [[seo-cross-domain-canonical]].

## Thin-page noindex

`public_base.html` adds `<meta name="robots" content="noindex">` when `page_noindex` is true:

- **Explore**: `city and topic and total == 0` — a city+topic combo with zero communities.
- **Community detail**: the record has no non-empty `description`.

Rationale: stop Google indexing empty/thin programmatic pages, a major trigger of the mass "Crawled – currently not indexed" devaluation.

## Sitemap scoping

`GET /sitemap.xml` is domain-scoped via `lang_context` + `_site_cities`:

- **HU cities are removed from the meetapedia sitemap** (`site_city_names -= _hu_city_names()`) — a sitemap must list only canonical URLs, and HU pages canonicalize to kozossegek.
- **Thin community pages are skipped** (no description) — consistent with `page_noindex`.
- Venue/person URLs are emitted only for kozossegek.
- **Country landing pages** (`/cities/<slug>`, meetapedia only) are listed for countries with live content, minus Hungary — see [[country-landing-pages]].
- Order-preserving dedup via `dict.fromkeys`; `changefreq weekly`, no `priority`.
- **`<lastmod>` on community pages** (2026-07-26): `get_community_lastmods()` supplies
  each community URL's `updated_at` date, keyed by `(city, public_slug)` and resolved
  the same way as the public route (`ORDER BY topic, id`, first-wins) so the date
  matches the record actually served. `updated_at` only advances on a real content
  change — `_bulk_upsert_communities` compares a content fingerprint that excludes the
  volatile `extracted_at` — so a fingerprint re-extraction does not churn every page's
  `<lastmod>` (the [[2026-06-seo-traffic-collapse]] stability lesson). See [[persistence-layer]].

## robots.txt and other signals

Per-domain `Sitemap:` line. Disallows `/admin, /source/, /api/, /set-lang, /unsubscribe, /community/, /healthz, /kereses`; special-cases `facebookexternalhit` with `Allow: /` for link previews. `/set-lang` also sends `X-Robots-Tag: noindex, nofollow`.

## JSON-LD

`schema.py:records_to_jsonld` builds a `@graph` of schema.org types (`SportsClub`/`MusicGroup`/`DanceGroup`/`PerformingGroup`, default `Organization`) mapped from topic, injected into `<head>` on community and explore pages. It escapes `</` → `<\/` to prevent script-tag breakout.

## Known gap

There are **no `hreflang`/`rel=alternate`** tags anywhere, despite the same content on two domains and ~50 language variants. Cross-domain canonical is the only consolidation signal. Acceptable while the two domains serve identical (untranslated) HU content; revisit if meetapedia ever serves genuinely translated pages.
