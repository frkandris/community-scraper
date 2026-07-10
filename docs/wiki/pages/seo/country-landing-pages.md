---
type: Concept
title: Country Landing Pages
description: Path-based /cities/<slug> country pages replace the ?country= query form (301'd) — self-canonical, sitemap-listed, and reachable from home headings and the /cities country index.
tags: [seo, urls, country, sitemap, canonical]
timestamp: 2026-07-10
resource: scraper/web/app.py
---

# Country Landing Pages

*Query-parameter filters are invisible to Google here (canonicals strip queries), so
the country-filtered city list moved to a real path that can rank.*

## URL scheme

- `/cities/{slug}` (EN) and `/varosok/{slug}` (HU) render the country-filtered city
  list — slug via `_slugify(country)`, reverse-resolved by `_country_from_slug()`.
- Legacy `?country=X` → **301** to the path form (link equity consolidates);
  unknown slugs → 302 to `/cities`.
- The country routes are registered before the `/{city_slug}` catch-alls in
  `scraper/web/app.py`, so "cities" is never parsed as a city name.

## SEO wiring

- Self-canonical (the base template canonical uses `request.url.path` — which is
  exactly why the old query form could never be indexed as a separate page).
- Title/H1: `Communities in {country}` via the `cities_in_country` i18n key.
- **Sitemap**: meetapedia's sitemap lists `/cities/<slug>` for countries with live
  content, **minus Hungary** — HU content is kozossegek-canonical per
  [[seo-cross-domain-canonical]], so meetapedia must not advertise a Hungary landing
  page. Full sitemap/noindex policy: [[indexing-strategy]].

## Navigation graph

- meetapedia home: each country heading (top-3 cities per country) links to its
  country page.
- `/cities` (unfiltered, meetapedia only): a "Countries" index grid — community-count
  badges, sorted by count — sits above the flat "All cities" grid, because 774 cities
  without grouping was unusable.
- Country page → "← All countries" back to `/cities`.

## Gotchas

- Country names come from `config/cities.yaml` `country:` fields — a rename there
  changes the slug and orphans the old URL (no redirect exists for renamed countries).
- On kozossegek.com the same routes work but only Hungary resolves (other countries'
  city lists are filtered to empty → redirect), keeping the HU site single-country.
