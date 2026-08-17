# DataForSEO beyond organic SERPs — Business Listings Search (2026-08-17)

Notes from reading <https://docs.dataforseo.com/v3/> against the question "is
there something there that suits us better than what we pull today".

## What we pull today

`serp/google/organic/task_post` → `task_get/regular`, standard mode at
`priority: 2` (~$1.2/1K). From each response we keep **only `type == "organic"`
items**, and of those only `url`, `title`, `description` (`search.py:_parse_tasks`).
The url then goes through fetch → trafilatura → LLM extraction to become a
community record.

So per (city, topic) pair: 2–3 paid queries, ~5 page fetches, ~5 LLM calls, and
whatever the model can find in unstructured page text.

## Business Listings Search

`POST /v3/business_data/business_listings/search/live`

A query against DataForSEO's own database of Google Maps business entities —
**not** a live SERP, so no crawling and no LLM.

| | |
|---|---|
| filter | `location_coordinate: "lat,lon,radius_km"`, `categories` (up to 10), `title`, `description`, `is_claimed`, up to 8 `filters`, 3 `order_by` |
| returns | title, address, phone, **url**, domain, category, rating + vote count, work hours, coordinates, claimed status, attributes, Maps check url |
| limit | 100 default, **1,000 max** per request, `offset_token` for more |
| rate | up to 2,000 calls/min, 30 simultaneous |
| cost | `"cost": 0.0109` in the docs example — **with `limit: 3`** |

`business_listings/categories` lists the 5,000+ category slugs and is **free**.
Community-relevant ones exist: `sports_club`, `community_center`,
`non_profit_organization`, `dance_school`, `choir`, `senior_center`,
`volunteer_organization`, `church`, `music_school`.

## Why this is interesting

It attacks both costs at once. The filter is coordinate + radius rather than a
text query, so **one task can cover a whole town or region** and return up to
1,000 structured organisations — with address, phone and opening hours, fields
our LLM extraction frequently cannot recover from a page at all.

Rough comparison, if the price is anywhere near flat:

| | today | Business Listings |
|---|---|---|
| paid calls per town | 2–3 per topic | 1 per radius query |
| page fetches | ~5 per pair | 0 |
| LLM calls | ~5 per pair | 0 |
| structured address/phone/hours | rarely | always |

## What is NOT established

Two things, and the second is the one that decides it.

**Price at scale.** `0.0109` came from an example request with `limit: 3`. The
docs do not say whether the price is flat per task or scales with results. At
1,000 results it could be the same cent or a hundred times that; both are still
cheap per organisation, but the number is unverified.

**Coverage.** This is a database of *Google Maps business entities*. Our corpus
is community groups — Hungarian `egyesület`, village choirs, `nyugdíjas klub` —
and a great many of them have no Maps presence at all. They surface today
precisely because the organic route finds them on municipal websites, local news
and federation pages. If Maps coverage is thin, Business Listings is a directory
of gyms and dance studios: a useful **seeding** pass, not a replacement.

There is also a corpus-composition risk worth naming: Maps skews commercial, and
leaning on it would quietly shift the site from community groups toward
businesses that happen to be in a category.

## Next step

`scripts/probe_business_listings.py` — one live request, about a cent, prints
what comes back for a city and category plus the cost per returned listing:

```bash
DATAFORSEO_LOGIN=… DATAFORSEO_PASSWORD=… \
  .venv/bin/python scripts/probe_business_listings.py \
  --city Szentendre --radius 10 --categories sports_club,community_center
```

Decide from the output, not from this page. Compare what comes back against
what the site already lists for that town: the question is not "are there
results" but "are they the kind of thing we index, and do we already have them".
