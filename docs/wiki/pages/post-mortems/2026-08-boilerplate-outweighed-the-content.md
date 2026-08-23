---
type: PostMortem
title: Boilerplate Outweighed the Content
description: Every community page shipped a 176 KB hidden city dropdown — 76% of the document, identical on all 42,091 pages — and /helyszinek rendered all 7,676 venues in 15.5 MB over 34 seconds on the event loop.
tags: [seo, performance, event-loop, near-duplicate, post-mortem, availability]
timestamp: 2026-08-21
resource: scraper/web/templates/public_community.html
---

# Boilerplate Outweighed the Content

*A customer-acquisition round started by fetching one live page. It answered a
question that had been open for a week.*

## What was found

Fetching `https://kozossegek.com/budapest/crokinole-klub` on 2026-08-21:

| | |
|---|---|
| Document | 231 KB |
| `<option>` elements | 3,922 |
| Bytes inside them | 176 KB — **76% of the page** |
| Visible words | 5,089 |
| Visible words that were the community | **510** |

Nine words in ten on a community page were a dropdown of every configured city,
**byte-identical across all 42,091 community pages**, nested inside two `hidden`
divs on a branch of a report form that most visitors never open.

Then `https://kozossegek.com/helyszinek`:

| | |
|---|---|
| Document | **15.5 MB** |
| Time to serve | **34.4 seconds** |
| Venue cards rendered | 7,676 |
| Links | 28,777 |

`get_all_venues`, `init_db` and the 15 MB render all ran **on the event loop**.
One request to that URL — a visitor's, or Googlebot's — stalled every other
request on the site for half a minute.

## Why it matters

Two separate failures, one root shape: a page rendering something proportional
to the whole database.

**The community pages look like near-duplicates.** A crawler fetching two of
them sees documents that agree on ~90% of their text. 23,461 pages sit in
"Crawled – currently not indexed" ([[2026-06-search-index-collapse]]) — Google
fetched them and declined. Thin content was the working theory and it was half
right: the pages are thin *and* padded, which is worse than thin, because the
padding is shared.

**The venues page is an availability incident on demand.** The site losing
minutes several times a day had been read all week as a deploy problem and
answered with health-check tuning and a route-recovery script. Those were real
fixes for a different cause. A 34-second blocking render is a 34-second outage,
and Googlebot receiving a timeout on a page it has indexed is the strongest
de-indexing signal there is ([[production-monitoring]]).

## The rule already existed

`CLAUDE.md` has said this since the Tailwind CDN work:

> Never server-render large lists in admin templates — load them via a JSON
> endpoint + `DocumentFragment` client-side.

It was scoped to *admin* templates, where the cost is one operator's patience.
Nobody restated it for the public templates, where the cost is 42,091 pages and
the crawl budget. `_render_people` had independently arrived at the right
answer — its docstring says the person list appears only after a city is picked,
"avoids dumping every city/person" — and the venues page beside it did the
opposite.

## The fix

- The wrong-city picker on community pages ships empty and fetches
  `/api/cities.json` the first time it is opened. One cacheable download,
  site-scoped, shared by every page.
- `/helyszinek` with no filter renders a **city index** — name, count, link —
  instead of every card. The cards moved one click away to `?city=…`.
- `get_all_venues` and `get_all_persons` moved off the event loop.
- The submit form now offers only the current site's cities. It was offering
  all 3,914 on both domains, so a visitor to the Hungarian site could submit a
  Swedish community to a site that will never show it. That select stays
  server-rendered: it is `required` and posted as a plain form, and on the page
  where a broken control costs the conversion outright, half the weight beats
  all of it.

`tests/test_community_page_weight.py` and `tests/test_public_page_weight.py`
hold the line — they assert against configured-but-unrendered filler cities and
venues, so a regression fails rather than merely getting slower.

## The lesson

Measure the artefact, not the template. Every one of these numbers came from
one `curl` and a regex; none of them was visible from reading the Jinja, where
a `{% for %}` over `all_cities` looks like every other loop on the page.

Related: [[acquisition-funnel]], [[2026-06-search-index-collapse]],
[[production-monitoring]].
