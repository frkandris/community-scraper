---
type: Post-mortem
title: The Search Index Collapse
description: Indexed pages fell from ~25,000 to 2,430 in the first week of June and never recovered — the trigger was fixed within a week, the reasons it stayed down were not.
tags: [seo, indexing, search-console, post-mortem, availability]
timestamp: 2026-08-21
resource: docs/wiki/pages/seo
---

# The Search Index Collapse

*The site was getting a hundred clicks a day. It has been getting almost none
since 5 June, and nobody knew because nothing measures it.*

## What the numbers say

Google Search Console, read on 2026-08-21 for `kozossegek.com`:

| | |
|---|---|
| clicks, last 3 months | 784 — nearly all before 6 June |
| impressions | 37.9K, same shape |
| **indexed pages** | **2,430**, down from ~25,000 in late May |
| not indexed | 34,900 |

The breakdown of *why* is the important part:

```
Crawled - currently not indexed      23,461
Discovered - currently not indexed    7,470
Page with redirect                    2,120
Blocked by robots.txt                 1,056
Duplicate without user-selected canonical 720
Excluded by 'noindex' tag                43
Server error (5xx)                       33
```

**"Crawled – currently not indexed" means Google fetched the page, looked at
it, and decided it was not worth indexing.** Twenty-three thousand times.

## The trigger

```
89819e8  2026-05-29  feat: shuffle community listings on all public pages
e7d373e  2026-06-05  revert: remove shuffle + outclick tracking (SEO traffic drop)
```

Listings were shuffled on every request, so every crawl of the same URL
returned different content and different internal links. A previous session
connected it to the traffic drop and reverted it within a week — correctly.

The revert did not bring the traffic back, and that is the part worth
understanding: **a de-indexing event is not undone by removing its cause.**
Every affected page has to be re-crawled and re-judged, and by then it was
being judged on its merits.

## Why it stayed down

Two reasons, both still live when this was written.

**The pages are thin.** 42,091 communities, **68% with no long description**.
A page's unique content is a name, a city, a topic and one line — inside a
219KB template whose navigation, footer and related listings are identical
across tens of thousands of pages. That is precisely what "crawled, not
indexed" describes, and it is why 23,461 sit in that bucket rather than being
ranked badly.

**The site keeps disappearing.** Three separate 404 episodes on 2026-08-21
alone: the pipeline blocks the event loop, `/healthz` times out, Docker marks
the container unhealthy and Traefik drops the route
([[2026-08-rate-limits-opened-the-breaker]] and [[production-monitoring]] have
the mechanism). Googlebot asking for a page it already knows and receiving a
bare 404 is the strongest de-indexing signal there is — far stronger than the
5xx it would treat as temporary. A site that vanishes for minutes several times
a day cannot recover an index.

## What follows from this

In order, because the order is the point:

1. **Availability is an SEO problem, not only an ops one.** Every remaining
   event-loop stall costs pages that are already being re-evaluated. This is
   why the blocking-write sweep and the widened liveness margin matter more
   than they looked.
2. **Enrichment is the ranking lever**, and it is now pointed at the primary
   market. It had been selecting `ORDER BY id` across the whole corpus, so it
   spent its budget on the international records that were imported first.
3. **More pages are worth almost nothing right now.** The marginal value of
   community 42,092 is close to zero while 23,461 existing pages are rejected.
   Collection ran at 63× extraction on 2026-08-20; that ratio is backwards for
   what the site needs.
4. **2,120 redirects and 1,056 robots-blocked pages** are unexamined. Both
   numbers are large enough to be a mistake rather than a design.

## The measurement that was missing

None of this was visible from inside the system. The daily report counts
records, pages and visitors; it never counted *indexed* pages, so a 90% index
collapse produced no alert and no line anywhere. GA4 showed the traffic fall as
a small number getting smaller.

## Update, 2026-08-21

A likelier explanation than thin content alone turned up when a live page was finally measured rather than read: 76% of every community page was a hidden city dropdown, byte-identical across all 42,091 of them, so two community pages agreed on roughly nine words in ten. The pages were thin *and* padded with shared boilerplate, which is the near-duplicate shape that produces "Crawled – currently not indexed" at this scale. See [[2026-08-boilerplate-outweighed-the-content]].
