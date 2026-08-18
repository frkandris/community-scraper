# DataForSEO Business Listings — investigated and rejected (2026-08-17/18)

**Verdict: not used.** Measured on live data, then dropped. This page exists so
the question is not reopened from the documentation alone, which makes the
endpoint look far better than it is for our corpus.

## The question

Extraction stopped being the bottleneck on 2026-08-18: the window found 27 pairs
of work, made 20 LLM calls and finished in 33 minutes
([[2026-08-rate-limits-opened-the-breaker]] has the throughput story). The
constraint moved to **collection**, which is paid DataForSEO. So: does DataForSEO
sell something better suited to us than the organic SERPs we buy today?

## What we buy today

`serp/google/organic/task_post` → `task_get/regular`, standard mode at
`priority: 2` (~$1.2/1K). We keep **only `type == "organic"` items**, and of
those only `url`, `title`, `description` (`search.py:_parse_tasks`). Each url
then goes through fetch → trafilatura → LLM extraction to become a record.

## The candidate

`POST /v3/business_data/business_listings/search/live` — a query against
DataForSEO's own database of Google Maps business entities. No SERP, no
crawling, no LLM.

| | |
|---|---|
| filter | `location_coordinate: "lat,lon,radius"` (radius in **whole** km, min 1), `categories` (≤10 slugs), `title`, `is_claimed`, `filters`, `order_by` |
| returns | title, address, phone, url, domain, category, rating, work hours, coordinates, Maps check url |
| limit | 100 default, 1,000 max, `offset_token` beyond |
| categories list | `business_listings/categories`, free; `tasks[].result` is a flat list of `{category_name, business_count}` |

Note the asymmetry that cost us a run: results carry **human-readable**
categories ("Community center"), the filter takes **slugs**
(`community_center`).

## What the live probe showed

One request, 5 km around Szentendre, `limit: 30`, no category filter.

**Price is not flat.** Two points — $0.0109 at `limit: 3` (docs example) and
$0.0228 at `limit: 30` (measured) — fit base ≈ $0.0096 plus ≈ $0.00044 per
result. A 1,000-result page would cost ≈ $0.45, or $0.00045 each. Cheap, but
roughly a factor of two against what we pay now, not a factor of a hundred.

**Relevance is the problem.** 4,713 businesses lie within 5 km. Of the 30
returned, three were things this site indexes: Pomázi Horgászegyesület, the
Felhangoló Központ community centre, and a Serbian Orthodox church. The rest
were a masonry contractor, a motorcycle dealer, a **bus stop**, a steel erector,
a restaurant, a hair salon, a tobacco shop, a printer repair service, a beauty
salon, an auto repair shop and a **sculpture**.

At ~10% signal the cost per *usable* record is ≈ $0.0076 — four to seven times
worse than the ~$0.001 a community costs today.

## Why we stopped there rather than tuning the filter

Category filtering would have raised the signal, and the arithmetic could have
inverted. It was not pursued, because the shape of the miss is the point: this
is a database of **Google Maps business entities**, and the corpus we are
building is community groups — `egyesület`, village choirs, `nyugdíjas klub`.
Those surface today precisely *because* the organic route finds them on
municipal websites, local news and federation pages. They are largely not on
Maps at all, so no filter recovers them.

There is a second reason, and it is about what the site is. Maps skews
commercial. Leaning on it would quietly shift the index from community groups
toward businesses that happen to sit in a plausible category — a worse product
that would look like growth in the record count.

## If it is ever reopened

Reproduce with one request; it costs about a cent:

```bash
curl -u "$DATAFORSEO_LOGIN:$DATAFORSEO_PASSWORD" \
  -H 'Content-Type: application/json' \
  -d '[{"location_coordinate":"47.67,19.07,5","limit":30}]' \
  https://api.dataforseo.com/v3/business_data/business_listings/search/live
```

Ask the coverage question first, not the price question: of what comes back, how
much would this site actually list, and how much of that is already indexed?
`scripts/probe_business_listings.py` did this and was removed once answered —
`git log --diff-filter=D -- scripts/probe_business_listings.py` finds it.
