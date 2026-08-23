---
type: Concept
title: Done-Pair Detection Uses url_hash, Not a city/topic JOIN
description: Done-pair detection resolves capped search URLs to hashes and checks every extraction family enabled for the current run mode.
tags: [done-pairs, url-hash, correctness, coverage, pipeline]
timestamp: 2026-07-14
resource: scraper/db.py
---

# Done-Pair Detection Uses url_hash, Not a city/topic JOIN

*Done detection is URL-hash based, run-mode aware, and fingerprint-complete across community, venue, and person extraction.*

## Why

`cache_pages.city` and `cache_pages.topic` are **last-write-wins** — the same URL reappears in many searches, and each save overwrites those columns with the latest (city, topic). Joining on them is therefore unreliable: a page could be attributed to the wrong pair. Both functions instead take the authoritative URL list from `search_cache[(city, topic)]`, hash each URL (`SHA-256[:16]`), and check membership in a scraped-hash set. This is a documented, hard-won correctness rule.

## Definition of "done"

A pair is fully processed when it has a `search_cache` entry **and** either its URL list is empty or every scraped URL inside `search_max_pages` is current for every extraction family enabled for the run. A visible community is not a shortcut: a stale community fingerprint keeps a green pair runnable. Venue/person fingerprints are required only when the pipeline's community-presence cost gate would call those extractors.

`search_only` uses the separate `get_collected_pairs`: `search_cache.collected_at` must be non-null. The marker is written after all selected URLs were attempted, regardless of individual fetch success. This resumes process-level interruptions without allowing permanently unreadable URLs to replay a pair forever.

Unscraped URLs are excluded from the extraction check, and URLs beyond the fetch cap are ignored. Otherwise a permanently failed or never-selected result could keep a pair runnable forever.

The `url_hash` formula is repeated across cache, DB, pipeline, and web helpers with no shared implementation — see [[url-hash-triplicated]].

## Cost (2026-08-23)

`/v1/backlog` answered **524 after 125 seconds** in production. The filter loads
every scraped `cache_pages` row — ~207K of them — and walks ~54K `search_cache`
rows hashing their URLs.

Three things were measured on a 6.15 GB synthetic copy, in this order, and the
first two guesses were wrong:

| Change | Time |
|---|---|
| Baseline, small (2 KB) blobs | 0.71 s |
| Baseline, realistic 30 KB blobs | 13.23 s |
| …asking for fewer columns | 11.03 s |
| …reading `records_count` instead of the blob | 11.03 s |
| **…plus a covering index** | **0.31 s** |

And on the shape production actually has — half the pages scraped and not yet
extracted, 5.95 GB:

| State | Time |
|---|---|
| Mid-migration, every row still NULL | 73.53 s |
| Backfill itself (200K rows, chunked) | 139.4 s |
| **After it, zero NULL rows left** | **0.38 s** |

`/v1/backlog` can still time out during those two minutes after a deploy. That
is the migration, not the steady state.

The JSON functions were not the problem and neither was the column list.
**SQLite was scanning the table**, and the table is 30 KB a row: naming three
small columns still walks six gigabytes of pages, because the row is where the
blob lives. `idx_cache_pages_done` covers `(url_hash, extract_fingerprint,
records_count)` with the same `WHERE scraped_at IS NOT NULL` the query uses, so
the scan reads a few megabytes of index instead. `EXPLAIN QUERY PLAN` goes from
`SCAN cache_pages` to `SCAN cache_pages USING INDEX idx_cache_pages_done`, and
`tests/test_done_pairs.py` asserts on exactly that.

`records_count` exists so the index *can* cover the query: it replaces
`json_type(data,'$.records') = 'array'` and `json_array_length(...)`, which
cannot be indexed.

Its three values matter, and getting them wrong once quietly undid the whole
optimisation:

| Value | Meaning |
|---|---|
| `-1` | scraped, never extracted |
| `0` | extracted, found no communities — a **finished** page |
| `n` | extracted, found n |
| `NULL` | the backfill has not reached this row **yet** |

The first version used NULL for both "never extracted" and "not backfilled",
and the backfill only filled rows whose blob already held a `records` array.
Every scraped-but-unextracted page therefore stayed NULL forever, so the blob
fallback opened its ~30 KB row on every scan — and those pages are most of what
a backlog *is*. The sentinel is what lets NULL mean exactly one thing, and lets
the fallback empty out.

Every writer sets it through `_records_count`, including the plain scrape,
which is the most common write in the system.

The venue and person columns are only selected when those families are being
run, which keeps the lean path off the blob entirely. That change alone bought
almost nothing, and is kept because it is what makes the covering index
possible.

## Migrating without stalling the writer

`records_count` needs a backfill, and the naive one is a single
`UPDATE cache_pages SET …` over every row. Those rows carry a ~30 KB blob, so
that is a multi-minute rewrite holding SQLite's **single writer slot**, with
the crawler and every request queued behind it. `_backfill_records_count`
chunks it 2,000 rows at a time with a commit between, so other writers
interleave and the worst case is a slow migration rather than a stalled app.

It runs as a background task started after boot, not inside `init_db` — that
function is on the startup path and is called from a dozen routes, so a
two-minute migration there is a two-minute deploy stall or a two-minute
request. Measured: ~9.3 s per 20,000 rows, ~97 s for the corpus.

Correctness deliberately does **not** wait for it. The filter reads the blob
for whatever rows are still NULL — one extra statement, scoped by
`records_count IS NULL`, returning nothing once the backfill is done. The
alternative, treating an un-backfilled row as unextracted, would have sent the
whole corpus back for re-extraction the moment this shipped: 207K pages at
~3.3 calls each, against a free fleet that manages ~650 pages a day. A year of
work, caused by a migration being slow.

Both reads share **one transaction**. They are separate statements and the
backfill commits concurrently, so without a snapshot a row can flip
`NULL -> count` between them: the bulk scan calls it unextracted and the
fallback no longer matches `records_count IS NULL` to correct it. The page then
reads as outstanding and its pair is re-extracted — the exact outcome the
fallback exists to prevent, in exactly the window it exists for.

`records_count` mirrors `$.records`, so the two leave together:
`invalidate_extraction_cache` clears the column alongside the blob key. The
done-pair verdict was already right without that (the fingerprint goes NULL and
fails the currency check first), but the backfill only fills NULLs, so a stale
non-NULL would never be repaired.

**The lesson worth keeping:** measure the artefact at production scale before
choosing the fix. A synthetic database with small blobs said this query was
fine (0.71 s); the same query with real blobs took 18× longer, and the fix that
looked obvious from reading the SQL — fewer JSON calls — bought 17% of what a
one-line index bought.
