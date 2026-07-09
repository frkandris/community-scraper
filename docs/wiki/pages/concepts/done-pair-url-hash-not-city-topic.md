---
type: Concept
title: Done-Pair Detection Uses url_hash, Not a city/topic JOIN
description: cache_pages.city/topic are last-write-wins, so done-pair detection resolves search-cache URLs to hashes instead of joining on those columns.
tags: [done-pairs, url-hash, correctness, coverage, pipeline]
timestamp: 2026-07-09
resource: scraper/db.py
---

# Done-Pair Detection Uses url_hash, Not a city/topic JOIN

*`get_fully_processed_pairs` and `get_city_topic_states` resolve each `search_cache` URL to its `url_hash` and look it up in `cache_pages`, rather than JOINing `cache_pages` on `city`/`topic`.*

## Why

`cache_pages.city` and `cache_pages.topic` are **last-write-wins** — the same URL reappears in many searches, and each save overwrites those columns with the latest (city, topic). Joining on them is therefore unreliable: a page could be attributed to the wrong pair. Both functions instead take the authoritative URL list from `search_cache[(city, topic)]`, hash each URL (`SHA-256[:16]`), and check membership in a scraped-hash set. This is a documented, hard-won correctness rule.

## Definition of "done"

A pair is fully processed when it has a `search_cache` entry **and** either the URL list is empty (search found nothing) **or** every *scraped* URL was extracted at the current fingerprint. Unscraped URLs (blocked/failed fetches) are excluded from the "all extracted" check — otherwise one permanently-blocked URL would keep a pair forever "not done." The amber-cells bug ([[2026-06-coverage-amber-cells]]) was exactly a mismatch between two functions on which URLs count.

The `url_hash` formula is duplicated in three places (`cache.py`, `get_city_topic_states`, `get_fully_processed_pairs`) with no shared constant — see [[url-hash-triplicated]].
