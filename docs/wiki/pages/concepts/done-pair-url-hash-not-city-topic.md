---
type: Concept
title: Done-Pair Detection Uses url_hash, Not a city/topic JOIN
description: Done-pair detection resolves capped search URLs to hashes and checks every extraction family enabled for the current run mode.
tags: [done-pairs, url-hash, correctness, coverage, pipeline]
timestamp: 2026-07-10
resource: scraper/db.py
---

# Done-Pair Detection Uses url_hash, Not a city/topic JOIN

*Done detection is URL-hash based, run-mode aware, and fingerprint-complete across community, venue, and person extraction.*

## Why

`cache_pages.city` and `cache_pages.topic` are **last-write-wins** — the same URL reappears in many searches, and each save overwrites those columns with the latest (city, topic). Joining on them is therefore unreliable: a page could be attributed to the wrong pair. Both functions instead take the authoritative URL list from `search_cache[(city, topic)]`, hash each URL (`SHA-256[:16]`), and check membership in a scraped-hash set. This is a documented, hard-won correctness rule.

## Definition of "done"

A pair is fully processed when it has a `search_cache` entry **and** either its URL list is empty or every scraped URL inside `search_max_pages` is current for every extraction family enabled for the run. A visible community is not a shortcut: a stale community fingerprint keeps a green pair runnable. Venue/person fingerprints are required only when the pipeline's community-presence cost gate would call those extractors.

`search_only` uses the separate `get_collected_pairs`: the capped URL set must be scraped, but no LLM fingerprint is required. This lets failed fetches retry without paying for an already cached search.

Unscraped URLs are excluded from the extraction check, and URLs beyond the fetch cap are ignored. Otherwise a permanently failed or never-selected result could keep a pair runnable forever.

The `url_hash` formula is duplicated in three places (`cache.py`, `get_city_topic_states`, `get_fully_processed_pairs`) with no shared constant — see [[url-hash-triplicated]].
