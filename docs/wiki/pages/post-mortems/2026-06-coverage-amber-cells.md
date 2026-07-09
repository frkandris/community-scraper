---
type: Post-mortem
title: Coverage Amber Cells Never Turned Blue (2026-06-04)
description: get_fully_processed_pairs() and get_city_topic_states() disagreed on which URLs count as done.
tags: [incident, coverage, fingerprint, url-hash]
timestamp: 2026-07-09
resource: scraper/db.py
---

# Coverage Amber Cells Never Turned Blue

**Date:** 2026-06-04  
**Symptom:** After a pipeline run processed a (city, topic) pair, cells remained amber (searched but not extracted) even after reload and repeated runs.

## Root Cause

`get_fully_processed_pairs()` built the set of "processable" URLs from all rows in `search_cache.urls` — including URLs that had never been scraped. It then checked whether all those URLs had a `cache_pages` row at the current `extract_fingerprint`.

`get_city_topic_states()` — which drives the badge colour — only counts **scraped** URLs (rows where `scraped_at IS NOT NULL`).

The two functions disagreed on what "all URLs" meant. A pair with 10 search results but only 3 successfully scraped would never satisfy `get_fully_processed_pairs()` (it required all 10 to be extracted), so it was always re-queued. But `get_city_topic_states()` saw those 3 scrapes extracted → showed them as "done" → yet the pair kept being retried → badge logic was confused.

## Fix

`get_fully_processed_pairs()` now computes `scraped_hashes` first (URLs where `scraped_at IS NOT NULL`) and intersects with the search results before checking fingerprints:

```python
scraped_hashes = {r[0] for r in conn.execute(
    "SELECT url_hash FROM cache_pages WHERE scraped_at IS NOT NULL"
)}
processable = [u for u in urls if _url_hash(u) in scraped_hashes]
if processable and all(_url_hash(u) in current_fp_hashes for u in processable):
    result.add((city, topic))
```

Both functions now agree: "done" means all **scraped** URLs are extracted at the current fingerprint.

## Lesson

Any two functions that must agree on "what counts as a URL for this pair" must use the same filter. Blocked/unscrapable URLs silently break that agreement.
