---
type: Hack
title: cache_pages Is a Non-Transactional Read-Modify-Write Blob
description: CacheManager reads the JSON blob, mutates it in Python, and writes it back across two separate connections — concurrent writers to the same URL can lose updates.
tags: [cache, concurrency, sqlite, blob]
timestamp: 2026-07-09
resource: scraper/cache.py
---

# cache_pages Is a Non-Transactional Read-Modify-Write Blob

*Every `CacheManager` method does `load_cache_page` → mutate the Python dict → `save_cache_page`, across two independent short-lived connections. There is no transaction spanning the read and the write.*

Concurrent writers to the same `url_hash` therefore race: last-writer-wins on the **whole blob**, so an update can be silently lost. In practice the pipeline processes a given URL serially, so this rarely bites — but admin manual re-scrape/re-extract buttons and the queue worker can touch the same page as a run. Keep writes to one page single-threaded.

Related blob semantics: `save_extracted` nulls all `enrich_*` markers (fresh extraction invalidates prior enrichment); deletes are soft (pop keys) except `delete_entry`; the person cache is keyed `f"{city}/{topic}"` *within* a page blob, so one URL can hold person lists for multiple contexts. See [[persistence-layer]].
