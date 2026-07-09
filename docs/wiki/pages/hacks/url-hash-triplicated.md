---
type: Hack
title: url_hash Formula Is Duplicated in Three Places
description: SHA-256(url)[:16] is re-implemented in cache.py, get_city_topic_states, and get_fully_processed_pairs with no shared constant — they must stay identical.
tags: [url-hash, duplication, correctness, done-pairs]
timestamp: 2026-07-09
resource: scraper/db.py
---

# url_hash Formula Is Duplicated in Three Places

*`hashlib.sha256(url.encode()).hexdigest()[:16]` appears independently in `cache.py:_url_hash`, `db.py:get_city_topic_states`, and `db.py:get_fully_processed_pairs`.*

All three must produce the same hash or [[done-pair-url-hash-not-city-topic|done-pair detection]] silently breaks — a page saved under one hash wouldn't be found under another, so pairs would never be marked done and would reprocess forever. There is no shared helper; changing the hash (algorithm or length) means editing all three. This is the kind of duplication the amber-cells post-mortem ([[2026-06-coverage-amber-cells]]) turned on.
