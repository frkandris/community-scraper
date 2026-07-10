---
type: Hack
title: url_hash Formula Is Duplicated Across Modules
description: SHA-256(url)[:16] is repeated across cache, DB, pipeline, and web paths; every copy must remain byte-for-byte compatible.
tags: [url-hash, duplication, correctness, done-pairs]
timestamp: 2026-07-10
resource: scraper/cache.py
---

# url_hash Formula Is Duplicated Across Modules

*`hashlib.sha256(url.encode()).hexdigest()[:16]` has one canonical meaning but no canonical implementation.*

The formula appears in `cache.py`, several `db.py` pair/cache helpers, pipeline re-extraction, and admin web routes. A mismatch means a saved page cannot be found through search-cache URLs, breaking [[done-pair-url-hash-not-city-topic]], false-positive invalidation, or manual cache actions.

Changing the algorithm or length therefore requires a repository-wide search plus data migration. Centralizing it would be safer; until then, regression coverage around done pairs and shared-URL attribution is the guardrail. See [[2026-06-coverage-amber-cells]] and [[false-positive-injection]].
