---
type: Decision
title: search_ttl_days = 3650
description: "TTL set to ~10 years: index the world first, worry about freshness later."
tags: [decision, cache, ttl, coverage]
timestamp: 2026-07-24
resource: config/settings.yaml
---

# search_ttl_days = 3650

*Changed from 7 days to 3650 (≈10 years) in May 2026.*

## Why

At the scale of 290+ Swedish municipalities × N topics × N countries, re-scraping already-indexed pairs every 7 days is impractical. The pipeline would spend all its time re-validating work instead of indexing new cities.

**Priority**: index the whole world first, then worry about freshness.

## Trade-off

- ✅ Pipeline always makes forward progress on new city×topic pairs
- ✅ No wasted API calls / search quota on pairs already indexed
- ❌ Community data can go stale (groups dissolve, move, change name)
- ❌ New communities that appear after initial indexing won't be found

## Mitigation

(The `revalidate` run mode used to re-validate existing communities without re-searching; it was removed 2026-07-23 with [[admin-simplification-2026-07]]. Content staleness is now handled by fingerprint-driven `ai_only` re-extraction only.)

## How to change

Edit `config/settings.yaml → cache → search_ttl_days`. Setting it back to `7` re-enables weekly re-scraping. This is a hot config — the scheduler picks it up on next run without restart.

## Related

- [[pipeline-run-modes]]
- [[done-pair-url-hash-not-city-topic]]
