---
type: Architecture
title: Pipeline Run Modes
description: full / ai_only / revalidate control how much work runs per city×topic pair.
tags: [pipeline, run-modes, orchestration]
timestamp: 2026-07-09
resource: scraper/pipeline.py
---

# Pipeline Run Modes

*Three modes control how much work the pipeline does per city×topic pair.*

## Modes

| Mode | Search | Fetch | Extract | When used |
|------|--------|-------|---------|-----------|
| `full` | ✓ | ✓ | ✓ | Default scheduled run ("Smart" in UI) |
| `ai_only` | ✗ | ✗ | ✓ | Re-extract from cached pages; no web requests |
| `revalidate` | ✗ | ✗ | special | Re-validates existing communities for accuracy |

## Startup progression

On each restart, the pipeline mode advances:
- Interrupted or failed → retry same mode
- Previous was `revalidate` → run `ai_only`
- Previous was `ai_only` → run `full`
- Previous was `full` → run `full` again

This ensures that after a quiet period, the first restart re-extracts stale pages cheaply (`ai_only`), then does a full fresh search on the next cycle.

## Priority ordering (Hungary first)

The scheduled run in `main.py` splits cities into three groups and runs them sequentially:
1. Hungary (primary market)
2. Sweden (secondary market)
3. Everything else

Each group is a separate `run_pipeline()` call so progress is visible in coverage per group.

## Done-pair pre-filter

Before entering the city×topic loop, `run_pipeline()` calls `get_fully_processed_pairs(db_path, current_fp)` to build a set of pairs to skip entirely:

```sql
SELECT sc.city, sc.topic FROM search_cache sc
WHERE json_array_length(sc.urls) = 0
  OR (
    EXISTS (SELECT 1 FROM cache_pages cp WHERE cp.city=sc.city AND cp.topic=sc.topic)
    AND NOT EXISTS (
        SELECT 1 FROM cache_pages cp
        WHERE cp.city=sc.city AND cp.topic=sc.topic
        AND (cp.extract_fingerprint IS NULL OR cp.extract_fingerprint != ?)
    )
  )
```

A pair qualifies as "done" if either:
- The search returned 0 URLs (nothing to extract), OR
- At least one `cache_pages` row exists AND none have a stale/null fingerprint

These pairs are subtracted from `all_pairs` before `_run_full` / `_run_ai_only` is called, so there's zero loop overhead — no log entry, no UI update, no DB reads per skipped pair.

## Cache-skip flags

`cache.skip_scraped` and `cache.skip_extracted` control per-URL skipping:
- `skip_scraped: true` + `search_ttl_days: 3650` → never re-search a pair once indexed
- `skip_extracted: true` → skip pages already extracted with current fingerprint

## Related

- [[extraction-fingerprint-cache]]
- [[search-cache]]
