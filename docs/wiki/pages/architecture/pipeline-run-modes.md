---
type: Architecture
title: Pipeline Run Modes
description: full / ai_only / search_only / revalidate control how much work runs per city×topic pair.
tags: [pipeline, run-modes, orchestration]
timestamp: 2026-07-10
resource: scraper/pipeline.py
---

# Pipeline Run Modes

*Four modes control how much work the pipeline does per city×topic pair.*

## Modes

| Mode | Search | Fetch | Extract | When used |
|------|--------|-------|---------|-----------|
| `full` | ✓ | ✓ | ✓ | Default scheduled run ("Smart" in UI) |
| `ai_only` | ✗ | ✗ | ✓ | Re-extract from cached pages; no web requests |
| `search_only` | ✓ | ✓ | ✗ | Cost-saver collector; cache search results and page text |
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

Before entering the city×topic loop, `run_pipeline()` chooses a mode-specific prefilter:

- `search_only` → `get_collected_pairs(db, search_max_pages)`: every selected URL has page text.
- `full` / `ai_only` → `get_fully_processed_pairs(...)`: every selected scraped URL is current for all enabled community/venue/person fingerprints.

Existing visible communities never override fingerprint freshness. A prompt/model change therefore makes green pairs runnable again, while disabled extraction families do not block completion.

These pairs are subtracted from `all_pairs` before `_run_full` / `_run_ai_only` is called, so there's zero loop overhead — no log entry, no UI update, no DB reads per skipped pair.

## Cache-skip flags

`cache.skip_scraped` and `cache.skip_extracted` control per-URL skipping:
- `skip_scraped: true` + `search_ttl_days: 3650` → never re-search a pair once indexed
- `skip_extracted: true` → skip pages already extracted with current fingerprint

## Related

- [[extraction-fingerprint-cache]]
- [[search-ttl-3650-days]]
