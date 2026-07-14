---
type: Architecture
title: Pipeline Run Modes
description: full / ai_only / search_only / revalidate control how much work runs per city×topic pair.
tags: [pipeline, run-modes, orchestration]
timestamp: 2026-07-14
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

## Priority ordering

The bounded saver jobs in `main.py` run Sweden → everything else → Hungary, putting the current expansion target first. Startup recovery retains Hungary → Sweden → everything else so its historical resume semantics do not change.

Each group is a separate `run_pipeline()` call so progress is visible in coverage per group.

## Done-pair pre-filter

Before entering the city×topic loop, `run_pipeline()` chooses a mode-specific prefilter:

- `search_only` → `get_collected_pairs(...)`: the search row has `collected_at`, written after every selected URL was attempted.
- `full` / `ai_only` → `get_fully_processed_pairs(...)`: every selected scraped URL is current for all enabled community/venue/person fingerprints.

Existing visible communities never override fingerprint freshness. A prompt/model change therefore makes green pairs runnable again, while disabled extraction families do not block completion.

`search_only` has a strict post-fetch exit and cannot read extraction caches or persist communities, venues, or persons. This invariant prevents collection runs from replaying stale entity data; see [[2026-07-search-only-cache-replay]].

These pairs are subtracted from `all_pairs` before `_run_full` / `_run_ai_only` is called, so there's zero loop overhead — no log entry, no UI update, no DB reads per skipped pair.

## Cache-skip flags

`cache.skip_scraped` and `cache.skip_extracted` control per-URL skipping:
- `skip_scraped: true` + `search_ttl_days: 3650` → never re-search a pair once indexed
- `skip_extracted: true` → skip pages already extracted with current fingerprint

## Related

- [[extraction-fingerprint-cache]]
- [[search-ttl-3650-days]]
