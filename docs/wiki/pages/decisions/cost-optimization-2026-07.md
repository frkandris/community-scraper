---
type: Decision
title: Cost Optimization Round (2026-07)
description: Cost controls reduce paid search and LLM work through caching, query short-circuiting, venue gates, off-peak extraction, standard search, and topic tiers.
tags: [cost, dataforseo, llm, cache, tiering, off-peak]
timestamp: 2026-07-10
resource: scraper/pipeline.py
---

# Cost Optimization Round (2026-07)

*A repo-wide cost review found the one-off world sweep cheap (~$90 worst case at live DataForSEO pricing) but several **recurring leaks**. All fixes below shipped together.*

## Recurring leaks closed

- **Empty searches are now cached.** Previously `save_search_cache` only ran when URLs were found — a 0-result (or all-blocked-domain) search was re-paid on *every* run, and **twice per run** because the catch-up pass saw the pair as uncovered. Now every search is recorded (empty list = pair correctly done per [[done-pair-url-hash-not-city-topic]]), in Full Refresh mode too, and with the **full** URL list (not the `[:max_pages]` cap), so raising `max_pages` later needs no re-search.
- **Venue extraction gated on communities** — same rationale as the person-skip: most pages yield 0 communities, and each used to get a venue LLM call anyway. Trade-off accepted: venues on community-less pages are not captured.
- **Canonical venue/person fingerprints** — cache keys remain stable if a fallback provider is ever configured again. See [[canonical-fingerprint-provider-shift]].

## Fewer queries per pair

`FallbackSearchClient.search_all` gained `stop_after` (pipeline passes `search_max_pages * 2`): once enough unique URLs are collected, remaining paid queries are skipped. Failover semantics also improved: a quota error mid-pair keeps already-collected results and moves only the *remaining* queries to the next provider. Fruitful pairs typically drop from 3 queries to 1.

## Cheaper providers / windows

- **`search.dataforseo_mode: standard`** is active in `settings.yaml`: task_post + task_get queue at **$0.6/1K vs $2/1K**, but ~0.5–5 min latency per query. The code fallback remains `live`; enrichment always constructs a live-mode client.
- **Off-peak extractor** (`schedule.saver_enabled: true`, `extract_cron: 35 16 * * *` UTC): DeepSeek work is boxed into the configured discount window. See [[cost-saver-schedule]] and [[scheduler-disabled-no-cron]].

## Topic tiering

`CityConfig.topic_tier: core` limits a city to `pipeline.core_topics` (12 topics: running, music, choir, dance, hiking, cycling, fitness, board_games, volunteering, religion, senior, baby). Stamped on the **260 smallest Swedish kommuner** (top 30 keep the full 36 topics) → **6,240 pairs eliminated** (~19K queries + downstream fetch/LLM). Tiered-out pairs are fully frozen: no search *and* no re-extraction (existing data keeps serving). Every pipeline mode applies the same `_tier_allows` guard.

## Still open (not implemented)

- Enrichment search results are not cached across re-extractions.
- The biggest "don't burn money" rule remains operational: after prompt edits with still-valid results, use `POST /admin/api/restamp-fingerprints` instead of letting the fingerprint change re-extract the world ([[extraction-fingerprint-cache]]).

**Follow-up 2026-07-10:** `_run_ai_only` now receives pair-scoped false-positive examples. Adding/removing one invalidates only that pair's community extraction cache; a global extraction rule still invalidates all. Raw page text is retained, so the correction costs LLM calls but no search/fetch calls.
