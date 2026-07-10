---
type: Integration
title: DataForSEO
description: The sole paid search provider — live mode ($2/1K, seconds) vs standard task queue ($0.6/1K, minutes); quota and transient failures raise typed errors that are never cached.
tags: [integration, search, dataforseo, cost, quota]
timestamp: 2026-07-10
resource: scraper/search.py
---

# DataForSEO

*Google SERP results as a paid API. Two modes trade latency for money; every successful
search is cached forever, every failure raises and is retried next run.*

## Contract

- **Auth**: `DATAFORSEO_LOGIN` + `DATAFORSEO_PASSWORD` env vars (HTTP Basic).
- **Endpoints** (`DataForSEOClient` in `scraper/search.py`):
  - live: `POST /v3/serp/google/organic/live/regular` — result in one call, ~$2/1K queries.
  - standard: `POST /v3/serp/google/organic/task_post` then poll `task_get` —
    ~70% cheaper (~$0.6/1K) but results arrive after 0.5–5 minutes of queueing.
- **Mode switch**: `search.dataforseo_mode: standard` in `config/settings.yaml`
  (production setting since 2026-07-09). Manual dashboard runs feel slow in standard
  mode — that is the task queue, not a bug (see [[cost-saver-schedule]]).
- Location/language are derived from the city's locale; the Norwegian locale quirk in
  [[pyyaml-no-norway-boolean]] applies to the request boundary.

## Quirks and hard-won rules

- **Never cache failures.** Quota exhaustion raises `SearchQuotaError` (provider is
  skipped for the rest of the run via `FallbackSearchClient._exhausted`); transient
  HTTP/network failures raise `SearchUnavailableError`. Neither is written to
  `search_cache` — caching an empty result for a failed call would permanently mark
  the pair as "searched, found nothing" (see [[searchquotaerror-reraise-ordering]]).
- **Empty ≠ failure.** A *successful* search with zero results IS cached — otherwise
  the same empty query is re-paid on every run ([[cost-optimization-2026-07]]).
- **`stop_after` short-circuit**: `FallbackSearchClient.search_all(stop_after=…)`
  stops issuing paid queries once enough unique URLs are collected for the pair.
- Costs accrue per query, so the pipeline's done-pair pre-filter
  ([[done-pair-url-hash-not-city-topic]]) is the main spend control: fully covered
  pairs never reach the client.

## Where it runs

The saver collector cron (01:00→16:20 UTC, `search_only` mode) does effectively all
DataForSEO traffic; see [[cost-saver-schedule]]. The extraction side is [[deepseek]].
