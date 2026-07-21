---
type: Integration
title: DataForSEO
description: The sole paid search provider — live mode ($2/1K, seconds) vs standard task queue ($0.6/1K, minutes); quota and transient failures raise typed errors that are never cached.
tags: [integration, search, dataforseo, cost, quota]
timestamp: 2026-07-21
resource: scraper/search.py
---

# DataForSEO

*Google SERP results as a paid API. Two modes trade latency for money; every successful
search is cached forever, every failure raises and is retried next run.*

## Contract

- **Auth**: `DATAFORSEO_LOGIN` + `DATAFORSEO_PASSWORD` env vars (HTTP Basic).
- **Endpoints** (`DataForSEOClient` in `scraper/search.py`):
  - live: `POST /v3/serp/google/organic/live/regular` — result in one call, ~$2/1K queries.
  - standard: `POST /v3/serp/google/organic/task_post` then poll `task_get`; normal
    priority is ~$0.6/1K, high priority is ~$1.2/1K.
- **Mode switch**: `search.dataforseo_mode: standard` plus `standard_priority: 2`
  in production. High priority normally returns within one minute; manual dashboard
  runs still wait on the queue (see [[cost-saver-schedule]]).
- Location/language are derived from the city's locale; the Norwegian locale quirk in
  [[pyyaml-no-norway-boolean]] applies to the request boundary.

## Quirks and hard-won rules

- **Never cache failures.** Quota exhaustion raises `SearchQuotaError` (provider is
  skipped for the rest of the run via `FallbackSearchClient._exhausted`); transient
  HTTP/network failures raise `SearchUnavailableError`. Neither is written to
  `search_cache` — caching an empty result for a failed call would permanently mark
  the pair as "searched, found nothing" (see [[searchquotaerror-reraise-ordering]]).
- A persistent transient failure disables the provider for the remainder of that
  geographic pass. The next pass/day constructs a fresh client; this prevents one
  outage or queue timeout from consuming the full collector window pair by pair.
- **Empty ≠ failure.** A *successful* search with zero results IS cached — otherwise
  the same empty query is re-paid on every run ([[cost-optimization-2026-07]]).
- **`stop_after` short-circuit**: `FallbackSearchClient.search_all(stop_after=…)`
  stops issuing paid queries once enough unique URLs are collected for the pair.
- **Normal-priority timeout incident (2026-07-18/19):** DataForSEO documents a
  45-minute target guarantee for the normal queue, longer than this client's
  5-minute poll timeout. Sequential tasks therefore timed out and were re-posted
  the next day. Production now sends `priority: 2`; do not switch it back without
  implementing persistent batched task IDs.
- Costs accrue per query, so the pipeline's done-pair pre-filter
  ([[done-pair-url-hash-not-city-topic]]) is the main spend control: fully covered
  pairs never reach the client.

## Where it runs

The saver collector cron (01:00→16:20 UTC, `search_only` mode) does effectively all
DataForSEO traffic; see [[cost-saver-schedule]]. The extraction side is [[deepseek]].
