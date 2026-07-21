---
type: Runbook
title: Cost-Saver Twin Schedule
description: Two independent daily crons — DataForSEO collects cheaply all day (search_only, standard mode), DeepSeek extracts only in its off-peak discount window (ai_only, stop_at-boxed).
tags: [operations, cost, schedule, search-only, off-peak, deepseek, dataforseo]
timestamp: 2026-07-21
resource: scraper/main.py
---

# Cost-Saver Twin Schedule

*Time-rich, money-tight operation: search and extraction run **independently**, each in its cheapest window. Enabled via `schedule.saver_enabled: true` (on by default since 2026-07-09).*

## The two jobs

| Job | Mode | Cron (UTC) | Window end | What it does |
|---|---|---|---|---|
| search collector | `search_only` | `0 1 * * *` | `search_until: 16:20` | DataForSEO **high-priority standard** queue (~$1.2/1K) searches + fetches pages into the cache. **Zero LLM calls.** |
| off-peak extractor | `ai_only` | `35 16 * * *` | `extract_until: 00:20` | DeepSeek extracts the **already-collected** pages, entirely inside its off-peak window (UTC 16:30–00:30, ~50–75% cheaper). |

Both use the Sweden → world → Hungary pass order and the shared `_cron_run` runner in `main.py`. The expansion-first order applies to the bounded scheduled jobs; startup recovery keeps its Hungary-first order.

## How the window boxing works

`run_pipeline(..., stop_at=…)` — `_next_window_end(start, "HH:MM")` computes the first HH:MM UTC after the start (midnight-crossing handled: 16:35 → 00:20 = next day). The pair loops check `_window_closed(stop_at)` and stop gracefully; unfinished pairs simply carry over to the next day (nothing is lost — search results and page texts are cached, un-extracted pages stay fingerprint-stale).

The complementary `*_until` values normally keep the jobs apart. `RunCoordinator` is the final overlap guard: if one window overruns, the next job cannot reserve the shared slot and skips instead of running concurrently. See [[shared-run-task-slot]].

## `search_only` mode

`run_pipeline` forces `run_communities/venues/persons = False`, skips the re-AI pass, and exits immediately after the fetch batch. This strict boundary means it never reads extraction caches, calls `save_results`, upserts entities, or runs entity duplicate detection. Tiering ([[cost-optimization-2026-07]]) applies as usual.

`search_cache.collected_at` is written only after every selected URL received a fetch attempt. A killed process leaves it `NULL` so the pair resumes, while a permanently unreadable URL is a logged fetch failure rather than a reason to replay the pair every day. The 2026-07-14 migration marks legacy search rows collected because their historical runs already attempted those batches. See [[2026-07-search-only-cache-replay]].

## Related safety (same commit)

- **Transient search failures are typed** (`SearchUnavailableError`: network error, 5xx, bad JSON, standard-queue timeout) and are **never cached** — previously they returned `[]` and the always-save logic would have permanently marked the pair "searched, empty". Quota still raises `SearchQuotaError`.
- **Full-key pair logs** (`_new_pair_log`) — failure entries carry every key `run_detail.html` sums/iterates, fixing the admin 500 on runs with failed pairs; the route also merges defaults for historical rows.
- **Retry-After parse guard** — an HTTP-date Retry-After no longer escapes the typed-error model as a run-aborting ValueError.

## Ops notes

- `dataforseo_mode: standard` and `standard_priority: 2` are global: **manual dashboard runs also search in the priority queue**. Live mode remains available for truly interactive searches.
- Legacy combined cron (`cron_enabled`) stays available and off.
- Watch progress on the coverage matrix; searched-but-unextracted pairs show amber until the night pass catches up.
- Failed scheduled/startup exceptions are persisted in `runs.error` and printed in the next daily report; the container log is no longer the only error surface.
- Provider failures make the run unsuccessful instead of displaying a green check.
  Graceful cancellation stores its cause; a row left unfinished by a hard restart or
  OOM is labeled as such when the report reads it.
- `ai_only` reads `cache_pages` pair by pair. Loading the complete raw cache before
  the first pair caused zero-log process restarts at ~74K cached pages.
- Triggering and startup behavior are summarized in [[run-modes-and-startup]].
