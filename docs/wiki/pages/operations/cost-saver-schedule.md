---
type: Runbook
title: Twin Schedule — Extract First, Then Collect
description: Two daily jobs — the free-tier extractor runs 00:30-10:00 UTC right after the quota reset, the DataForSEO collector 10:30-23:50.
tags: [operations, cost, schedule, search-only, quota, free-tier, dataforseo]
timestamp: 2026-08-16
resource: scraper/main.py
---

# Twin Schedule — Extract First, Then Collect

*Time-rich, money-tight operation: search and extraction run **independently**, each in its cheapest window. Enabled via `schedule.saver_enabled: true` (on by default since 2026-07-09).*

## The two jobs

| Job | Mode | Cron (UTC) | Window end | What it does |
|---|---|---|---|---|
| extractor | `ai_only` | `30 0 * * *` | `extract_until: 10:00` | Extracts already-collected pages on the free-tier fleet, starting right after the 00:00 UTC quota reset. |
| search collector | `search_only` | `30 10 * * *` | `search_until: 23:50` | DataForSEO **high-priority standard** queue (~$1.2/1K) searches + fetches pages into the cache. **Zero LLM calls**, so it never competes for the AI budget. |

Both scheduled jobs and startup recovery use the Sweden → world → Hungary pass
order. Startup and cron share `_saver_city_groups`, so a deploy cannot silently
restore the old Hungary-first behavior.

## How the window boxing works

`run_pipeline(..., stop_at=…)` — `_next_window_end(start, "HH:MM")` computes the first HH:MM UTC after the start (midnight-crossing still handled, though neither current window crosses). The pair loops check `_window_closed(stop_at)` and stop gracefully; unfinished pairs simply carry over to the next day (nothing is lost — search results and page texts are cached, un-extracted pages stay fingerprint-stale).

The complementary `*_until` values normally keep the jobs apart. `RunCoordinator` is the final overlap guard: if one window overruns, the next job cannot reserve the shared slot and skips instead of running concurrently. See [[shared-run-task-slot]].

## `search_only` mode

`run_pipeline` forces `run_communities/venues/persons = False`, skips the re-AI pass, and exits immediately after the fetch batch. This strict boundary means it never reads extraction caches, calls `save_results`, upserts entities, or runs entity duplicate detection. Tiering ([[cost-optimization-2026-07]]) applies as usual.

`search_cache.collected_at` is written after the selected URL batch has been
attempted and at least one page is available (or the SERP was legitimately empty).
A killed process or an all-URL fetch outage leaves it `NULL`, so the cached URLs
are fetched again without paying for another search. Individual failures in a
partially successful batch remain logged without replaying the whole pair. The
2026-07-14 migration marks legacy search rows collected because their historical
runs already attempted those batches. See [[2026-07-search-only-cache-replay]].

## Related safety (same commit)

- **Transient search failures are typed** (`SearchUnavailableError`: network error, 5xx, bad JSON, standard-queue timeout) and are **never cached** — previously they returned `[]` and the always-save logic would have permanently marked the pair "searched, empty". Quota still raises `SearchQuotaError`.
- **Full-key pair logs** (`_new_pair_log`) — failure entries carry every key `run_detail.html` sums/iterates, fixing the admin 500 on runs with failed pairs; the route also merges defaults for historical rows.
- **Retry-After parse guard** — an HTTP-date Retry-After no longer escapes the typed-error model as a run-aborting ValueError.

## Ops notes

- `dataforseo_mode: standard` and `standard_priority: 2` are global: **manual dashboard runs also search in the priority queue**. Live mode remains available for truly interactive searches.
- Legacy combined cron (`cron_enabled`) stays available and off.
- Watch progress on the coverage matrix; searched-but-unextracted pairs show amber until the night pass catches up.
- Failed scheduled/startup exceptions are persisted in `runs.error` and printed in the next daily report; the container log is no longer the only error surface.
- Provider failures make the run unsuccessful instead of displaying a green check;
  their counts come from the pair log and are not duplicated as a top-level error.
  Graceful cancellation stores its cause; an unfinished row is conservatively
  labeled as still running, restarted, or OOM-killed.
- `ai_only` reads `cache_pages` pair by pair. Loading the complete raw cache before
  the first pair caused zero-log process restarts at ~74K cached pages.
- Triggering and startup behavior are summarized in [[run-modes-and-startup]].

## Why extract runs first (2026-08-16)

The original order — collect 01:00→16:20, extract 16:35→00:20 — existed for
DeepSeek's off-peak discount window. Extraction now runs on the free-tier fleet
([[free-tier-model-router]]), and free allowances reset at **00:00 UTC**.

Under the old order the extractor began spending 16 hours after the budget
refilled, and whatever it could not reach by 00:20 was lost — allowances do not
roll over. It also meant every call piled into the last third of the day, which
is how a single Groq model returned a 1197-second rate limit during the first
live measurement.

The extract window is 9.5 h because the fleet is ~16.5K calls/day and the chain
is serial at roughly 2 s/call: about 9.2 h to spend it all. The collector takes
the remaining 13.3 h; its constraint is money per DataForSEO query, not
wall-clock, so it is the one that can afford to be squeezed.
