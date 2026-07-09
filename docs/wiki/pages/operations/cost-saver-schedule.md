---
type: Runbook
title: Cost-Saver Twin Schedule
description: Two independent daily crons — DataForSEO collects cheaply all day (search_only, standard mode), DeepSeek extracts only in its off-peak discount window (ai_only, stop_at-boxed).
tags: [operations, cost, schedule, search-only, off-peak, deepseek, dataforseo]
timestamp: 2026-07-09
resource: scraper/main.py
---

# Cost-Saver Twin Schedule

*Time-rich, money-tight operation: search and extraction run **independently**, each in its cheapest window. Enabled via `schedule.saver_enabled: true` (on by default since 2026-07-09).*

## The two jobs

| Job | Mode | Cron (UTC) | Window end | What it does |
|---|---|---|---|---|
| search collector | `search_only` | `0 1 * * *` | `search_until: 16:20` | DataForSEO (**standard** queue, $0.6/1K) searches + fetches pages into the cache. **Zero LLM calls.** |
| off-peak extractor | `ai_only` | `35 16 * * *` | `extract_until: 00:20` | DeepSeek extracts the **already-collected** pages, entirely inside its off-peak window (UTC 16:30–00:30, ~50–75% cheaper). |

Both use the Hungary → Sweden → world pass order and the shared `_cron_run` runner in `main.py`.

## How the window boxing works

`run_pipeline(..., stop_at=…)` — `_next_window_end(start, "HH:MM")` computes the first HH:MM UTC after the start (midnight-crossing handled: 16:35 → 00:20 = next day). The pair loops check `_window_closed(stop_at)` and stop gracefully; unfinished pairs simply carry over to the next day (nothing is lost — search results and page texts are cached, un-extracted pages stay fingerprint-stale).

The complementary `*_until` values keep the two jobs from overlapping — only one run may be active (`app_state.is_running`), so an overrunning collector would otherwise make the extract cron skip a day.

## `search_only` mode

New `run_pipeline` mode: forces `run_communities/venues/persons = False`, skips the re-AI pass, does search + fetch + `save_scraped` only. Enrichment is inside the extraction branch, so it is skipped too. Tiering ([[cost-optimization-2026-07]]) applies as usual.

## Related safety (same commit)

- **Transient search failures are typed** (`SearchUnavailableError`: network error, 5xx, bad JSON, standard-queue timeout) and are **never cached** — previously they returned `[]` and the always-save logic would have permanently marked the pair "searched, empty". Quota still raises `SearchQuotaError`.
- **Full-key pair logs** (`_new_pair_log`) — failure entries carry every key `run_detail.html` sums/iterates, fixing the admin 500 on runs with failed pairs; the route also merges defaults for historical rows.
- **Retry-After parse guard** — an HTTP-date Retry-After no longer escapes the typed-error model as a run-aborting ValueError.

## Ops notes

- `dataforseo_mode: standard` is global: **manual dashboard runs also search in queue mode** (minutes/query). Flip back to `live` in settings if an interactive run needs instant search.
- Legacy combined cron (`cron_enabled`) stays available and off.
- Watch progress on the coverage matrix; searched-but-unextracted pairs show amber until the night pass catches up.
