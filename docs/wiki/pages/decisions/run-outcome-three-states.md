---
type: Decision
title: Run Outcome Has Three States
description: A run is ok / warning / aborted rather than a success boolean, because one retryable pair failure out of 1414 is not the same event as a provider outage that ended the window.
tags: [runs, reporting, error-handling, daily-report, schema]
timestamp: 2026-07-31
resource: scraper/pipeline.py
---

# Run Outcome Has Three States

*`runs.success` could not tell "1413 of 1414 pairs done, one timeout queued for tomorrow" apart from "the provider died and the window is gone" — both showed ❌.*

## The problem

Until 2026-07-31 every run writer computed
`success = not (search_failures or extract_failures)`, duplicated in three
places (`main.py` scheduled + startup, `web/app.py` manual). Any single item
failure made the whole run a failure. The 2026-07-30 daily report:

> ❌ search_only · 01:00 UTC · 1414 pairs · 0 records — errors: 1 search …
> (DataForSEO standard task timed out)

That run was healthy. The pair was not cached and the next run picks it up
([[non-quota-errors-drop-page]]) — exactly the designed behaviour, reported as
a failure. Meanwhile a genuinely dead provider produced the same ❌, so the
mark carried no information.

## Decision

`pipeline.classify_run_outcome(pair_logs, run_error)` is the single classifier:

| Outcome | Meaning | Trigger |
|---|---|---|
| `ok` | everything attempted succeeded | no failure counters |
| `warning` | finished; some items failed and are queued | any `search_failed` / `extract_failed` |
| `aborted` | stopped early | `run_error`, or a pair log with `aborted: True` |

An abort is **explicitly marked** at the two places that stop a run early — the
search-provider-down marker entry and the `providers_down` extract branch — not
inferred from `search_error` being present, because an ordinary per-pair failure
records `search_error` too (`pipeline.py:544`). That ambiguity is what made a
boolean insufficient in the first place.

## Schema and compatibility

- `runs.outcome TEXT`, added with the usual `ALTER TABLE` guard.
- `runs.success` stays and now means **"the run completed"**:
  `success = outcome != 'aborted'`, so a warning run is successful. Every
  existing reader (`get_last_run`, `get_last_run_mode`) keeps working and stops
  treating a one-timeout run as a failed one.
- `db._OUTCOME_SQL` reads `COALESCE(outcome, CASE success …)`, so rows written
  before the column map onto `ok` / `aborted`.
- `finish_run(outcome=None)` derives the value from `success` — callers that
  never learned about outcomes (tests, future code) stay correct.

## Consequences

- **Startup recovery changed on purpose**: `_startup_run` re-runs the previous
  mode only when the last row is unfinished or `aborted`. A warning run is no
  longer re-run on deploy; its failed pairs were never cached, so the next
  scheduled run covers them anyway ([[run-modes-and-startup]]).
- Dashboard run list and `/admin/runs/{id}` render amber for `warning`
  ("completed with retries"); the daily email uses ✅ / ⚠️ / ❌
  ([[daily-report]]).
- Rejected alternative: returning the outcome from `run_pipeline()` as a third
  tuple element. The abort already has to be visible in the persisted pair logs
  for the report to explain itself, so marking it there keeps one source of
  truth and leaves the function signature (and every caller) alone.
