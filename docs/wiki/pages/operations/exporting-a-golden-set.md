---
type: Runbook
title: Exporting a Golden Set from Production
description: The scoring golden set lives in an 8.7 GB production DB behind a browser-only terminal; scripts/make_golden_db.py slims it to a few MB in one short command.
tags: [operations, scoring, database, coolify, providers]
timestamp: 2026-09-05
resource: scripts/make_golden_db.py
---

# Exporting a Golden Set from Production

*Model scoring needs pages whose answer we already hold, and those live only on the server — so the slimming runs there and only the result travels.*

## Why this is not a `docker cp`

Three constraints stack up, and each one rules out the obvious answer:

- `data/scraper.db` is **8.7 GB** (measured 2026-09-05). Copying it whole is
  not worth it for the twelve pages [[free-tier-model-router]] scoring actually
  reads, and the server had 23 GB free at the time.
- Coolify's terminal drops you **inside the container**, not on the host, so
  `docker ps` and `docker cp` are not available — `sh: docker: not found` is the
  expected answer there, not a broken setup.
- That terminal **inserts newlines into pasted input**. A heredoc breaks at the
  first wrap and leaves the shell on a `>` continuation prompt; a single-line
  base64 blob breaks the same way. Anything much over ~60 characters is
  unreliable, which is why the logic lives in a file in the image instead of in
  the command.

The Dockerfile already copies `scripts/` for exactly this reason: *"Maintenance
scripts must be runnable in the container: that is where the database and the
API keys live."*

## Steps

In the Coolify terminal for the app:

```
python3 /app/scripts/make_golden_db.py
```
```
cp /tmp/golden.db.gz /app/scraper/web/static/g6.gz
```

Then from a workstation, `curl` it off `https://meetapedia.com/static/g6.gz`,
gunzip it to `data/scraper.db`, and **delete it from the server immediately**:

```
rm /app/scraper/web/static/g6.gz
```

`/static` is public for as long as the file sits there. Use an unguessable
name, keep the window to minutes, and know what is in it: cached text of public
pages plus community names the site already publishes. There is no admin-gated
static mount to use instead — `_fastapi.mount("/static", ...)` is the only one.

## What travels, and why exactly this

`scraper/scoring.py` reads two tables and nothing else, so the export copies two
tables and nothing else — no run history, no `provider_usage`, no search cache.

| Table | Slice | Why |
|---|---|---|
| `cache_pages` | first 400 rows by `url_hash`, `extracted_at IS NOT NULL` | `golden_set()` orders by `url_hash`; copying the **prefix** reproduces the identical sample the full DB would yield |
| `communities` | all visible rows, `record_key`/`city`/`$.name` only | genericness is a property of the whole corpus, and a dozen pages cannot show it |

The determinism is the point. A sample that moves between runs makes scores
incomparable and does it silently, because the numbers still look like numbers —
the trap that made `mistral-small` appear to fall 80 → 55 in an hour on
2026-08-16. 400 rows for a golden set of 12 because `golden_set()` reads
`limit * 8` and then discards pages with no cached text or no extracted records.

## Notes

- Scores measured on the export are comparable with the `quality:` values in
  `config/providers.yaml` **only** because the code path is identical — the same
  `golden_set()`, the same `corpus_names()`, the same generic-token derivation.
  Do not reimplement any of it to save a step.
- The script opens the source `mode=ro` and runs in its own process, so it does
  not touch the app's event loop. It is safe on a running container.
- What it measures is agreement with the *incumbent* extraction, not ground
  truth: the expected names come from whichever model processed each page
  before. See [[free-tier-model-router]].
