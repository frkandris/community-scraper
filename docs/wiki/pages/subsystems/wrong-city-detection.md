---
type: Subsystem
title: Wrong-City Detection
description: scan() flags communities whose text fields mention another known city — a strong signal the record landed under the wrong city; admin review at /admin/wrong-city with a one-click move that merges on identity conflict.
tags: [wrong-city, data-quality, moderation, detection]
timestamp: 2026-07-24
resource: scraper/wrong_city.py
---

# Wrong-City Detection

*A community filed under city A whose description mentions city B probably belongs in B (motivating case: a "Szentendre" seniors club whose description was all about Szentgotthárd). `wrong_city.py:scan()` writes `wrong_city_candidates` rows for review at `/admin/wrong-city` — grouped with [[duplicate-detection]] under the admin nav's "Data quality" dropdown (added 2026-07-24; the two remain separate pages).*

## Detection

`detect_wrong_city_candidates(db_path, cities)` scans every visible community's
text fields (`SCANNED_FIELDS`: name, description, location, meeting_schedule,
contact, history, join_process) with **one** compiled alternation over all known
city names (from `app_state.cities`, i.e. `cities.yaml`):

```
(?<!\w)(CityA|CityB|…)\w{0,4}(?!\w)   # IGNORECASE
```

- **Longest name first** in the alternation, so a known `Vácrátót` is reported as
  Vácrátót, never as a `Vác` mention.
- **`\w{0,4}` inflection tail** covers Hungarian suffix/adjective forms
  ("Szentgotthárdon", "szentgotthárdi", "Szegedről") without a suffix table. A
  tail longer than 4 word chars does not match, so `Vác` alone never fires
  inside "Vácrátót…" even when Vácrátót isn't a known city.
- **Own-city guard**: `_is_own_city` skips the community's own city and any
  prefix relative of it in either direction.
- One candidate per (record, mentioned city); the snippet stores ±60 chars of
  context plus the exact `matched_text` for `<mark>` highlighting.

## Review queue semantics

- UNIQUE index on `(record_key, mentioned_city)` has **no** `WHERE resolution IS
  NULL` clause (unlike `duplicate_candidates`) — a dismissed pair stays dismissed
  forever; re-scans never nag about the same mention again.
- `cleanup_stale_wrong_city_candidates` (run at the start of every `scan()`)
  auto-dismisses pending candidates whose record vanished or whose text no
  longer mentions the flagged city.
- Scan is admin-triggered only (`POST /admin/wrong-city/scan`); it is not wired
  into the pipeline.

## Actions

- **Move to X** → `apply_community_edit(record_key, "wrong_city", mentioned_city)`
  — the exact code path an approved user edit request takes, including the
  merge-on-conflict behaviour from [[2026-07-wrong-city-approve-conflict]]: if
  the community already exists under the correct city, the row is merged into it
  (source_urls unioned, source hidden) instead of failing.
- **Correct city** → resolution `dismissed`.

## Traps

- The regex approach trades precision for zero LLM cost: a description that
  legitimately mentions another city ("kirándulás Szentendrére") will be
  flagged once. That is by design — dismissing is one click and permanent.
- City names shorter than 3 chars are excluded from the alternation; nothing in
  the current `cities.yaml` hits this.
