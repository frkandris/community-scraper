---
type: Decision
title: Country Priority in Bounded Saver Runs
description: Country order in the bounded saver windows lives in config, not code, so whichever market has the largest unprocessed backlog can lead.
tags: [decision, pipeline, priority, sweden]
timestamp: 2026-08-16
resource: scraper/main.py
---

# Country Priority in Bounded Saver Runs

*Added in May 2026 when 290 Swedish municipalities were added to the config.*

> **Update 2026-08-16 — the order is now configuration.** `_saver_city_groups`
> takes a list and returns one group per named country plus a trailing
> "everything else" group; the list comes from `pipeline.country_priority` in
> `settings.yaml`, defaulting to **Hungary → Germany → Indonesia → Sweden →
> rest**. Hungary moved to the front because the 1000+ inhabitant import
> ([[importing-city-lists]]) left it with 973 unprocessed settlements on the
> primary market — it had been last precisely *because* it was finished.
> Indonesia (83 kota) joined as the next expansion market. Re-ordering markets
> no longer needs a code change.
>
> **Update 2026-07-27 — superseded by Germany-first.** Sweden is now fully indexed
> (4200/4200 pairs). `_saver_city_groups` leads with **Germany** (the ~2,057 Städte
> added 2026-07-26), then Sweden, then rest of world, then Hungary — startup recovery
> uses the same order. Sweden/Hungary are fast-skipped by the done-pair pre-filter.
> The four-group ordering below replaces the three-group split. See `main.py` and the
> 2026-07-26 log entry.

## Priority order

1. Sweden (active expansion market, 290 municipalities)
2. Everything except Hungary and Sweden
3. Hungary (mature inventory; still receives genuinely unfinished tail work)

## Implementation

`main.py:_saver_city_groups` splits `app_state.cities` into three lists for `_cron_run`. Startup recovery deliberately retains the older Hungary-first order.

```python
hu_cities = [c for c in cities if c.country == "Hungary"]
se_cities = [c for c in cities if c.country == "Sweden"]
intl_cities = [c for c in cities if c.country not in {"Hungary", "Sweden"}]
```

## Why not a single call

A single call processes cities in the order they appear in `cities.yaml`. Splitting gives explicit control over priority without reordering the YAML, and makes the coverage page's country-tab display reflect actual pipeline order.

## Why Sweden first

The saver jobs have hard stop windows. Hungary's mature backlog previously consumed most of the collector window, so Swedish coverage progressed only partially. Expansion-first ordering guarantees Sweden receives both collection and off-peak extraction capacity before tail work.

## Related

- [[pipeline-run-modes]]
- [[adding-city-topic]]
