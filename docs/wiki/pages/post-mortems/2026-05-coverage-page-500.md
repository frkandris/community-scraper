---
type: Post-mortem
title: Coverage Page 500 (2026-05-30)
description: app_state.cities/topics are dataclasses, not dicts; dict-style access 500s any route touching them.
tags: [incident, coverage, app-state, dataclass]
timestamp: 2026-07-09
resource: scraper/web/app.py
---

# Coverage Page 500 (2026-05-30)

*The route treated dataclass configuration objects as dictionaries and crashed on first render.*

## Summary

The `/admin/coverage` page threw an Internal Server Error immediately after launch.

## Root cause

The coverage route accessed cities and topics as dicts (`city["name"]`, `t["name"]`), but `app_state.cities` and `app_state.topics` are **dataclass objects**, not dicts. The objects have `.name`, `.country`, `.locale`, `.search_variants` attributes.

Incorrect:
```python
countries.setdefault(city["country"], []).append(city["name"])
topic_names = [t["name"] for t in (app_state.topics or [])]
```

Correct:
```python
country = getattr(city, "country", "Other") or "Other"
countries.setdefault(country, []).append(city.name)
topic_names = [t.name for t in (app_state.topics or [])]
```

## Why it happened

The route was written by analogy with dict-returning DB functions (`get_city_topic_counts` returns `dict[str, dict[str, int]]`). It was natural to assume cities/topics had the same shape, but they're dataclass instances loaded from YAML via `load_config()`.

## Lesson

When writing code that touches `app_state.cities` or `app_state.topics`, always use attribute access (`.name`, `.country`, etc.). Use `getattr(obj, attr, default)` defensively for nullable fields.

## Related

- [[web-app]]
