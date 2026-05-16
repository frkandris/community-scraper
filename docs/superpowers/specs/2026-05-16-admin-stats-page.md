# Admin Stats Page

**Date:** 2026-05-16  
**Status:** Approved

## Goal

Add a `/admin/stats` page that shows key data quality metrics for the communities database. Motivated by the PM's audit recommendation: before any growth effort, understand what fraction of records have usable contact information.

## URL & Navigation

- **Route:** `GET /admin/stats`
- **Nav:** standalone link labelled "Stats" inserted before "Subscribers" in both desktop and mobile nav in `base.html`
- **Active state:** `_p == '/admin/stats'`

## Approach

Server-rendered (approach A). Aggregated SQL on page load. No JS, no async. 9 000 rows of aggregate queries run in < 100 ms — no caching or async layer needed.

## Template: `stats.html`

Extends `base.html`. Four visual sections:

### 1. Sarokszámok (4 stat cards)
| Card | Value |
|---|---|
| Összes közösség | `total` |
| Látható | `visible` |
| Érintett városok | `cities` |
| Topicok | `topics` |

### 2. Adatminőség (4 stat cards, base = visible)
| Card | Value | Description |
|---|---|---|
| Van website | `has_website` / `visible` % | `json_extract(data,'$.website')` not null/empty |
| Van elérhetőség | `has_contact` / `visible` % | `json_extract(data,'$.contact')` not null/empty |
| Van leírás | `has_description` / `visible` % | description length > 50 chars |
| Bármilyen elérhetőség | `has_any` / `visible` % | website OR contact |

### 3. Top 20 város (table)
Columns: Város | Közösségek | Website % | Elérhetőség %  
Sorted by community count descending. Only visible communities.

### 4. Topic megoszlás (table)
Columns: Topic | Közösségek  
Reuses existing `get_topic_counts(db_path)`. Sorted by count descending.

## New DB Function: `get_data_quality_stats(db_path)`

Location: `scraper/db.py`

Returns a single dict:

```python
{
    "total": int,        # all communities incl. hidden
    "visible": int,
    "hidden": int,
    "cities": int,       # distinct cities (visible only)
    "topics": int,       # distinct topics (visible only)
    "has_website": int,
    "has_contact": int,
    "has_description": int,
    "has_any": int,      # website OR contact
    "city_rows": [
        {"city": str, "cnt": int, "w": int, "c": int},
        ...  # top 20, sorted by cnt desc
    ],
    "topic_counts": dict[str, int],  # from get_topic_counts()
}
```

**SQL for summary + quality** (single query):
```sql
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN hidden=0 THEN 1 ELSE 0 END) as visible,
  SUM(CASE WHEN hidden=1 THEN 1 ELSE 0 END) as hidden,
  COUNT(DISTINCT CASE WHEN hidden=0 THEN city END) as cities,
  COUNT(DISTINCT CASE WHEN hidden=0 THEN topic END) as topics,
  SUM(CASE WHEN hidden=0
       AND json_extract(data,'$.website') IS NOT NULL
       AND json_extract(data,'$.website') != '' THEN 1 ELSE 0 END) as has_website,
  SUM(CASE WHEN hidden=0
       AND json_extract(data,'$.contact') IS NOT NULL
       AND json_extract(data,'$.contact') != '' THEN 1 ELSE 0 END) as has_contact,
  SUM(CASE WHEN hidden=0
       AND length(COALESCE(json_extract(data,'$.description'),'')) > 50
       THEN 1 ELSE 0 END) as has_description,
  SUM(CASE WHEN hidden=0 AND (
       (json_extract(data,'$.website') IS NOT NULL AND json_extract(data,'$.website') != '')
       OR
       (json_extract(data,'$.contact') IS NOT NULL AND json_extract(data,'$.contact') != '')
     ) THEN 1 ELSE 0 END) as has_any
FROM communities
```

**SQL for city breakdown:**
```sql
SELECT city, COUNT(*) as cnt,
  SUM(CASE WHEN json_extract(data,'$.website') IS NOT NULL
           AND json_extract(data,'$.website') != '' THEN 1 ELSE 0 END) as w,
  SUM(CASE WHEN json_extract(data,'$.contact') IS NOT NULL
           AND json_extract(data,'$.contact') != '' THEN 1 ELSE 0 END) as c
FROM communities
WHERE hidden=0
GROUP BY city
ORDER BY cnt DESC
LIMIT 20
```

## Route: `@admin.get("/stats")`

Location: `scraper/web/app.py`, near the other simple read-only admin routes (e.g. after `/subscriptions`).

```python
@admin.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request):
    from ..db import get_data_quality_stats
    stats = {}
    if app_state.db_path and app_state.db_path.exists():
        stats = get_data_quality_stats(app_state.db_path)
    return templates.TemplateResponse(request, "stats.html", {"stats": stats})
```

## Error handling

`get_data_quality_stats` returns a zeroed-out dict if DB doesn't exist. Template uses `stats.get("visible", 0)` style access so the page renders safely on empty/missing DB.

## What's out of scope

- Per-city drill-down pages
- Time-series / historical trends
- Filtering by topic or country
- Caching or background refresh
