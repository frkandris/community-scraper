---
type: Architecture
title: Two-Domain Single-Container Architecture
description: One FastAPI container serves közösségek.com and meetapedia.com via Host-header detection.
tags: [multi-domain, fastapi, hosting]
timestamp: 2026-07-09
resource: scraper/web/app.py
---

# Two-Domain Single-Container Architecture

*One FastAPI app serves both közösségek.com (Hungarian-first) and meetapedia.com (international) from the same Coolify container.*

## How it works

`_detect_site(request)` reads the `Host` header and returns `"kozossegek"` or `"meetapedia"`. This runs on every request and feeds:

- `lang_context(request)` — injects `site`, `site_name`, `site_url`, `lang`, `locale`, `map_url`, `about_url`, `explore_url`, `submit_url`, `map_center` into every public template
- `_site_cities(request)` — kozossegek shows Hungary-only cities; meetapedia shows all

## Why one container

Cost and operational simplicity. Both domains share the same SQLite database, the same pipeline run, and the same config. Routing at the application layer (not nginx/proxy) keeps deployment trivial.

## Implications

- Nav active-state checks need BOTH HU and EN path prefixes: `_p.startswith('/terkep') or _p.startswith('/map')`  
  See [[web-app]].
- Any new route that exists on both domains needs both URL shapes
- The admin UI has no domain gating — it's always on the internal host

## Related

- [[i18n-and-site-detection]]
- [[web-app]]
