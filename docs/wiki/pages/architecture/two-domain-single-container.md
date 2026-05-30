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
  See [[nav-active-state-dual-prefix]]
- Any new route that exists on both domains needs both URL shapes
- The admin UI has no domain gating — it's always on the internal host

## Related

- [[lang-context-injection]]
- [[site-cities-filter]]
