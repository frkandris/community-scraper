---
type: Post-mortem
title: A Database Query in /healthz Took the Site Down (2026-08-16)
description: /healthz queried the database, so a write lock failed the healthcheck and Traefik pulled the container from rotation — four apparent outages with a healthy process.
tags: [incident, deployment, coolify, traefik, monitoring, routing]
timestamp: 2026-08-16
resource: scripts/smoke_test.py
---

# A Database Query in /healthz Took the Site Down (2026-08-16)

*The container was healthy, the app was idle, the logs were clean — and nobody outside could reach the site.*

## Summary

Three separate times during the free-tier router rollout, `kozossegek.com` and
`meetapedia.com` returned 404 to every request while Coolify reported
`running:healthy` and no deployment was in flight. Twice the operator noticed
before any monitoring did.

## Root cause

**`/healthz` queried the database.** It ran `SELECT COUNT(*)` over
`communities` on every call — and the Docker healthcheck calls it every 30s with
a 10s timeout. While the pipeline held a write lock, that query blocked, the
check timed out, three failures marked the container `unhealthy`, and Traefik
pulled it out of rotation. Every public request then 404'd until the lock
cleared and the check went green again.

That is the whole mechanism, and it explains each observation that looked
contradictory:

- Coolify reported `running:healthy` **between** episodes, because the container
  genuinely recovered each time;
- the application log was clean, because the process was never at fault;
- it started today, because the 973-settlement import pushed write volume up far
  enough for lock waits to exceed 10 seconds;
- it happened with the app apparently idle, because the writer was the
  concurrent enrichment/pipeline work, not the request being served.

The first episode did follow `POST /applications/{uuid}/restart`, which sent me
looking for a routing bug; the later ones followed ordinary pushes and one
followed nothing at all. Restart may or may not drop the route independently —
force-deploy fixed it every time, so that suspicion is unresolved and no longer
load-bearing.

## Fix

`/healthz` is now a **liveness** probe: the record count is cached for 60s and
computed off the event loop with a 3s ceiling, and a slow or locked database
reports `db: "busy"` while the endpoint still returns 200. The app being up is
the only question a liveness probe should answer; whether the database is fast
is a different one, and answering it here took the site down.

Two contributing loads were removed the same evening: the sitemap's N+1 query
(one `get_communities` per city×topic pair, >30s at 3.8K cities) and `init_db()`
running per request — its body is CREATE TABLE / ALTER TABLE, i.e. a write lock,
on a dozen routes.

To restore routing when it is already lost:
`POST /api/v1/deploy?uuid=…&force=true`. Prefer it over `restart`.

## Why nothing caught it

Every check we had looked from the inside:

- the Docker healthcheck calls localhost;
- Coolify's status reports the container, not its reachability;
- the application logs are written by a process that is running perfectly.

An outage where the process is healthy is invisible to all three.

`scripts/smoke_test.py` now checks the public hostnames over the CDN, exactly as
a browser does, and refuses to pass on anything a visitor could not load. Run it
after every deploy:

```bash
PYTHONPATH=. .venv/bin/python scripts/smoke_test.py --wait 420
```

It also flags slow responses. That is not decoration — on its first run it
surfaced a 30-second sitemap that had been quietly
degrading every page load.

## Lesson

**Health means reachable by a user, not alive as a process.** Every signal we
had answered the second question, and the difference between them was a full
outage. Any monitoring worth the name has to make at least one request from
outside the trust boundary it is monitoring.

Operational detail in [[production-monitoring]].
