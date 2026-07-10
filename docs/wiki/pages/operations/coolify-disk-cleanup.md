---
type: Runbook
title: Coolify Disk Cleanup
description: High-disk-usage alerts after deploy-heavy days are old Docker images and build cache; prune them from the server terminal — volumes and running containers are untouched.
tags: [operations, coolify, docker, disk, runbook]
timestamp: 2026-07-10
resource: scraper/main.py
---

# Coolify Disk Cleanup

*Every deploy leaves an image (~6GB) and build cache behind. Coolify's nightly
cleanup handles normal cadence; a deploy-heavy day needs one manual prune.*

## Diagnose

Coolify → Servers → localhost → **Terminal**:

```
df -h / && docker system df
```

Reference incident (2026-07-09, after ~8 deploys in one day): disk at **93%**
(67G/75G); `docker system df` showed 30 images with **48.21GB reclaimable (79%)**
and **11.7GB build cache** — while all live data (SQLite volume) was 105MB.

## Fix

Same terminal:

```
docker image prune -af
docker builder prune -af
df -h /
```

Result of the reference run: 93% → **20%** (15G used, 58G free). Safe because:

- running containers' images are "in use" and never pruned;
- named volumes (`/app/data` SQLite, `/app/config`) are NOT touched by image or
  builder prune;
- pruned images are rebuildable artifacts — the only cost is a slower next build.

Alternatively use the **Trigger Manual Cleanup** button under Server → Configuration →
Docker Cleanup (same effect; scheduled nightly at 00:00 UTC with "Force" enabled).

## Do NOT

- Do not enable **Delete Unused Volumes** in the Docker Cleanup advanced options —
  a stopped app's data volume counts as "unused" and would be destroyed.
- Do not run `docker system prune --volumes` for the same reason.

Related deployment behavior lives in [[deployment-coolify]].
