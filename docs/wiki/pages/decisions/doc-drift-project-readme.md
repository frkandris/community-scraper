---
type: Decision
title: PROJECT.md Has Drifted
description: Root PROJECT.md describes retired providers and scheduling; README.md, code, and this wiki reflect the current system.
tags: [documentation, drift, providers, caution]
timestamp: 2026-07-10
resource: PROJECT.md
---

# PROJECT.md Has Drifted

*The long root-level design document predates multiple provider and scheduling changes; use the current code, README, and linted wiki for operational decisions.*

Known stale claims in `PROJECT.md`:

- **Serper → Brave → SearXNG** search; current code uses DataForSEO only ([[search-provider-fallback-chain]]).
- **DeepSeek → Groq → Ollama** extraction; current code uses DeepSeek only ([[extraction-provider-fallback-chain]]).
- `search_ttl_days: 7`; current config is `3650` ([[search-ttl-3650-days]]).
- one active combined cron; current defaults enable twin cost-saver jobs and daily report while leaving the legacy combined cron off ([[scheduler-disabled-no-cron]]).
- single-domain assumptions; one container serves two domains ([[two-domain-single-container]]).

`README.md` has already been reduced to the current DataForSEO/DeepSeek architecture. `CHANGELOG.md` remains the chronological source; this wiki stores the current invariants and historical lessons.
