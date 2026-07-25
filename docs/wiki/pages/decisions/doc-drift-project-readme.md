---
type: Decision
title: PROJECT.md Has Drifted
description: PROJECT.md was archived behind an out-of-date banner on 2026-07-25; README.md is now a project introduction, CLAUDE.md the agent brief (AGENTS.md is generated from it), and this wiki the technical source of truth.
tags: [documentation, drift, providers, caution]
timestamp: 2026-07-25
resource: PROJECT.md
---

# PROJECT.md Has Drifted

*The long root-level design document predates multiple provider and scheduling changes; it now carries an explicit "do not trust" banner, and the current documentation set is README + CLAUDE.md + this wiki.*

Known stale claims in `PROJECT.md`:

- **Serper → Brave → SearXNG** search; current code uses DataForSEO only ([[search-provider-fallback-chain]]).
- **DeepSeek → Groq → Ollama** extraction; current code uses DeepSeek only ([[extraction-provider-fallback-chain]]).
- `search_ttl_days: 7`; current config is `3650` ([[search-ttl-3650-days]]).
- one active combined cron; current defaults enable twin cost-saver jobs and daily report while leaving the legacy combined cron off ([[scheduler-disabled-no-cron]]).
- single-domain assumptions; one container serves two domains ([[two-domain-single-container]]).

## 2026-07-25 documentation split

Because `PROJECT.md` is addressed to AI assistants, a stale copy is an active hazard —
an agent reading it assumes providers that no longer exist. It now opens with an
ARCHIVED banner pointing at the live sources, and is safe to delete outright.

Current roles:

| File | Role |
|---|---|
| `README.md` | What the project is, who makes it, why — links into this wiki for depth |
| `CLAUDE.md` | Working brief for coding agents: commands, architecture, easy-to-break patterns |
| `AGENTS.md` | **Generated** from CLAUDE.md by `scripts/sync_agents_md.py`; `tests/test_agents_md.py` fails on drift |
| `docs/wiki/` | Maintained technical knowledge base |
| `CHANGELOG.md` | Chronological record |

`AGENTS.md` earned its generator the same way: it still documented the deleted
`revalidate` mode months after removal ([[admin-simplification-2026-07]]). Two
hand-maintained copies of the same brief will always drift; one plus a test will not.
