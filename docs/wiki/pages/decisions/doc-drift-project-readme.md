---
type: Decision
title: PROJECT.md and README.md Have Drifted
description: The root PROJECT.md and README.md describe retired providers (Serper→Brave→SearXNG, Ollama); the code and this wiki are authoritative.
tags: [documentation, drift, providers, caution]
timestamp: 2026-07-09
resource: PROJECT.md
---

# PROJECT.md and README.md Have Drifted

*The root-level `PROJECT.md` (and parts of `README.md`) predate several provider changes and now describe systems that no longer exist. Trust the code and this wiki, not those files.*

Known stale claims in `PROJECT.md`:

- Search chain listed as **Serper → Brave → SearXNG**. Actual: **Google Playwright → DataForSEO → Serper** ([[search-provider-fallback-chain]]). No Brave or SearXNG client exists (the `LOCALE_TO_BRAVE_COUNTRY` table is dead).
- Extraction chain listed as **DeepSeek → Groq → Ollama**. Actual: **DeepSeek → Groq** ([[extraction-provider-fallback-chain]]); Ollama removed.
- `search_ttl_days: 7` — actual is `3650` ([[search-ttl-3650-days]]).
- Scheduler cron described as active — actually a no-op ([[scheduler-disabled-no-cron]]).
- Single-domain assumptions — the app is now two-domain ([[two-domain-single-container]]).

`CHANGELOG.md` is accurate and is the best chronological source. When updating this wiki from the code, prefer the code; when a root doc conflicts, note it here rather than trusting it.
