# Glossary

Domain vocabulary. One line per term; details live in the linked pages.

- **pair (város–téma páros)** — the unit of pipeline work: one (city, topic) combination; ~774 cities × topics, minus tiering.
- **site** — which domain served the request: `kozossegek` (HU-only cities) or `meetapedia` (all cities), detected from the Host header.
- **fingerprint** — SHA-256[:12] of a prompt family + model name; keys the extraction cache, so editing a prompt stales exactly its own results.
- **done pair** — a pair with a `search_cache` entry and all its `cache_pages` extracted at the current fingerprint; skipped before the loop, invisible in logs.
- **run mode** — `full` (search+fetch+extract, "Smart" in the UI), `ai_only` ("re-ai": extraction over cached texts, no web), `search_only` ("collect": search+fetch, zero LLM). The former `revalidate` mode was removed 2026-07-23.
- **saver schedule** — the twin crons: collector (`search_only`, 01:00→16:20 UTC) and extractor (`ai_only`, 16:35→00:20 UTC, inside DeepSeek's off-peak discount).
- **window boxing / stop_at** — a run receives a deadline and exits gracefully at the window edge; unfinished pairs carry over to the next day.
- **standard mode** — DataForSEO's queued task API; production uses high priority (~40% cheaper than live, normally ≤1 minute) because normal priority can exceed the client timeout.
- **off-peak** — DeepSeek's discount window, UTC 16:30–00:30 (~50–75% cheaper).
- **record_key** — DB uniqueness key: normalized `name|city|topic` (Unicode-safe, NFKC+casefold hashing for non-Latin names).
- **community_id** — stable public identity: SHA-256[:12] of `name.lower()|city.lower()`; survives re-scrapes, used in URLs and history.
- **joinable** — the LLM-emitted quality gate: recurring + open to the public + has group identity; only `joinable=True` records survive.
- **hidden** — moderation flag on a community (merged duplicate or approved "not a community" report); survives re-scrapes.
- **false positive** — an admin-curated negative example injected into extraction prompts so the same non-community stops being extracted.
- **thin page** — a community page without a description; noindexed and excluded from sitemaps.
- **canonical domain** — kozossegek.com for HU-city content even when served on meetapedia.com; everything else self-canonicalizes.
- **topic tier / core topics** — cities marked `topic_tier: core` (260 small Swedish kommuner) only run the 12 `pipeline.core_topics`; other pairs are frozen.
- **stock (állomány)** — current absolute totals (communities, venues, persons, pages, covered pairs) as opposed to daily diffs; both appear in the daily email.
- **collector / extractor** — nicknames for the two saver crons (see saver schedule).
- **pageview counter** — the bot-filtered server-side traffic middleware; fallback + footnote under GA4 numbers in the daily report.
