# Glossary

Domain vocabulary. One line per term; details live in the linked pages.

- **pair (város–téma páros)** — the unit of pipeline work: one (city, topic) combination. 139,536 of them as of 2026-08-23, of which 54,326 have been searched; tiering freezes most pairs for small settlements.
- **site** — which domain served the request: `kozossegek` (HU-only cities) or `meetapedia` (all cities), detected from the Host header.
- **fingerprint** — SHA-256[:12] of a prompt family + model name; keys the extraction cache, so editing a prompt stales exactly its own results.
- **done pair** — a pair with a `search_cache` entry and all its `cache_pages` extracted at the current fingerprint; skipped before the loop, invisible in logs.
- **run mode** — `full` (search+fetch+extract, "Smart" in the UI), `ai_only` ("re-ai": extraction over cached texts, no web), `search_only` ("collect": search+fetch, zero LLM). The former `revalidate` mode was removed 2026-07-23.
- **continuous worker** — the single loop that replaced the twin crons (deleted 2026-08-21): it picks `ai_only` while free quota lasts and `search_only` otherwise, with no clock. The old "saver schedule" wording described windows that no longer exist.
- **window boxing / stop_at** — a run receives a deadline and exits gracefully at the window edge; unfinished pairs carry over to the next day.
- **standard mode** — DataForSEO's queued task API; production uses high priority (~40% cheaper than live, normally ≤1 minute) because normal priority can exceed the client timeout.
- **off-peak** — DeepSeek's discount window. Per their pricing page (read 2026-08-23) *peak* is 01:00–04:00 and 06:00–10:00 UTC on weekdays and everything else is off-peak at half price — not the 16:30–00:30 this line used to claim.
- **record_key** — DB uniqueness key: normalized `name|city|topic` (Unicode-safe, NFKC+casefold hashing for non-Latin names).
- **community_id** — stable public identity: SHA-256[:12] of `name.lower()|city.lower()`; survives re-scrapes, used in URLs and history.
- **joinable** — the LLM-emitted quality gate: recurring + open to the public + has group identity; only `joinable=True` records survive.
- **hidden** — moderation flag on a community (merged duplicate or approved "not a community" report); survives re-scrapes.
- **false positive** — an admin-curated negative example injected into extraction prompts so the same non-community stops being extracted.
- **thin page** — a community page without a description; noindexed and excluded from sitemaps.
- **canonical domain** — kozossegek.com for HU-city content even when served on meetapedia.com; everything else self-canonicalizes.
- **topic tier / core topics** — cities marked `topic_tier: core` (260 small Swedish kommuner) only run the 12 `pipeline.core_topics`; other pairs are frozen.
- **stock (állomány)** — current absolute totals (communities, venues, persons, pages, covered pairs) as opposed to daily diffs; both appear in the daily email.
- **collector / extractor** — the two halves of the continuous worker's job: `search_only` buys searches and fetches pages, `ai_only` extracts from what is already cached. They were separate crons until 2026-08-21; the names outlived the schedule.
- **pageview counter** — the bot-filtered server-side traffic middleware; fallback + footnote under GA4 numbers in the daily report.
