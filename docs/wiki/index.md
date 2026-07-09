---
okf_version: "0.1"
---

# Wiki Index

Content catalog. One line per page, grouped by category. Mirrors each page's `description`
frontmatter. See [SCHEMA.md](SCHEMA.md) for conventions.

## Architecture

- [[two-domain-single-container]] — One FastAPI container serves közösségek.com and meetapedia.com via Host-header detection
- [[extraction-fingerprint-cache]] — SHA-256[:12] of (prompt + model) keys all extraction; changing either forces re-extraction
- [[pipeline-run-modes]] — full / ai_only / revalidate control how much work runs per pair
- [[indexing-strategy]] — canonical, thin-page noindex, sitemap scoping, robots, JSON-LD

## Subsystems

- [[persistence-layer]] — db.py owns SQL, cache.py is a JSON-blob facade, store.py merges/dedups
- [[search-layer]] — Google Playwright → DataForSEO → Serper behind a block/exhaust state machine
- [[fetch-layer]] — httpx + trafilatura (html2text fallback); blocked domains and optional Playwright fetch
- [[extraction-layer]] — DeepSeek → Groq; four prompt families; live-editable prompts; fingerprint cache
- [[pipeline-orchestration]] — run_pipeline sequences ai_only + full with a done-pair pre-filter
- [[duplicate-detection]] — same-city fuzzy dedup of communities/venues/persons; idempotent re-scans
- [[web-app]] — public + /admin routers, pure-ASGI Basic auth, app_state, coverage page
- [[i18n-and-site-detection]] — Host-based site detection; English-base translations; missing keys render as themselves

## Data model

- [[sqlite-schema]] — every table in scraper.db and its purpose
- [[community-record]] — the core entity with multilingual auto-cleanup and a stable community_id
- [[person-record]] — leaders/instructors; two-word name rule; role normalized to 12 values
- [[venue-record]] — physical locations; spans topics via welcomed_topics
- [[extraction-fingerprints]] — three fingerprints; canonical variant pins to the primary provider

## Concepts

- [[community-identity]] — community_id (stable URL slug) vs record_key (topic-aware DB uniqueness)
- [[search-provider-fallback-chain]] — Google Playwright → DataForSEO → Serper with per-run exhaustion
- [[extraction-provider-fallback-chain]] — DeepSeek → Groq; different models = different fingerprints
- [[joinable-quality-gate]] — only joinable=True records survive; a 3-condition AND rule
- [[false-positive-injection]] — admin negatives appended to the prompt; do NOT invalidate the cache
- [[done-pair-url-hash-not-city-topic]] — done detection resolves URLs to hashes, never JOINs on city/topic
- [[fuzzy-dedup-and-record-key]] — in-memory fuzzy dedup + record_key derivation duplicated across two files
- [[history-created-sentinel-overcounting]] — __created__ rows and the MIN(changed_at) dedup (skipped for communities)

## Decisions

- [[search-ttl-3650-days]] — TTL ~10 years: index the world first, worry about freshness later
- [[sweden-pipeline-priority]] — Sweden runs second after Hungary (290 municipalities)
- [[hungary-sweden-intl-three-passes]] — three sequential run_pipeline calls; order = business priority
- [[scheduler-disabled-no-cron]] — cron is opt-in (`cron_enabled`); preset to DeepSeek's off-peak window
- [[cost-optimization-2026-07]] — eight levers cutting DataForSEO + LLM spend (empty-search caching, short-circuit, tiering…)
- [[doc-drift-project-readme]] — PROJECT.md/README.md describe retired providers; trust the code

## Hacks

- [[tailwind-cdn-jit-large-lists]] — never server-render large lists; the JIT scanner freezes the page
- [[asyncio-task-cancellation]] — use asyncio.create_task + _run_task; CancelledError is a BaseException
- [[jinja2-macro-definition-order]] — macros must be defined before they're called; Jinja2 does not hoist
- [[jinja2-namespace-mutable-counter]] — use namespace() for mutable variables inside Jinja2 loops
- [[playwright-vs-blocked-domain-ordering]] — Playwright check runs before blocked-domain check
- [[init-db-before-prompt-overrides]] — fingerprint migrations must use a runtime endpoint, not init_db()
- [[llm-prompt-language-bias]] — non-English prompt examples bias output language; keep examples English
- [[canonical-fingerprint-provider-shift]] — pin the cache key to primaries[0] so fallback extractions still count
- [[pyyaml-no-norway-boolean]] — locale "no" parses as boolean False; cast to str
- [[searchquotaerror-reraise-ordering]] — re-raise SearchQuotaError before the broad except, or failover breaks
- [[non-quota-errors-drop-page]] — only 402/429 trigger fallback; other errors silently drop the page
- [[get-prompt-empty-override-falls-back]] — an empty-string prompt override reverts to the default
- [[name-json-tail-bleed]] — strip leaked JSON tail off the name field
- [[cache-blob-read-modify-write]] — cache_pages is a non-transactional read-modify-write blob
- [[shared-run-task-slot]] — pipeline and revalidate share one _run_task slot; stop can hit the wrong task
- [[url-hash-triplicated]] — SHA-256(url)[:16] is duplicated in three places; must stay identical

## SEO

- [[seo-cross-domain-canonical]] — HU pages canonicalize to kozossegek.com so Google consolidates correctly

## Post-mortems

- [[2026-05-coverage-page-500]] — app_state cities/topics are dataclasses, not dicts
- [[2026-06-coverage-amber-cells]] — get_fully_processed_pairs() and get_city_topic_states() disagreed on which URLs count
- [[2026-06-seo-traffic-collapse]] — ~20K pages devalued to "Crawled - currently not indexed"; cross-domain duplication

## Operations

- [[run-modes-and-startup]] — how to trigger runs; the startup escalation state machine
- [[deployment-coolify]] — Docker on Coolify; volumes; env vars
- [[adding-city-topic]] — config files + app.py dicts + i18n labels to update in lockstep
- [[local-search-worker]] — offload Google search to your machine's browser, feeding search_cache via an admin API
