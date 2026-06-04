# Wiki Index

One line per page. LLM-maintained — update after every ingest or new page creation.

## Architecture

- [[two-domain-single-container]] — One FastAPI container serves both közösségek.com and meetapedia.com via Host header detection
- [[extraction-fingerprint-cache]] — SHA-256[:12] of (prompt + model) keys all extraction results; prompt change = full re-extraction
- [[pipeline-run-modes]] — full / ai_only / revalidate; Hungary → Sweden → rest priority ordering

## Hacks

- [[tailwind-cdn-jit-large-lists]] — Never server-render large lists; JIT scanner freezes the page
- [[asyncio-task-cancellation]] — Use asyncio.create_task, not BackgroundTasks; CancelledError is BaseException
- [[jinja2-macro-definition-order]] — Macros must be defined before they're called; Jinja2 does not hoist
- [[jinja2-namespace-mutable-counter]] — Use namespace() for mutable variables inside Jinja2 for loops
- [[playwright-vs-blocked-domain-ordering]] — Playwright check runs before blocked-domain check; social domains must stay out of playwright_domains
- [[init-db-before-prompt-overrides]] — init_db() runs before overrides load; fingerprint migrations must use a runtime endpoint, not init_db()
- [[llm-prompt-language-bias]] — Example strings in the system prompt bias LLM output language for all cities; keep examples in English

## Post-mortems

- [[2026-05-coverage-page-500]] — Coverage page 500 error: app_state cities/topics are dataclasses, not dicts
- [[2026-06-coverage-amber-cells]] — Amber cells never turned blue: get_fully_processed_pairs() and get_city_topic_states() disagreed on which URLs count

## Decisions

- [[search-ttl-3650-days]] — TTL set to 10 years: index the world first, worry about freshness later
- [[sweden-pipeline-priority]] — Sweden runs second after Hungary due to 290-municipality city list size

## Concepts

- [[community-identity]] — Two keys: community_id (stable URL slug) vs record_key (topic-aware DB uniqueness)
- [[search-provider-fallback-chain]] — Google Playwright → DataForSEO → Serper with per-run exhaustion tracking
- [[extraction-provider-fallback-chain]] — DeepSeek → Groq; different models = different fingerprints
