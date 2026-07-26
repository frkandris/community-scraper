---
okf_version: "0.1"
---

# Wiki Index

Content catalog. One line per page, grouped by category. Mirrors each page's `description`
frontmatter. See [SCHEMA.md](SCHEMA.md) for page format and [CLAUDE.md](CLAUDE.md) for
maintenance triggers. Root-level companions: [glossary.md](glossary.md) (domain
vocabulary), [faq.md](faq.md) (recurring questions).

## Architecture

- [[two-domain-single-container]] — One FastAPI container serves közösségek.com and meetapedia.com via Host-header detection.
- [[end-to-end-pair-walkthrough]] — One worked example — (Szentendre, running) — traced from scheduler wake-up through search, fetch, extraction, storage, and the public page, naming every file on the path.
- [[extraction-fingerprint-cache]] — SHA-256[:12] of each prompt family plus model keys extraction caches; changing either makes the corresponding results stale.
- [[pipeline-run-modes]] — full / ai_only / search_only control how much work runs per city×topic pair (revalidate was removed 2026-07-23).
- [[indexing-strategy]] — Canonical tags, thin-page noindex, domain-scoped sitemaps, and robots rules that keep the two-domain directory from cannibalizing its own search rankings.

## Subsystems

- [[persistence-layer]] — db.py owns all SQL, cache.py is a JSON-blob facade over cache_pages, store.py merges/dedups community records before upsert.
- [[search-layer]] — DataForSEO is the sole search client (live or standard mode) behind the FallbackSearchClient wrapper with per-run exhaustion state.
- [[fetch-layer]] — An SSRF-safe httpx/Playwright fetcher validates public DNS and redirects before trafilatura/html2text turns HTML into clean text.
- [[extraction-layer]] — DeepSeek LLM extraction of communities, venues, and persons from page text, with four prompt families, live-editable prompts, and fingerprint-keyed caching.
- [[pipeline-orchestration]] — run_pipeline() sequences mode-specific passes with a done-pair pre-filter; bounded saver jobs prioritize Sweden while startup recovery remains Hungary-first.
- [[duplicate-detection]] — detect_all() finds same-city duplicate communities/venues/persons via URL match and fuzzy name similarity, with a stable canonical key so re-scans are idempotent.
- [[wrong-city-detection]] — scan() flags communities whose text fields mention another known city — a strong signal the record landed under the wrong city; admin review at /admin/wrong-city with a one-click move that merges on identity conflict.
- [[web-app]] — One FastAPI app with a public router and an /admin router gated by pure-ASGI Basic auth; Hungarian paths are canonical and English paths redirect.
- [[i18n-and-site-detection]] — _detect_site reads the Host header; lang_context injects an i18n + nav bundle into every template. English is the translation base; missing keys render as themselves.
- [[daily-report]] — report.py builds one email per UTC day — GA4 visitors, per-site diffs, run outcomes, and current stock totals — sent via Resend at 04:30 UTC or on demand.

## Integrations

- [[dataforseo]] — The sole paid search provider — live mode ($2/1K, seconds) vs standard task queue ($0.6/1K, minutes); quota and transient failures raise typed errors that are never cached.
- [[deepseek]] — The sole LLM extractor — OpenAI-compatible chat API; the 2026-07 peak-valley pricing (2× at UTC 01–04 and 06–10) and the v4 model rename shape the extract schedule and the fingerprint_model cache pin.
- [[resend-email]] — All outbound email (feedback routes + daily report) goes through Resend from info@kozossegek.com; the free plan allows one verified domain, so meetapedia.com has no sender identity.
- [[ga4-reporting]] — The daily email reads visitor/session/pageview numbers from the GA4 Data API via a service account; property 536914034 covers both domains, split by hostName.

## Data model

- [[sqlite-schema]] — Every table in scraper.db, its purpose, and its key columns — all created idempotently by init_db().
- [[community-record]] — The core entity — a pydantic model with aggressive multilingual auto-cleanup and a stable derived community_id.
- [[person-record]] — Leaders/instructors extracted per community; enforces a two-word name rule and normalizes role to one of 12 values (default "leader").
- [[venue-record]] — Physical locations that host communities; spans topics via welcomed_topics rather than a topic column.
- [[extraction-fingerprints]] — Three SHA-256[:12] fingerprints key community, venue, and person cache results; canonical variants stay pinned to the configured primary.
- [[unicode-safe-identity-keys]] — Entity record keys hash NFKC+casefold canonical text, preventing non-Latin names from collapsing to the same database key.

## Concepts

- [[community-identity]] — Two keys: community_id (stable URL slug) vs record_key (topic-aware DB uniqueness).
- [[search-provider-fallback-chain]] — DataForSEO is the sole search provider (2026-07 cleanup); FallbackSearchClient remains as a single-provider wrapper with per-run exhaustion.
- [[extraction-provider-fallback-chain]] — DeepSeek is the sole extractor (2026-07 cleanup); FallbackExtractor remains as a single-provider wrapper.
- [[joinable-quality-gate]] — The primary quality filter — only records the LLM marks joinable=True survive; a 3-condition AND rule defines it.
- [[false-positive-injection]] — Admin negatives feed both extraction paths and explicitly invalidate only the affected community-extraction cache.
- [[done-pair-url-hash-not-city-topic]] — Done-pair detection resolves capped search URLs to hashes and checks every extraction family enabled for the current run mode.
- [[fuzzy-dedup-and-record-key]] — store.py dedups records in-memory (fuzzy) and upserts through the shared Unicode-safe community record-key helper.
- [[history-created-sentinel-overcounting]] — Brand-new records log __created__; every activity/report query groups by stable entity ID and MIN(changed_at) to neutralize delete-reinsert churn.
- [[not-community-moderation-flow]] — Public reports stay pending and cannot hide records; only admin approval hides the community and creates a false-positive example.
- [[server-side-url-safety]] — Every server-side fetch validates HTTP(S) syntax, public DNS answers, blocked domains, and each redirect target before connecting.

## Decisions

- [[search-ttl-3650-days]] — TTL set to ~10 years: index the world first, worry about freshness later.
- [[sweden-pipeline-priority]] — Bounded saver jobs prioritize Sweden before world and Hungary so the active expansion market cannot be starved by legacy work.
- [[hungary-sweden-intl-three-passes]] — main.py partitions Hungary, Sweden, and world into independent passes; bounded saver jobs are expansion-first while startup recovery is Hungary-first.
- [[scheduler-disabled-no-cron]] — APScheduler registers the enabled twin cost-saver jobs and daily report; the legacy combined cron remains opt-in.
- [[cost-optimization-2026-07]] — Cost controls reduce paid search and LLM work through caching, query short-circuiting, venue gates, off-peak extraction, standard search, and topic tiers.
- [[admin-simplification-2026-07]] — Removed the revalidate, recategorize, description-maintenance and Full Rebuild admin flows; the admin now centers on low-cost world indexing plus a user-interaction Inbox with pending badges.
- [[doc-drift-project-readme]] — PROJECT.md was archived behind an out-of-date banner on 2026-07-25; README.md is now a project introduction, CLAUDE.md the agent brief (AGENTS.md is generated from it), and this wiki the technical source of truth.

## Hacks

- [[tailwind-cdn-jit-large-lists]] — The CDN JIT scans the full initial DOM before paint; load big admin lists via JSON + DocumentFragment.
- [[asyncio-task-cancellation]] — Long runs use asyncio.create_task through RunCoordinator; BackgroundTasks cannot be cancelled and CancelledError is a BaseException.
- [[jinja2-macro-definition-order]] — Jinja2 does not hoist macro definitions; a macro called before its block fails at render, silently if the branch is skipped.
- [[jinja2-namespace-mutable-counter]] — Use namespace() to mutate an outer variable from inside a {% for %} block.
- [[playwright-vs-blocked-domain-ordering]] — URL safety and blocked-domain checks run before Playwright, and browser requests repeat the public-address guard.
- [[init-db-before-prompt-overrides]] — Fingerprint migrations must use a runtime endpoint, not init_db(), because overrides aren't loaded yet at init.
- [[llm-prompt-language-bias]] — Non-English example strings in SYSTEM_PROMPT make the LLM emit that language for all cities; keep examples English.
- [[canonical-fingerprint-provider-shift]] — Canonical community, venue, and person fingerprints always use primaries[0], keeping cache keys stable if fallback providers return.
- [[pyyaml-no-norway-boolean]] — The Norwegian locale code "no" is read by PyYAML as False; config and search boundaries cast locale keys back to strings.
- [[searchquotaerror-reraise-ordering]] — DataForSEO and its wrapper preserve quota versus transient errors so the pipeline never caches provider failure as a legitimate empty search.
- [[non-quota-errors-drop-page]] — FIXED 2026-07-09 — transient/quota extractor failures now raise ExtractorUnavailableError; the pipeline skips caching so the page is retried next run.
- [[get-prompt-empty-override-falls-back]] — get_prompt uses `or`, so an override set to "" is falsy and falls back to the built-in prompt — you cannot blank a prompt via override.
- [[name-json-tail-bleed]] — The LLM sometimes appends following JSON fields into the name string; _LEAKED_JSON_RE strips the leaked tail.
- [[cache-blob-read-modify-write]] — CacheManager reads the JSON blob, mutates it in Python, and writes it back across two separate connections — concurrent writers to the same URL can lose updates.
- [[shared-run-task-slot]] — Pipeline, scheduled, and startup runs reserve one coordinator-owned task slot with identity-safe cleanup.
- [[url-hash-triplicated]] — SHA-256(url)[:16] is repeated across cache, DB, pipeline, and web paths; every copy must remain byte-for-byte compatible.
- [[function-local-import-shadowing]] — A from-import inside one branch makes the name local to the entire function, so other branches crash with UnboundLocalError even though the module-level import exists.
- [[extractor-circuit-breaker]] — A dead LLM provider now fails a run in seconds — one live preflight extraction before the pair loops, plus a breaker that opens after 20 consecutive failed calls.

## SEO

- [[seo-cross-domain-canonical]] — HU-city pages on meetapedia.com canonicalize to kozossegek.com so Google stops consolidating the duplicate toward the traffic-less domain.
- [[country-landing-pages]] — Path-based /cities/<slug> country pages replace the ?country= query form (301'd) — self-canonical, sitemap-listed, and reachable from home headings and the /cities country index.
- [[sister-site-cross-links]] — The twin-record link between kozossegek.com and meetapedia.com — an icon on the community card only, same path, no redirect hop, suppressed where the twin would 302 home.

## Post-mortems

- [[2026-05-coverage-page-500]] — app_state.cities/topics are dataclasses, not dicts; dict-style access 500s any route touching them.
- [[2026-06-coverage-amber-cells]] — get_fully_processed_pairs() and get_city_topic_states() disagreed on which URLs count as done.
- [[2026-06-seo-traffic-collapse]] — kozossegek.com organic clicks fell from ~95/day to ~0 around 2026-06-01 as ~20K pages were devalued to "Crawled - currently not indexed."
- [[2026-07-bug-hunt]] — Three-agent review found 15+ verified defects; fixed in three batches — moderation survival, domain matching, persons lookup, recategorize, venue scope, timeline dedup, and a set of hot-path optimizations.
- [[2026-07-ga4-env-buildtime-failure]] — A multiline JSON secret marked "Available at Buildtime" in Coolify was injected as a Dockerfile ARG and broke the build parse; runtime-only env vars fixed it.
- [[2026-07-search-only-cache-replay]] — The first saver collector replayed extraction-cache records into Hungarian communities and retried pairs forever when any selected URL could not be fetched.
- [[2026-07-search-provider-down-noise]] — Unmapped city locales made every task_post fail with 40501 Invalid Field location_name; the fail-fast then amplified 3 poisoned pairs into 4972 logged failures while the email lost the original error.
- [[2026-07-wrong-city-approve-conflict]] — Approving a wrong_city edit request failed with "community not found or unsupported change type" — apply_community_edit collapsed three distinct failures into one boolean, hiding that the record already existed under the correct city.
- [[2026-07-deepseek-model-retired]] — DeepSeek dropped the deepseek-chat model name and the whole 2026-07-24 ai_only window failed with 1368 uncached pages; the fix swaps to deepseek-v4-flash while fingerprint_model pins the cache identity so 74K cached extractions survive.
- [[2026-07-deploy-truncates-collector]] — A deploy landing inside the 15 h search_only window kills the in-flight collector; with auto_run_on_startup off it never resumed and lost the rest of the day's page collection — invisible because the evening extractor lived off the cached-page backlog.

## Operations

- [[run-modes-and-startup]] — How to trigger runs (dashboard cards, manual, startup) and how the startup escalates ai_only → full.
- [[deployment-coolify]] — Docker on Coolify; persist only /app/data and /app/config; required and optional env vars.
- [[adding-city-topic]] — The config files plus the app.py dicts and i18n labels you must update in lockstep.
- [[local-search-worker]] — REMOVED 2026-07-09 — browser-driven search never beat engine bot detection; kept as post-mortem. Code in git history.
- [[cost-saver-schedule]] — Two independent daily crons — DataForSEO collects cheaply all day (search_only, standard mode), DeepSeek extracts only in its off-peak discount window (ai_only, stop_at-boxed).
- [[coolify-disk-cleanup]] — High-disk-usage alerts after deploy-heavy days are old Docker images and build cache; prune them from the server terminal — volumes and running containers are untouched.
