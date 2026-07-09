# Wiki Log

Date-grouped operation log, newest first. See [SCHEMA.md](SCHEMA.md).

## 2026-07-09
- **Update**: Proper provider-failure handling — new `ExtractorUnavailableError`; `_post` raises instead of returning `{}`; `FallbackExtractor._call` shared runner (quota→exhaust, rate-limit→wait ≤5 min, transient→one retry); pipeline skips caching on failure (no more permanent empty results), `FallbackSearchClient` raises `SearchQuotaError` when nothing could be searched so pairs aren't falsely marked done; run-end failure summary. Rewrote [[non-quota-errors-drop-page]] as fixed. docker-compose purged of searxng/ollama relics.
- **Deprecation**: Provider cleanup — only DeepSeek (LLM) and DataForSEO (search) remain. Removed: GroqExtractor, SerperSearchClient, GooglePlaywrightSearchClient, DuckDuckGoSearchClient, the local search worker (script + /admin/api/search endpoints + SEARCH_WORKER_TOKEN), dead locale tables, groq settings block. FallbackSearchClient/FallbackExtractor stay as single-provider wrappers. Updated: [[search-layer]], [[extraction-layer]], [[search-provider-fallback-chain]], [[extraction-provider-fallback-chain]], [[local-search-worker]] (removal note), [[deployment-coolify]] (obsolete env vars), hack pages.
- **Creation**: [[cost-optimization-2026-07]] — empty-search caching (recurring-leak fix), query short-circuit (`stop_after`), venue extraction gated on communities, canonical venue/person fingerprints, opt-in off-peak cron, DataForSEO standard mode, topic tiering (260 small Swedish kommuner → 12 core topics; −6,240 pairs).
- **Update**: [[scheduler-disabled-no-cron]] (cron now opt-in), [[extraction-fingerprints]] + [[canonical-fingerprint-provider-shift]] (venue/person canonical fix landed).
- **Creation**: [[local-search-worker]] — new `scripts/local_search_worker.py` + `/admin/api/search/{jobs,ingest}` endpoints let a residential-IP browser do Google searches and feed `search_cache`, replacing datacenter DataForSEO calls. `GooglePlaywrightSearchClient` gained a `headless` param.
- **Update**: Migrated the wiki to the combined Karpathy + OKF v0.1 format — rewrote SCHEMA.md, added `okf_version: "0.1"` to index.md, gave every page YAML frontmatter (`type` required), and switched log.md to date-grouped newest-first.
- **Creation**: New subsystem pages from a full-codebase sweep — [[persistence-layer]], [[search-layer]], [[fetch-layer]], [[extraction-layer]], [[pipeline-orchestration]], [[duplicate-detection]], [[web-app]], [[i18n-and-site-detection]].
- **Creation**: Data-model pages — [[sqlite-schema]], [[community-record]], [[person-record]], [[venue-record]], [[extraction-fingerprints]].
- **Creation**: Concept pages — [[joinable-quality-gate]], [[false-positive-injection]], [[done-pair-url-hash-not-city-topic]], [[fuzzy-dedup-and-record-key]], [[history-created-sentinel-overcounting]].
- **Creation**: Decisions — [[hungary-sweden-intl-three-passes]], [[scheduler-disabled-no-cron]], [[doc-drift-project-readme]].
- **Creation**: Hacks — [[canonical-fingerprint-provider-shift]], [[pyyaml-no-norway-boolean]], [[searchquotaerror-reraise-ordering]], [[non-quota-errors-drop-page]], [[get-prompt-empty-override-falls-back]], [[name-json-tail-bleed]], [[cache-blob-read-modify-write]], [[shared-run-task-slot]], [[url-hash-triplicated]].
- **Creation**: SEO + post-mortem for this session's work — [[indexing-strategy]], [[seo-cross-domain-canonical]], [[2026-06-seo-traffic-collapse]].
- **Creation**: Operations runbooks — [[run-modes-and-startup]], [[deployment-coolify]], [[adding-city-topic]].

## 2026-06-04
- **Update**: Session-3 — fixed amber cells never turning blue ([[2026-06-coverage-amber-cells]]); added coverage cell live-update + `/admin/api/restamp-fingerprints`; removed Hungarian example bias from SYSTEM_PROMPT ([[llm-prompt-language-bias]]); i18n'd meetapedia community pages; SEO groundwork (canonical, noindex, robots); split `/admin/stats` into 3 sub-pages.
- **Creation**: [[2026-06-coverage-amber-cells]], [[init-db-before-prompt-overrides]], [[llm-prompt-language-bias]].

## 2026-05-30
- **Update**: Session-2 — 290 Swedish municipalities; coverage page (country dropdown, 5 cell states, jump-to-active, JS live highlight); pipeline done-pair pre-filter via `get_fully_processed_pairs`; `on_pair_start` callback; `search_ttl_days` → 3650; Resend email notifications on 4 routes.
- **Creation**: Wiki initialized — pre-populated architecture, hacks, post-mortems, decisions, and concepts from codebase knowledge.
