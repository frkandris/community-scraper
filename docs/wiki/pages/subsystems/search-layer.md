---
type: Subsystem
title: Search Layer
description: Three interchangeable search clients (Google Playwright → DataForSEO → Serper) behind FallbackSearchClient, with a per-provider block/exhaust state machine.
tags: [search, playwright, dataforseo, serper, fallback, captcha]
timestamp: 2026-07-09
resource: scraper/search.py
---

# Search Layer

*`SerperSearchClient`, `DataForSEOClient`, and `GooglePlaywrightSearchClient` share one `search()`/`search_all()` interface; `FallbackSearchClient` stacks them cheapest-first.*

See [[search-provider-fallback-chain]] for the concept, [[fetch-layer]] for what happens to the URLs next.

## Provider order and rationale

Assembled in `pipeline.py:360` as `[GooglePlaywright, DataForSEO?, Serper?]`. Google Playwright is always first and **free** (headless Chromium scraping Google directly, 8 s between requests); the paid APIs are appended only if their credentials exist and only serve as fallbacks when the free path is CAPTCHA-blocked or empty.

## The exhaustion state machine

`FallbackSearchClient._blocked_until[i]` is a single float encoding three states: `0.0` = available, `float('inf')` = permanently exhausted, a future `monotonic()` timestamp = temporary cooldown. On `SearchQuotaError`:

- **Google Playwright** → 15-minute cooldown (`PLAYWRIGHT_COOLDOWN_SECONDS = 15*60`). One CAPTCHA early in a run must not disable Playwright for the remaining thousands of pairs.
- **Any paid API** → `float('inf')` (permanent). If credits are gone, retrying is pointless.

State is **per-instance**, not persisted — a fresh `FallbackSearchClient` is built each run, so last run's exhaustion is forgotten.

## `SearchQuotaError` is the only failover signal

Every quota/rate/payment/CAPTCHA condition is funneled into this one exception (Serper 402/429 or 400-with-"credit"/"quota"; DataForSEO 402/429 or API status 40201; Playwright CAPTCHA marker). It is the single thing `FallbackSearchClient` catches to roll to the next provider. **Any other exception is swallowed → `[]` with no failover.** See [[searchquotaerror-reraise-ordering]] for the ordering invariant this depends on.

Two failover semantics: `search()` (single query) advances **only** on `SearchQuotaError`; `search_all()` (batch) advances on `SearchQuotaError` **or** an empty result list. The pipeline uses `search_all`, so emptiness triggers fallback there.

## Query construction

`build_queries(city, variants, terms)` emits at most **3** queries: `terms[:2]` × the primary city variant, plus one `terms[0] × variants[1]` if a second variant exists. Empty `search_variants` falls back to `[city_name]`. This caps API fan-out per pair — critical at ~27,900 city×topic pairs (see [[pipeline-orchestration]]).

## Locale mapping tables

Four separate dicts because each provider wants a different geo/language encoding: `LOCALE_TO_SERPER` (gl/hl tuples), `LOCALE_TO_DATAFORSEO_LOCATION` (integer location codes, HU=2348), plus the currently-unused `LOCALE_TO_LANGUAGE` and `LOCALE_TO_BRAVE_COUNTRY` (dead tables — no Brave client exists here). Unmapped locales fail open to US/English.

## Gotchas

- **PyYAML "no" → False**: Norwegian locale `"no"` parses as boolean `False`; guarded by `str(locale)` casts. Playwright also remaps `hl == "no"` → `"nb"`. See [[pyyaml-no-norway-boolean]].
- **Result count double-capped**: Serper/Playwright request `min(num, 10)` then slice — can never return more than 10 even if config asks for more.
- **`search_all` dedups by exact URL string** — no trailing-slash/scheme/param normalization.
- **Playwright consent latch**: `_consent_done` only latches on the visible-button success branch; a page with no consent banner re-attempts consent every page.
- **Playwright is brittle to Google markup**: scraping selector `#search a:has(h3)` and snippet class `.VwiC3b` are Google's obfuscated names — they will break when Google changes its HTML.
- **Silent Chromium failure**: if the browser fails to launch, the client logs and returns `[]` (no raise), so `search_all` quietly moves to the next provider.
