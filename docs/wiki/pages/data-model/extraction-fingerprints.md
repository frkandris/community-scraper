---
type: Data-model
title: Extraction Fingerprints
description: Three SHA-256[:12] fingerprints key community, venue, and person cache results; canonical variants stay pinned to the configured primary.
tags: [fingerprint, cache, invalidation, providers]
timestamp: 2026-07-10
resource: scraper/extract.py
---

# Extraction Fingerprints

*A fingerprint is `SHA-256[:12]` of `prompt_text + model_name`. Changing either the prompt or the model changes the fingerprint and auto-invalidates the cache. See [[extraction-fingerprint-cache]].*

## Three independent fingerprints

`cache_pages` stores `extract_fingerprint` (communities), `venue_fingerprint`, and `person_fingerprint` — as both dedicated columns (fast SQL filtering in `get_scope_stats`, `get_city_topic_states`, `get_fully_processed_pairs`) and inside the JSON blob (source of truth). Each is computed over its own prompt family. The pipeline prefilter passes all three current fingerprints and only skips a pair when every enabled family is current.

## `canonical_fingerprint` — the done-pairs fix

`model_fingerprint` follows the first available provider; `canonical_fingerprint` always uses `primaries[0]`. DeepSeek is currently the only provider, so the values are equal. The split remains important if a fallback returns: every result must be stored under the same key used by done-pair detection. See [[canonical-fingerprint-provider-shift]] and [[extraction-provider-fallback-chain]].

The canonical treatment covers venues and persons too (`canonical_venue_fingerprint` / `canonical_person_fingerprint` on `FallbackExtractor`; all pipeline cache read/write sites use them).

## False positives use an explicit invalidation path

False-positive negative examples are appended **after** the fingerprinted `get_prompt(...)`, so a pair-level moderation action does not change the global fingerprint. `false_positives.add/remove` instead removes the affected rows' community extraction fields; global extraction rules invalidate all community extraction rows. The next done-pair check then selects precisely the stale scope. See [[false-positive-injection]].
