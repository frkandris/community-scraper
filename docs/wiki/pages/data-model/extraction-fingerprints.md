---
type: Data-model
title: Extraction Fingerprints
description: Three SHA-256[:12] fingerprints (community/venue/person) key the cache; the canonical variant pins to the primary provider so fallback extractions still count as done.
tags: [fingerprint, cache, invalidation, providers]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# Extraction Fingerprints

*A fingerprint is `SHA-256[:12]` of `prompt_text + model_name`. Changing either the prompt or the model changes the fingerprint and auto-invalidates the cache. See [[extraction-fingerprint-cache]].*

## Three independent fingerprints

`cache_pages` stores `extract_fingerprint` (communities), `venue_fingerprint`, and `person_fingerprint` — as both dedicated columns (fast SQL filtering in `get_scope_stats`, `get_city_topic_states`, `get_fully_processed_pairs`) and inside the JSON blob (source of truth). Each is computed over its own prompt family.

## `canonical_fingerprint` — the done-pairs fix

`model_fingerprint` returns the **first-available** provider's fingerprint — so when DeepSeek is exhausted, pages extracted by Groq get stored under Groq's fingerprint, which never matches the done-pairs check (that always uses `primaries[0]`). `canonical_fingerprint` always returns `primaries[0].model_fingerprint`, and the community cache read/write path uses it, so every extraction — whichever provider ran — is stored under the key the done-pairs check looks for. See [[canonical-fingerprint-provider-shift]].

**Incomplete fix:** the canonical treatment was applied to communities only. `venue_fingerprint`/`person_fingerprint` still use the provider-shifting variants, so venues/persons extracted by Groq can be re-extracted when DeepSeek recovers.

## Not affected by false positives

False-positive negative examples are appended to the prompt **after** the fingerprinted `get_prompt(...)`, so adding/removing false positives does **not** change the fingerprint or invalidate the cache. See [[false-positive-injection]].
