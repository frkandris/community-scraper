---
type: Hack
title: canonical_fingerprint Pins the Cache Key to the Primary Provider
description: model_fingerprint shifts to the fallback when the primary is exhausted, so cache reads/writes use canonical_fingerprint (always primaries[0]) to match the done-pairs check.
tags: [fingerprint, cache, providers, deepseek, groq]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# canonical_fingerprint Pins the Cache Key to the Primary Provider

*`model_fingerprint` returns the first-**available** provider's fingerprint; `canonical_fingerprint` always returns `primaries[0].model_fingerprint`.*

## The bug it fixes

When DeepSeek is exhausted mid-run, extraction shifts to Groq. If cache reads/writes used `model_fingerprint`, those pages would be stored under **Groq's** fingerprint — but the done-pairs check ([[done-pair-url-hash-not-city-topic]]) always computes the fingerprint from `primaries[0]` (DeepSeek). The pages would never match, so the pair would look "not done" forever and be re-extracted every run.

The community cache read/write path uses `canonical_fingerprint`, so every extraction — whichever provider actually ran — is stored under the same key the done-pairs check looks for.

## Incomplete

The canonical fix was applied to **communities only**. `venue_fingerprint`/`person_fingerprint` still use the provider-shifting variants, so venues/persons extracted by Groq can be re-extracted when DeepSeek recovers. See [[extraction-fingerprints]].
