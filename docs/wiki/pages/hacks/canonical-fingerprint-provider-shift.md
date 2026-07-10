---
type: Hack
title: canonical_fingerprint Pins the Cache Key to the Primary Provider
description: Canonical community, venue, and person fingerprints always use primaries[0], keeping cache keys stable if fallback providers return.
tags: [fingerprint, cache, providers, deepseek]
timestamp: 2026-07-10
resource: scraper/extract.py
---

# canonical_fingerprint Pins the Cache Key to the Primary Provider

*The current one-provider chain makes canonical and active fingerprints equal, but the distinction protects done-pair semantics if a fallback is configured again.*

## Historical bug

When DeepSeek → Groq fallback still existed, `model_fingerprint` followed the first available provider. Pages handled by Groq were stored under a different model hash, while done-pair detection computed DeepSeek's hash from `primaries[0]`; those pages looked stale forever.

Community cache reads/writes therefore use `canonical_fingerprint`, which always returns `primaries[0].model_fingerprint`. The same invariant now covers `canonical_venue_fingerprint` and `canonical_person_fingerprint`. See [[extraction-fingerprints]] and [[done-pair-url-hash-not-city-topic]].

Groq was removed in 2026-07 ([[extraction-provider-fallback-chain]]), but retaining this split makes reintroducing a provider safe by default.
