---
type: Concept
title: Extraction Provider Fallback Chain
description: FallbackExtractor is the one failure path for every provider; since 2026-08 it carries a routed free-tier fleet instead of a single DeepSeek.
tags: [llm, extraction, fallback, providers]
timestamp: 2026-08-16
resource: scraper/extract.py
---

# Extraction Provider Fallback Chain

*The current chain contains one provider: DeepSeek. The wrapper retains quota, rate-limit, retry, and future-fallback semantics.*

## Current provider

`run_pipeline` adds `DeepSeekExtractor` when `DEEPSEEK_API_KEY` is configured, then wraps the resulting list in `FallbackExtractor`. No key means an empty list and `exhausted=True`; there is no local or free fallback.

The wrapper tracks permanent HTTP-402 exhaustion and temporary HTTP-429 blocks per provider. It waits at most five minutes for the shortest rate-limit window and retries transient API/network failure once. If no call succeeds it raises `ExtractorUnavailableError`, which the pipeline records but does not cache as an empty extraction. See [[non-quota-errors-drop-page]] and [[extraction-layer]].

## Historical chain

DeepSeek → Groq existed before the 2026-07 provider cleanup. `GroqExtractor` and `GROQ_API_KEY` support were removed because the fallback was unused. The generic list wrapper and canonical fingerprints remain so a future provider can be added without redesigning cache semantics. See [[canonical-fingerprint-provider-shift]], [[extraction-fingerprints]], and [[doc-drift-project-readme]].

Related provider architecture: [[search-provider-fallback-chain]].
