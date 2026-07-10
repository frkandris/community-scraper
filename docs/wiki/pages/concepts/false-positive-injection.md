---
type: Concept
title: False-Positive Injection
description: Admin negatives feed both extraction paths and explicitly invalidate only the affected community-extraction cache.
tags: [false-positives, prompt, quality, moderation]
timestamp: 2026-07-10
resource: scraper/false_positives.py
---

# False-Positive Injection

*Admin-marked false positives are appended to the system message as negative examples at call time. Pair examples affect one `(city, topic)`; extraction rules affect every pair.*

`build_prompt_section(fps, city, topic, fp_type)` builds a text block: for extraction, a `NEGATIVE EXAMPLES — … do NOT extract them:` list plus `ADDITIONAL EXTRACTION RULES:` from `extraction_rule`-type entries. `FP_TYPES = ("extraction", "enrichment", "extraction_rule")`. Prompt-version history is stored with a monotonically increasing `version`, skipping the write if the effective text is unchanged.

Both `_run_full` and `_run_ai_only` load the current false-positive list and pass the pair-scoped section to `extract()`. A cache-only run therefore applies the same quality rules as a full run.

## Explicit, targeted invalidation

The injected section still sits **after** fingerprinted `get_prompt("extraction_system")`, so a pair-specific example does not change the global [[extraction-fingerprints|fingerprint]]. Instead, `add()` and `remove()` explicitly strip the community extraction fields from the affected cache rows:

- a normal `extraction` example resolves the pair's URLs through `search_cache`, the denormalized `cache_pages.city/topic`, and the explicit `source_url`;
- an `extraction_rule` is global, so it invalidates every community extraction;
- `raw_text` survives, as do the independent venue/person cache fields, so the next `ai_only` pass can re-extract without search or fetch cost.

The done-pair prefilter sees the missing community extraction metadata and schedules that pair again. This avoids both failure modes of the former design: cached pages no longer ignore a new negative rule, and one local moderation action no longer forces a world-wide re-extraction.

Enrichment negatives are different: enrichment runs only inside `_run_full`, so they affect future enrichment calls but do not currently trigger cache invalidation.

Related: [[joinable-quality-gate]], [[extraction-layer]].
