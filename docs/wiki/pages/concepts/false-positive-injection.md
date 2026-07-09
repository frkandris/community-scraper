---
type: Concept
title: False-Positive Injection
description: Admins mark bad extractions; those become negative examples appended to the system prompt at call time — but they do not invalidate the cache.
tags: [false-positives, prompt, quality, moderation]
timestamp: 2026-07-09
resource: scraper/false_positives.py
---

# False-Positive Injection

*Admin-marked false positives are appended to the system message as negative examples at call time, scoped to the relevant (city, topic) pair for extraction (global for enrichment).*

`build_prompt_section(fps, city, topic, fp_type)` builds a text block: for extraction, a `NEGATIVE EXAMPLES — … do NOT extract them:` list plus `ADDITIONAL EXTRACTION RULES:` from `extraction_rule`-type entries. `FP_TYPES = ("extraction", "enrichment", "extraction_rule")`. Prompt-version history is stored with a monotonically increasing `version`, skipping the write if the effective text is unchanged.

## The cache trap

The section is appended **after** `get_prompt("extraction_system")`, i.e. *after* the fingerprinted portion. So **adding or removing a false positive does not change the [[extraction-fingerprints|fingerprint]] and does not invalidate the extraction cache** — already-cached pages won't pick up new negative rules until the fingerprint changes for another reason (prompt or model edit). Also note `_run_ai_only` doesn't pass the false-positive examples at all, so cache-only re-extractions skip them.

Related: [[joinable-quality-gate]], [[extraction-layer]].
