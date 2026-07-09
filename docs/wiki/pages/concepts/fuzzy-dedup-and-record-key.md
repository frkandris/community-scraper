---
type: Concept
title: Fuzzy Dedup and record_key
description: store.py dedups records in-memory (fuzzy) and upserts by record_key = norm(name)|norm(city)|norm(topic), a formula duplicated in db.py that must stay identical.
tags: [dedup, record-key, store, normalization]
timestamp: 2026-07-09
resource: scraper/store.py
---

# Fuzzy Dedup and record_key

*`record_key = norm(name)|norm(city)|norm(topic)` where `norm(s) = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")`. It is the UNIQUE upsert key and drives conflict resolution.*

The formula is implemented **identically in both `store.py` and `db.py`** with no shared function — they must stay in sync or store-layer dedup and db-layer upsert disagree. `record_key` changes if name/city/topic change; the separate `community_id` is meant to be stable (see [[community-identity]]). Recategorizing a community's topic recomputes `record_key` while preserving `community_id`.

## In-memory fuzzy dedup

`store.py:_dedup` collapses near-duplicates before upsert: same website (trailing-slash-stripped), substring-after-article-strip, or `SequenceMatcher ratio > 0.88`. On collision it keeps the **richer** record (`_richness` counts populated fields + social_links). This is distinct from the cross-topic [[duplicate-detection]] that runs after a full pipeline.

`save_results` lets new records win on key collision; `patch_results` fills only NULL fields and never overwrites non-null.
