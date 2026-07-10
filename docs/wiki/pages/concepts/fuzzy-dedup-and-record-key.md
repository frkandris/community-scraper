---
type: Concept
title: Fuzzy Dedup and record_key
description: store.py dedups records in-memory (fuzzy) and upserts through the shared Unicode-safe community record-key helper.
tags: [dedup, record-key, store, normalization]
timestamp: 2026-07-10
resource: scraper/store.py
---

# Fuzzy Dedup and record_key

*`record_key` is a deterministic hash of canonical Unicode identity fields. It is the UNIQUE upsert key and drives conflict resolution.*

`store.py`, `db.py`, and duplicate detection all call `scraper.identity.community_record_key`, so their equality semantics cannot drift. `record_key` changes if name/city/topic change; the separate `community_id` is meant to be stable (see [[community-identity]]). Recategorizing a community's topic recomputes `record_key` while preserving `community_id`. See [[unicode-safe-identity-keys]].

## In-memory fuzzy dedup

`store.py:_dedup` collapses near-duplicates before upsert: same website (trailing-slash-stripped), substring-after-article-strip, or `SequenceMatcher ratio > 0.88`. On collision it keeps the **richer** record (`_richness` counts populated fields + social_links). This is distinct from the cross-topic [[duplicate-detection]] that runs after a full pipeline.

`save_results` lets new records win on key collision; `patch_results` fills only NULL fields and never overwrites non-null.
