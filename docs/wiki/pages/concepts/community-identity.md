---
type: Concept
title: Community Identity
description: "Two keys: community_id (stable URL slug) vs record_key (topic-aware DB uniqueness)."
tags: [identity, hashing, keys]
timestamp: 2026-07-10
resource: scraper/models.py
---

# Community Identity

*A community's identity is determined by two separate keys with different purposes.*

## community_id

`SHA-256[:12]` of `name.lower() | city.lower()`.

**Purpose**: stable URL slug and public identifier. Survives topic re-categorization. Shown in URLs like `/community/{community_id}`.

## record_key

`c2:SHA-256[:24]` over NFKC+casefold canonical `(name, city, topic)` components.

**Purpose**: unique DB constraint. Prevents duplicate rows when the same group is found under different topics. Triggers an upsert (merge `source_urls`) instead of insert on conflict.

The implementation is centralized in `scraper.identity` and preserves non-Latin scripts; see [[unicode-safe-identity-keys]].

## Why two keys

`record_key` includes topic so the same group can appear under multiple topics (e.g., a hiking group that also does cycling). `community_id` doesn't include topic so the public URL doesn't change if the topic is corrected.

## Implication: topic changes don't affect URLs

If a community is re-categorized to a different topic, its `community_id` stays the same. The old `record_key` is deleted and a new one is created, but the public page URL is unchanged.

## Related

- [[false-positive-injection]]
- [[persistence-layer]]
