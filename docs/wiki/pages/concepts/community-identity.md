# Community Identity

*A community's identity is determined by two separate keys with different purposes.*

## community_id

`SHA-256[:12]` of `name.lower() | city.lower()`.

**Purpose**: stable URL slug and public identifier. Survives topic re-categorization. Shown in URLs like `/community/{community_id}`.

## record_key

`norm(name) | norm(city) | norm(topic)` where `norm()` strips punctuation and lowercases.

**Purpose**: unique DB constraint. Prevents duplicate rows when the same group is found under different topics. Triggers an upsert (merge `source_urls`) instead of insert on conflict.

## Why two keys

`record_key` includes topic so the same group can appear under multiple topics (e.g., a hiking group that also does cycling). `community_id` doesn't include topic so the public URL doesn't change if the topic is corrected.

## Implication: topic changes don't affect URLs

If a community is re-categorized to a different topic, its `community_id` stays the same. The old `record_key` is deleted and a new one is created, but the public page URL is unchanged.

## Related

- [[false-positives]]
- [[upsert-source-urls-merge]]
