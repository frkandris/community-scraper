---
type: Subsystem
title: Duplicate Detection
description: detect_all() finds same-city duplicate communities/venues/persons via URL match and fuzzy name similarity, with a stable canonical key so re-scans are idempotent.
tags: [duplicates, dedup, fuzzy-matching, moderation]
timestamp: 2026-07-24
resource: scraper/duplicates.py
---

# Duplicate Detection

*`detect_all(db_path)` runs automatically after every pipeline run (fire-and-forget) and from `POST /admin/duplicates/scan`. It writes `duplicate_candidates` rows for admin review.*

## What it compares

Three entity types, all **same-city scoped**:

- **Communities** — only compares **different-topic** pairs (same-topic is already deduped in `store.py`). Signals: `url_match` (normalized websites equal → similarity 1.0) or `fuzzy_name` when `_name_similarity ≥ 0.85`.
- **Venues** — same-city, plain similarity ≥ 0.85 or url_match.
- **Persons** — name similarity ≥ 0.90 **and** community-name similarity ≥ 0.70. The second gate stops the same person leading two different communities from being flagged as a duplicate.

## Name-similarity subtleties (hard-won)

- `_strip_city` removes the city name **and** its Hungarian adjectival `+i` form (Pécs→Pécsi) before comparison, so "Budapest Futók" vs "Budapest Focisták" don't inflate similarity via the shared "Budapest".
- Returns 0.0 if the shorter stripped name is < 5 chars or is a single generic word.
- Substring containment (either name ⊂ other, both > 4 chars) forces 0.90.
- `_GENERIC_WORDS` is a curated HU+EN frozenset (klub, kör, egyesület, csoport, group, club, association, plus suffix traps színpad/stúdió) that alone must not count as a match.
- `_norm_url` treats junk values (`n/a, empty, none, #, /`) as empty so they never produce a false url_match.

## Idempotency

`_richness` picks the winner (more filled fields + social_links) and since 2026-07-24 the stored `(winner_key, loser_key)` **follows richness** — winner_key is what a merge keeps, so the earlier canonical string ordering silently kept the poorer record and ignored the admin's manual "keep" choice. Re-scan idempotency moved into `insert_duplicate_candidate`, which checks the pair in **both key orders**. The old code also swapped the loop variables (`a, b = b, a`), leaking the previous candidate into later inner-loop comparisons — the loop variables are no longer mutated. `cleanup_stale_community_candidates` auto-dismisses pending candidates whose records vanished.

## Merge

`POST /admin/duplicates/{id}/merge` for communities backgrounds `_ai_merge_communities` (LLM merges records into the winner_key, hides the loser via `set_community_hidden`, unions `source_urls`); on LLM failure it falls back to `merge_community_into`. Non-community entities just mark `"merged"` without an actual merge.
