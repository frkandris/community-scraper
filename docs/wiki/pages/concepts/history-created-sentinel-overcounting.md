---
type: Concept
title: History __created__ Sentinel and Overcounting
description: Brand-new records log a __created__ history row; delete+reinsert cycles re-log it, so activity timelines dedup via MIN(changed_at) — except for communities.
tags: [history, change-tracking, timeline, overcounting]
timestamp: 2026-07-09
resource: scraper/db.py
---

# History __created__ Sentinel and Overcounting

*When a record is brand-new (`old_data is None`), history logging inserts one row with `field="__created__"`. Otherwise it diffs each field and logs one row per change.*

## The overcounting trap

Several flows delete and re-insert records every AI run — e.g. `delete_leader_persons_for_community` wipes all leader persons before re-inserting parsed ones. Each cycle logs a **new** `__created__` row, so a churned person/venue would be counted as "new" repeatedly.

The activity timeline guards against this for venues and persons by wrapping the count in `SELECT <id>, MIN(changed_at) … GROUP BY <id>` — each entity counts once, at its earliest creation. **`new_communities` does not apply this MIN-dedup**, so churned communities *can* be overcounted in the "new communities" timeline. Inconsistent treatment worth remembering.

Related mechanism: `replace_communities_for_topic` snapshots existing rows before its DELETE so history can still diff old vs new (without the snapshot, every save would re-log every field as changed). `community_id` can be an empty string in history when a record lacks one, colliding all such records into one bucket. See [[persistence-layer]].
