---
type: Concept
title: History __created__ Sentinel and Overcounting
description: Brand-new records log __created__; every activity/report query groups by stable entity ID and MIN(changed_at) to neutralize delete-reinsert churn.
tags: [history, change-tracking, timeline, overcounting]
timestamp: 2026-07-10
resource: scraper/db.py
---

# History __created__ Sentinel and Overcounting

*When a record is brand-new (`old_data is None`), history logging inserts one row with `field="__created__"`. Otherwise it diffs each field and logs one row per change.*

## The overcounting trap and its fix

Several flows delete and re-insert records — e.g. `delete_leader_persons_for_community` wipes leader rows before inserting the latest parse. The stable entity ID can therefore acquire multiple `__created__` rows.

Both activity aggregators (`get_activity_timeline`, `get_daily_summary`) now derive first-seen time as `MIN(changed_at) GROUP BY <entity_id>` for communities, venues, and persons. A churned entity is counted once, on its earliest creation. This must remain symmetrical across all three families; the 2026-07 bug hunt fixed the former communities-only exception. See [[2026-07-bug-hunt]].

Related mechanism: `replace_communities_for_topic` snapshots existing rows before its DELETE so history can still diff old vs new (without the snapshot, every save would re-log every field as changed). Models generate stable IDs before persistence; record-key migration details live in [[unicode-safe-identity-keys]]. See [[persistence-layer]].
