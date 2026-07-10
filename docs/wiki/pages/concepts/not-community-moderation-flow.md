---
type: Concept
title: Not-community Moderation Flow
description: Public reports stay pending and cannot hide records; only admin approval hides the community and creates a false-positive example.
tags: [moderation, visibility, false-positive, security]
timestamp: 2026-07-10
resource: scraper/web/app.py
---

# Not-community Moderation Flow

*A public report is evidence for admin review, not authorization to change public visibility.*

## State transitions

1. `POST /report-not-community` inserts a pending `not_community_reports` row.
2. Pending reports leave `communities.hidden` unchanged.
3. Admin **approve** adds the item to `false_positives`, hides the matching `record_key`, then removes the report.
4. Admin **dismiss** only removes the report, so a visible community stays visible.

## Why this is load-bearing

Before 2026-07-10, `init_db()` repeatedly back-filled `hidden=1` from every pending report. Any unauthenticated report therefore hid a community as soon as another request called `init_db()`, and dismissing the report did not restore it. Visibility changes now happen only in the authenticated approval route.

## Related

- [[false-positive-injection]]
- [[sqlite-schema]]
- [[community-record]]
