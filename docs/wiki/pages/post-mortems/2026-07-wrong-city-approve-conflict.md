---
type: Post-mortem
title: Wrong-City Approve Conflict
description: Approving a wrong_city edit request failed with "community not found or unsupported change type" — apply_community_edit collapsed three distinct failures into one boolean, hiding that the record already existed under the correct city.
tags: [edit-requests, wrong-city, merge, error-handling]
timestamp: 2026-07-24
resource: scraper/db.py
---

# Wrong-City Approve Conflict

*Approving a user-submitted "wrong city" correction alerted a generic error and left the admin with no path forward; the likely underlying state — the community already indexed under its real city — is normal, not exceptional, and now resolves as a merge.*

## Symptom

2026-07-24, `/admin/edit-requests`: approving the pending wrong_city request for
"Nappali Idősek Klubja" (Szentendre → Szentgotthárd, submitted from the public
report form) popped `Error: community not found or unsupported change type`.

## Root cause

`db.py:apply_community_edit` returned a single `bool`, collapsing three distinct
failures into one message:

1. **record not found** under the key recomputed from the displayed
   (name, city, topic) identity — happens after merges/renames re-key the row;
2. **target-key conflict** — the corrected identity already exists because the
   scraper *also* found the community under its real city, so the
   `UPDATE … SET record_key=…` hits the UNIQUE constraint (`IntegrityError`)
   and the code bailed with `return False`;
3. **unsupported change type**.

Case 2 is the expected state for genuine wrong-city reports: if the club really
is in Szentgotthárd, a Szentgotthárd scrape has probably already indexed it.
Failing there turned the most legitimate reports into dead ends.

## Fix (same commit)

- `apply_community_edit` returns a status string (`"ok" | "merged" |
  "not_found" | "unsupported"`).
- The conflict branch now **merges instead of failing**: union `source_urls`
  into the existing target, force the target visible (`hidden=0`), hide the
  source row — all on the same connection (a second connection would deadlock
  inside the open transaction). Returns `"merged"`.
- The approve route falls back to a **unique name+city match** (via
  `normalized_match_key`) when the recomputed key finds nothing, then reports
  the precise failure and logs `edit_request_apply_failed` with the identity.

## Lessons

- A boolean return that fans in from multiple error branches guarantees an
  unactionable error message at the UI layer.
- For identity-correction features, "target already exists" is a merge
  scenario, not an error — the same insight that motivated
  [[wrong-city-detection]]'s move action reusing this exact code path.
