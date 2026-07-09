---
type: Data-model
title: PersonRecord
description: Leaders/instructors extracted per community; enforces a two-word name rule and normalizes role to one of 12 values (default "leader").
tags: [models, person, pydantic, validation]
timestamp: 2026-07-09
resource: scraper/models.py
---

# PersonRecord

*Required: `name, role, city, topic, community_name`. `person_id` = SHA-256[:12] of `name|city|role|community_name`.*

# Schema

`role` is normalized: if not one of `PERSON_ROLES` (12 values — `leader, instructor, speaker, organizer, founder, coach, trainer, moderator, admin, member, volunteer, coordinator`) it is forced to `"leader"`. The `PERSON_SYSTEM_PROMPT` only documents 3 of these (leader/instructor/speaker); the other 9 are accepted-but-undocumented, so most non-standard roles collapse to `"leader"`.

## Two-word name rule

`if len(self.name.split()) < 2: raise ValueError` — a single-word "name" aborts record creation. `_parse_persons` catches this and logs `person_validation_failed`. Rationale: single-word names are usually role labels or noise, not real people.

## Extraction gating

Persons are only extracted for pages that yielded communities — see the person-skip optimization in [[extraction-layer]]. The `PERSON_USER_PROMPT_TEMPLATE` injects the known `community_names` as reference context. Extraction runs at `temperature 0.0`.

## History churn

`delete_leader_persons_for_community` deletes all `role='leader'` persons before re-inserting parsed ones; each cycle re-logs a `__created__` history row, which is why the activity timeline dedups via `MIN(changed_at)`. See [[history-created-sentinel-overcounting]].
