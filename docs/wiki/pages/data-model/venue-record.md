---
type: Data-model
title: VenueRecord
description: Physical locations that host communities; spans topics via welcomed_topics rather than a topic column.
tags: [models, venue, pydantic]
timestamp: 2026-07-09
resource: scraper/models.py
---

# VenueRecord

*Required: `name, city, locale, source_url, extracted_at`. `venue_id` = SHA-256[:12] of `name|city`.*

# Schema

`venue_type` is a free string — the prompt constrains it to an enum (`café|bar|park|cultural_center|library|church|sports_hall|studio|coworking|restaurant|other`) but the model does **not** validate against it. `welcomed_topics: list[str]` holds English topic slugs (the venue user template injects the valid slug list as a hint). Same website/social/email/phone cleanup as [[community-record]], but `VenueRecord._NULL_STRINGS` is a subset (no German entries).

The `venues` table has **no topic column** — a venue spans topics via `welcomed_topics` JSON. `_parse_venues` never populates the model's `contact`/`community_ids` fields even though they exist. Extraction runs at `temperature 0.0`.
