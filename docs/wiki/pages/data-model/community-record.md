---
type: Data-model
title: CommunityRecord
description: The core entity — a pydantic model with aggressive multilingual auto-cleanup and a stable derived community_id.
tags: [models, community, pydantic, validation, cleanup]
timestamp: 2026-07-09
resource: scraper/models.py
---

# CommunityRecord

*The core entity. Required: `name, topic, city, locale, source_url, extracted_at`. `community_id` = SHA-256[:12] of `f"{name.lower()}|{city.lower()}"` (stable across re-runs). See [[community-identity]].*

# Schema

Beyond the required fields: `joinable: bool = True`, `confidence: float | None`, `source_urls: list`, plus an extended profile — `description, meeting_schedule, location, website, social_links, contact, email, phone, founding_year, member_count, fee, age_range, skill_level, join_process, leader, tags, language, history, frequency`.

## Two-stage validation

**`_coerce_str` (mode="before")** converts LLM mistakes on every string field: a dict → `json.dumps`, a list → comma-join. This absorbs the model occasionally returning structured values where a string is expected.

**`_clean_and_generate_id` (mode="after")** does the heavy cleanup:

- **`_NULL_STRINGS` nulling** — a multilingual frozenset of placeholders (`n/a, unknown, none, not provided, -, –, na` + Hungarian `nincs megadva, nem ismert, ismeretlen` + German `keine angabe, unbekannt`) is nulled case-insensitively on all optional text fields. The LLM is told to answer in the page's language, so it returns localized "not specified" strings that must become real `None`.
- **`phone` nulled if it has no digit** — catches `"Nincs megadva"` being rendered as `<a href="tel:Nincs megadva">`.
- **`email` nulled if it has no `@`**.
- **`website`** gets `https://` prepended if it has no scheme; **`social_links`** filtered to only `http(s)://` URLs.
- **`tags`** stripped, order-preserving deduped (`dict.fromkeys`), capped at **8** — note the prompt asks for "1–5 tags" but the cap is 8.
- **`source_url`** always prepended into `source_urls`.

## `_LEAKED_JSON_RE` — name tail-bleed fix

A regex strips a leaked JSON tail off `name` (e.g. `Choir", 0.9, true, …` → `Choir`). Hard-won fix for an LLM glitch where subsequent JSON fields bleed into the name string. See [[name-json-tail-bleed]].

## Quality gate

Records with `joinable=False` are extracted but dropped by the pipeline. The default is `True` at both model and parse layers, so an omitted `joinable` keeps the record. See [[joinable-quality-gate]]. Related entities: [[person-record]], [[venue-record]].
