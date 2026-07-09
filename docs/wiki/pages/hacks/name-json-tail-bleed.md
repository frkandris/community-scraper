---
type: Hack
title: Stripping JSON Tail-Bleed From the name Field
description: The LLM sometimes appends following JSON fields into the name string; _LEAKED_JSON_RE strips the leaked tail.
tags: [models, llm, cleanup, regex]
timestamp: 2026-07-09
resource: scraper/models.py
---

# Stripping JSON Tail-Bleed From the name Field

*`_LEAKED_JSON_RE` matches a leaked JSON tail on `name` — e.g. `Choir", 0.9, true, …` — and strips it back to `Choir`.*

The LLM occasionally bleeds subsequent JSON fields (the closing quote, the next numeric/boolean values) into the `name` string. The regex matches a closing smart/plain quote followed by a numeric/`true`/`false` token and everything after, then `.strip(' ",')`. This runs in `CommunityRecord._clean_and_generate_id` before `community_id` is derived, so the id is computed from the cleaned name. A hard-won fix for a recurring LLM formatting glitch. See [[community-record]].
