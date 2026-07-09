---
type: Concept
title: joinable Quality Gate
description: The primary quality filter — only records the LLM marks joinable=True survive; a 3-condition AND rule defines it.
tags: [quality, joinable, extraction, filtering]
timestamp: 2026-07-09
resource: scraper/extract.py
---

# joinable Quality Gate

*The pipeline keeps only `joinable=True` records. This is the primary quality gate — non-joinable records are extracted then immediately dropped.*

The `SYSTEM_PROMPT` defines `joinable=true` as a 3-condition AND: the group (a) meets regularly/recurring, (b) is open to the general public (not invite/audition-only), and (c) has a group identity (not just a venue or gym). It is explicitly false for competitive ensembles, paid instruction courses, venues/facilities, and one-time or annual events.

**The default is `True`** at both the model layer (`joinable: bool = True`) and the parse layer (`item.get("joinable", True)`) — if the LLM omits the field, the record is kept. Contrast `confidence`, which defaults to `None`; enrichment additionally requires `confidence ≥ 0.7`.

Related: [[false-positive-injection]] (the other quality lever), [[community-record]].
