---
type: Hack
title: PyYAML Parses "no" (Norway) as Boolean False
description: The Norwegian locale code "no" is read by PyYAML as False; string casts guard it, and Google search remaps hl "no" → "nb".
tags: [yaml, locale, norway, gotcha, config]
timestamp: 2026-07-09
resource: scraper/search.py
---

# PyYAML Parses "no" (Norway) as Boolean False

*YAML 1.1 treats the bareword `no` as boolean `False`. The Norwegian locale `"no"` in `cities.yaml` therefore loads as `False` unless cast.*

Defensive `str(locale)` / `str(c["locale"])` casts exist specifically for this (with the comment "guard against PyYAML parsing 'no' as bool False") in both `search.py` and `config.py`. Without the cast, the Norway locale becomes `False` and breaks every string operation on it.

(A related trap lived in the now-removed Google Playwright client, which had to remap `hl "no"` → `"nb"` for Google's interface-language code.) See [[search-layer]].
