# Design: Reject single-word person names

**Date:** 2026-05-09

## Problem

`PersonRecord` accepts names like "Ági" or "Léna" (single first names). These are almost always extraction noise — a real person entry needs at minimum a first and last name to be useful and non-ambiguous.

## Solution

Add a model validator in `PersonRecord._clean_and_generate_id()` that raises `ValueError` if the name contains fewer than two whitespace-separated words. This is consistent with the existing email (`@` required) and phone (digit required) validations.

```python
if len(self.name.split()) < 2:
    raise ValueError(f"Person name is a single word, skipping: {self.name!r}")
```

## Affected files

| File | Change |
|------|--------|
| `scraper/models.py` | Add 3-line validator in `PersonRecord._clean_and_generate_id()` |
| `scraper/pipeline.py` | Wrap `PersonRecord(...)` in `_persons_from_leaders()` with `try/except` to skip invalid records |

## Why `extract.py` needs no change

All `PersonRecord(...)` calls in `extract.py` are already inside `except Exception` blocks (e.g. lines 347–360). The new `ValueError` is silently absorbed there with a debug log.

## Edge cases

- "Dr. Kiss" — two tokens, passes (acceptable)
- "Ági Tóth" — two tokens, passes
- "Ági" — one token, rejected
- Multi-word role descriptions from the leader field (e.g. "karmester") are already parsed out by `_parse_leader_field` before name creation, so they won't accidentally pad the word count
