---
type: Data-model
title: Unicode-safe Identity Keys
description: Entity record keys hash NFKC+casefold canonical text, preventing non-Latin names from collapsing to the same database key.
tags: [identity, unicode, record-key, migration, slug]
timestamp: 2026-07-10
resource: scraper/identity.py
---

# Unicode-safe Identity Keys

*Community, venue, and person keys preserve identity across every writing system instead of deleting non-ASCII characters.*

## Record keys

`scraper.identity` canonicalizes every component with NFKC, Unicode `casefold()`, and collapsed whitespace, then hashes the joined components with SHA-256[:24]:

- community: `c2:<digest>` from `(name, city, topic)`
- venue: `v2:<digest>` from `(name, city)`
- person: `p2:<digest>` from `(name, city, role, community_name)`

The digest input retains punctuation and all scripts, so distinct Japanese, Cyrillic, Arabic, or symbol-only names cannot collapse to an empty component. `store.py`, `db.py`, duplicate detection, edits, and moderation all call the same helpers.

## Migration

`init_db()` records `unicode_record_keys_v2` in `schema_migrations`. Before marking it applied, it rewrites existing entity keys plus persisted references in `duplicate_candidates`, `edit_requests`, and `recategorize_suggestions` in one SQLite transaction.

## Public slugs

Latin text keeps familiar ASCII transliteration. If a name contains characters Python cannot transliterate, the slug receives a stable SHA-256 suffix (`東京` → `u-…`, `東京 Club` → `club-…`) so detail URLs remain non-empty and unique.

## Related

- [[community-identity]]
- [[fuzzy-dedup-and-record-key]]
- [[sqlite-schema]]
