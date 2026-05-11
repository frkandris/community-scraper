# Venue & Person Detail Pages — Design

## Goal

Give venues and persons their own public detail pages, and link to them from every place they currently appear.

## URLs

- `/{city_slug}/helyszin/{venue_slug}` — e.g. `/budapest/helyszin/mupa`
- `/{city_slug}/ember/{name_slug}` — e.g. `/budapest/ember/kovacs-janos`

Both are 3-segment routes; no conflict with the existing `/{city_slug}/{segment}` (2-segment) catch-all.

## Venue Detail Page

**Route:** `GET /{city_slug}/helyszin/{venue_slug}`

**Data shown:**
- Name, venue_type badge, description
- Address, contact, phone, email
- Website and social_links
- `welcomed_topics` as clickable chips → `/{city_slug}/{topic_slug}`
- Communities meeting here (see "Communities for Venue" below)
- Breadcrumb: city name → `/` + city_slug

**Communities for Venue query:**
Try `venue.community_ids` first (SHA-256 IDs stored on the venue record). If empty, fall back to searching `communities` table where `city = venue.city` AND `json_extract(data, '$.location')` LIKE `%venue_name%`.

## Person Detail Page

**Route:** `GET /{city_slug}/ember/{name_slug}`

**Merging:** `name_slug = _slugify(person.name)`. One page shows ALL `persons` rows in the same city where `_slugify(name) == name_slug`. This merges "Kovács János" appearing in 3 communities into one page.

**Data shown:**
- Name, city
- Each community they belong to (role badge + community name as link → community detail page)
- Bio (first non-null across merged records)
- Website, social_links (deduplicated)

## New DB Functions (db.py)

```python
def get_venue_by_city_slug(db_path, city, name_slug) -> dict | None
    # find venue in city where _slugify(json_extract(data,'$.name')) == name_slug

def get_persons_by_city_slug(db_path, city, name_slug) -> list[dict]
    # return all persons in city where _slugify(name) == name_slug

def get_communities_for_venue(db_path, venue_name, city) -> list[dict]
    # 1. collect community_ids from venues table for this venue_name+city
    # 2. SELECT communities WHERE community_id IN (...)
    # 3. if empty, fallback: WHERE city=city AND data LIKE %venue_name%
```

## New Routes (app.py)

```python
@_fastapi.get("/{city_slug}/helyszin/{venue_slug}")
async def public_venue_detail(request, city_slug, venue_slug): ...

@_fastapi.get("/{city_slug}/ember/{name_slug}")
async def public_person_detail(request, city_slug, name_slug): ...
```

Both return 404 redirect (→ `/helyszinek` or `/emberek`) if not found.

## New Templates

- `public_venue_detail.html` — extends `public_base.html`
- `public_person_detail.html` — extends `public_base.html`

Design language matches existing public pages (brand-gradient header, card grid).

## Linking Updates (no new routes, template edits only)

| Page | What changes |
|---|---|
| `public_venues.html` | Venue card `<h2>` wrapped in `<a href="/{city_slug}/helyszin/{venue_slug}">` |
| `public_explore.html` | Same for venue cards in `topic_venues` section |
| `public_community.html` | `r.leader` text becomes `<a href="/{city_slug}/ember/{slugify(leader)}">` |
| `public_people.html` | Replace placeholder with grouped person list; each name links to detail page |

## Slug Helper

Use existing `_slugify()` (already available in templates as a Jinja2 filter).

In Python routes, compare `_slugify(record_name) == requested_slug` to find the right record.

## Out of Scope

- Venue edit requests (separate feature)
- Person deduplication / merge UI
- Persons section on community detail pages (persons are only linked via `r.leader`)
