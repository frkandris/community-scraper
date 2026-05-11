# Listing Pages Filters — Design Spec

**Goal:** Add filtering and name search to `/helyszinek` and `/emberek` listing pages.

---

## `/emberek` changes

### Server-side filters

Add `city: str = ""` and `role: str = ""` query params to the `public_people` route. After deduplication, filter `unique` persons by both params (case-insensitive for city, exact match for role). Build `all_cities` and `all_roles` from the full deduplicated set (not the filtered subset) so all options are always visible.

Pass to template: `selected_city`, `selected_role`, `all_cities`, `all_roles`.

### Role filter dropdown

`all_roles` contains raw English role values (e.g. `"leader"`, `"organizer"`). The `<option>` value is the raw English key; the display label goes through the `role_hu` Jinja2 filter.

### JS name search

A text input above the person grid. On each `input` event, iterate all `[data-name]` elements and toggle `hidden` class based on whether the person's name contains the query (case-insensitive). After each filter pass, iterate city group `<section>` elements: if all their `[data-name]` children are hidden, hide the section too; otherwise show it.

### `role_hu` fix

Apply `| role_hu` to `p.role` in `public_people.html` (was missed in the earlier translation fix).

---

## `/helyszinek` changes

### JS name search

Same pattern as `/emberek`: text input, `data-name` on venue cards, hide/show cards and their city section headers based on the search query.

No server-side changes needed — city and topic filters already exist.

---

## Architecture notes

- No new DB functions or API endpoints.
- Filter form on `/emberek` follows the identical HTML/CSS pattern as `/helyszinek` (label + select pairs, `onchange="this.form.submit()"`, "Szűrők törlése" link).
- JS name search is identical on both pages — small inline `<script>` block per template, no shared file needed.
- `data-name` attribute on each card holds `{{ p.name | lower }}` / `{{ v.name | lower }}` so JS comparison is a simple `includes()`.
- City section headers are wrapped in a `<section data-city-section>` element so the JS can find and hide them.

---

## Out of scope

- Pagination (both pages render all items; count is manageable).
- Topic/role filter on `/helyszinek` (already has topic filter; role doesn't apply to venues).
- Search synced to URL (JS-only, no history pushState).
