# Design Review: kozossegek.com Public Site Redesign

Reviewed against: `.design/public-site-redesign/DESIGN_BRIEF.md`
Philosophy: **Warm Editorial Minimalism** (Notion/Craft reference)
Date: 2026-05-09

## Screenshots Captured

> **Status: Not captured — server was not running during review.**
> Screenshots folder created at `.design/public-site-redesign/screenshots/`.
> Start the app and run `/design-review` again to capture live screenshots.
> Required: desktop-1280, tablet-768, mobile-375 for home, explore, community detail, about.

---

## Summary

The palette migration from emerald/slate to sand/terracotta is complete and consistent across all six public templates (`public_base`, `public_home`, `public_explore`, `public_community`, `public_about`, `public_map`). No residual `emerald-*` or `slate-*` Tailwind classes remain in any public template. The community detail page CTA zone has been restructured as specified — links now appear directly below the description rather than at the bottom. **However, the terracotta primary color (#C2613A) fails WCAG AA contrast against both white text (4.15:1, need 4.5:1) and sand-50 backgrounds as link text (3.97:1). This is a must-fix before ship.**

---

## Must Fix

### 1. CTA Button contrast fails WCAG AA
**File**: All templates using `bg-[#C2613A]` with white text.
- `bg-[#C2613A]` (terra-500) with `text-white`: **4.15:1** — fails 4.5:1 requirement for normal-sized text.
- Affected: Explore button (`public_home.html` line ~113), Subscribe button (`public_explore.html`), "Get involved" links, About CTA, all `btn-primary` usage.
- **Fix**: Replace button background `#C2613A` with `#A8512F` (terra-600) everywhere buttons carry white text. Contrast becomes **5.41:1** ✓. Update `app.css` `.btn-primary` and all inline occurrences.

```diff
- background-color: #C2613A;
+ background-color: #A8512F;
```

Also update in templates:
```diff
- bg-[#C2613A] text-white hover:bg-[#A8512F] active:bg-[#8A4226]
+ bg-[#A8512F] text-white hover:bg-[#8A4226] active:bg-[#6B341E]
```

### 2. Link text contrast fails WCAG AA
**File**: All templates using `text-[#C2613A]` as inline text color.
- terra-500 on sand-50 (`#FAFAF8`): **3.97:1** — fails 4.5:1 for body-size links/text.
- Affected: breadcrumb active segments, city count badges (small, decorative — acceptable), overline labels, nav active states, "View world map" links, "Change filters" links, all `text-[#C2613A]` occurrences used as text (not icon-only decorative).
- **Fix**: Replace `#C2613A` with `#A8512F` (terra-600) for all text links and active states. **5.18:1** ✓.
- Exception: icon-only decorative uses (map marker dot, bell icon) and count badges that have white/light backgrounds with bold text — these are UI components, require only 3:1 (so 3.97:1 passes for those).

### 3. Logo mark uses terra-500 background with white icon
**File**: `public_base.html` line 27.
- `bg-[#C2613A]` with white Phosphor icon: same as issue 1 — 4.15:1.
- **Fix**: `bg-[#A8512F]` for the logo mark.

---

## Should Fix

### 4. "Get involved" label is hardcoded English
**File**: `public_community.html` line 61.
```html
<p ...>Get involved</p>
```
The entire site uses `t('key')` for all user-visible strings. This hardcoded English breaks any non-English language visit.
- **Fix**: Add i18n key `community_get_involved` (value: "Get involved") to `i18n.py` translations, then use `{{ t('community_get_involved') }}`.

### 5. `public_source.html` and `public_unsubscribe.html` not updated
**Files**: `scraper/web/templates/public_source.html`, `public_unsubscribe.html`.
- These still use `emerald-*` and `slate-*` classes. Low-traffic pages but visually inconsistent if a user lands on them.
- **Fix**: Apply the same sand/terra palette swap. Scope is small — mostly border and text color changes.

### 6. `.label-overline` text (sand-500) contrast borderline
**File**: `app.css`, all templates using `.label-overline` or `text-[#8C8478]` on white/sand-50.
- sand-500 (#8C8478) on white: **3.69:1** — fails 4.5:1 for 11px text.
- Technically WCAG-exempt only if the text is "incidental" (decorative). Section labels like "When & where" convey meaning and are not incidental.
- **Fix**: Use `text-[#6A6259]` (sand-600, 5.99:1) for `.label-overline`. The visual impact is minimal — slightly darker overline text, still clearly recessive.

### 7. JS filterTopic toggles arbitrary hex classes
**File**: `public_explore.html` script block.
```js
c.classList.toggle('bg-[#FDF0EA]', isActive);
```
Tailwind CDN JIT scans HTML via MutationObserver, so dynamic class additions work — but there's a known race condition on first toggle where the style may not yet exist. If the first click flickers, it's this.
- **Fix**: Define `.chip-active` in `app.css` with the warm hover states, then toggle `.chip-active` instead of arbitrary value classes. This ensures the style exists before the first click.

### 8. Community detail — CTA links all rendered equally (no visual hierarchy)
**File**: `public_community.html` lines 60–90.
- All links (website, Facebook, Instagram, etc.) render as the same row format with equal weight. There's no visual distinction between the primary link (website) and secondary links.
- **Fix**: Give the first link (website) a slightly stronger treatment — `font-semibold` name already is, but the row itself could have a `bg-[#FAFAF8]` background to distinguish it, or a thin terracotta left border on the primary link row.

---

## Could Improve

### 9. City hint text layout shift
**File**: `public_home.html` — `#city-hint` paragraph with `min-h-[1rem]`.
- The `min-h-[1rem]` prevents layout shift but `1rem = 16px` may be tight — text is `text-xs = 11px`. If two lines wrap, it shifts.
- **Suggestion**: Use `min-h-[2.5rem]` or `min-h-[40px]` to hold up to two lines.

### 10. Explore worldwide view: "Near you" badge overuse
**File**: `public_explore.html` lines ~103–114.
- The "Near you" badge in yellow/terra on a page full of terra accents risks blending in.
- **Suggestion**: Try `font-semibold` on the country heading instead of a badge, or use a map-pin icon to signal proximity.

### 11. Community detail — founding year lonely section
**File**: `public_community.html` lines ~174–181.
- Founding year gets its own section with a border-t, which looks oversized for a single data point.
- **Suggestion**: Move founding year into the "Who" section as a final row, removing its standalone section wrapper.

### 12. Topic chip icon doesn't track peer-checked state
**File**: `public_home.html` topic grid.
- The icon `text-[#8C8478]` inside the chip doesn't change color when checked — only the border/bg updates.
- **Suggestion**: Add `peer-checked:text-[#C2613A]` on the icon element (or use the CSS `peer-checked` variant with a child combinator in app.css).

### 13. `public_about.html` — "About" overline not using `.label-overline`
**File**: `public_about.html` line 6.
- Uses inline `text-xs font-semibold uppercase tracking-[0.08em] text-[#C2613A]` — same as the terra overline in home. Consistent but not using the shared class.
- **Suggestion**: Extract as `.label-overline-accent` variant in `app.css` for terra-colored overlines, to keep visual consistency declarative.

---

## What Works Well

- **Complete palette migration**: Zero emerald/slate residue in the six core public templates. The switch from `slate-*` to sand hex values is consistent and intentional — no misses.
- **Community detail restructure**: The CTA zone elevation (from bottom of card to just below description) is exactly what the brief called for and is the most impactful UX improvement in the redesign.
- **Detail section grouping**: "When & where / Who / Joining" replaces the flat 10-row list. Visual scanability is substantially improved.
- **Warm shadows**: Using `rgb(28 25 23 / 0.08)` instead of cool-gray shadows is a subtle but effective detail — the depth reads as part of the same warm world.
- **Token system**: The CSS custom properties in `app.css` are well-structured. When a build tool is added, `input.css` with `@tailwind` directives is ready to go.
- **RTL preservation**: All `dir="{{ lang_dir }}"` and `lang="{{ lang }}"` attributes maintained throughout — no regressions on the 50+ language support.
- **i18n pattern preserved**: Every user-visible string except "Get involved" goes through `t()` — one slip in an otherwise clean migration.
