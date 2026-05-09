# Design Brief: kozossegek.com Public Site Redesign

## Problem

Someone new to a city — or just ready to break out of isolation — opens this site and
immediately feels overwhelmed. Stats, city grids, topic chips, and a sticky form all compete
for attention at once. Nothing says "start here." The community detail pages list facts in
uniform rows, so a warm, active running club reads the same as a dormant chess group with no
contact info. The site looks like a generic Tailwind template: functional, but forgettable.
People leave without trusting the data or knowing what to do next.

## Solution

A clean, editorial-feeling directory that guides visitors through a single question at a time:
*Where are you?* → *What are you into?* → *Here's your community.* The redesign gives the home
page a clear focal point, brings breathing room into every layout, and makes the community
detail page feel like a proper card you'd want to share — not a database dump. The visual
language shifts from cold slate/emerald to warm sand and terracotta, referencing the warmth of
community without being cutesy.

## Experience Principles

1. **One thing at a time over everything at once** — Each page has a single primary action.
   The home page is about choosing a city and interest. The explore page is about scanning and
   picking. The detail page is about deciding to show up.

2. **Earned trust over claimed trust** — Data quality, source transparency, and recency signals
   matter more than marketing copy. The About page and community detail page should surface
   these honestly.

3. **Warmth through restraint** — The warm palette and generous whitespace do the emotional
   work. No illustrations, no emoji-as-design, no gradients. Calm and considered.

## Aesthetic Direction

- **Philosophy**: Warm Editorial Minimalism — like a well-designed print almanac or Notion
  document. Clean grid, strong typographic hierarchy, muted warm background, ink-dark text,
  one terracotta/amber accent for calls-to-action.
- **Tone**: Welcoming, credible, unhurried. Not a startup landing page. Not a government form.
  Closer to a local zine that respects your time.
- **Reference points**: Notion.so (whitespace, typographic scale, subtle borders), Craft.do
  (warm paper feel, hierarchy), are.na (quiet confidence), Linear (sharp utility, no
  decoration for decoration's sake).
- **Anti-references**: Meetup.com (dense, pushy, promotional), Eventbrite (bright CTAs
  everywhere), early-2020s Tailwind starter kits (teal/indigo on gray-100).

## Existing Patterns

The codebase uses **raw Tailwind utility classes** — no CSS variables, no design tokens file,
no component abstraction layer. Everything lives inline in Jinja2 templates.

- **Typography**: Inter (Google Fonts). Weights 400/500/600/700/800 loaded. No type scale
  defined — sizes chosen ad-hoc per template.
- **Colors**: Tailwind's `slate` for neutrals, `emerald` for primary/accent, `white` for
  surfaces. No custom palette in tailwind.config.js.
- **Spacing**: Tailwind defaults (4px base). No custom scale.
- **Icons**: Phosphor Icons (web CDN, `ph` class prefix). Already present everywhere.
- **Components**: No reusable component files. Patterns repeated inline:
  - Pill/chip (topic chip, language badge)
  - Card with hover border highlight
  - Sticky header with logo + nav + language switcher
  - Detail row (icon + label + value)
  - Stat block (number + label)
  - Breadcrumb
  - Subscribe form
- **i18n**: Full — all user-visible strings go through `t('key')`. 50+ languages. RTL
  supported via `dir="{{ lang_dir }}"` on `<html>`. Any new strings must follow this pattern.
- **Fonts loaded in**: `public_base.html` (Inter via Google Fonts link). Admin base loads the
  same.

## Component Inventory

| Component | Status | Notes |
|---|---|---|
| `<html lang dir>` wrapper | Exists | RTL support already wired — preserve |
| Sticky header / nav | Modify | Logo mark + nav links need new palette; language switcher stays |
| Footer | Modify | Simplify; warm palette |
| Hero / home split layout | Modify | Keep two-panel; left = headline + stats + city grid; right = search form |
| Stat block (number + label) | Modify | New typographic weight, warm surface |
| City grid card | Modify | Warm hover state instead of emerald |
| Topic chip / filter pill | Modify | New palette; checked state with terracotta accent |
| Search form (city input + topic grid) | Modify | New focus ring color, warmer border |
| Community list card (explore grid) | Modify | Add subtle visual hierarchy within card |
| Community detail page | Redesign | New: prominent name + icon header, grouped detail sections, clear CTA zone |
| Detail row (icon + label + value) | Modify | Group into sections, not a flat list |
| Breadcrumb | Modify | Smaller, muted, warm |
| Subscribe form | Modify | Warm surface, terracotta submit button |
| Map page | Keep as-is | Leaflet integration stays; popup style gets warm palette |
| About page | Modify | Reorder for trust-building: mission first, stats second, browse last |
| Topic browser (About / Home) | Modify | Same chip component, new palette |
| Data quality notice | Modify | Replace amber warning with a calm informational tone |

## Key Interactions

**Home — city autocomplete**: As the user types a city, topic chip counts update via
`/api/city-topics`. The hint text ("N communities in City") should appear inline below the
input, not hidden. On mobile the form collapses to full-width first.

**Home — topic chip click without city**: Navigates directly to `/explore?topic=X`. With a
city typed: toggles the checkbox, user then hits Explore.

**Explore — topic filter chips (client-side)**: When no URL topic is set, clicking a chip
filters visible sections in JS without page reload. Active chip gets terracotta/amber
highlight. Visible count updates.

**Community detail — primary CTA**: The website/social links currently live at the bottom of
the card. In the redesign they move to a prominent "Get involved" zone near the top, below the
description. Secondary links (social, additional URLs) appear below.

**Language switcher**: `<details>` + `<summary>` with a scrollable dropdown. Keep the
mechanism; update styling to match warm palette.

## Responsive Behavior

- **Home**: On mobile, right-panel search form stacks below left content. City grid becomes
  2-column. Topic grid: 2 columns on mobile, 3 on sm, keeps scrollable `max-h` container.
- **Explore**: Card grid: 1 col mobile → 2 col md → stays 2 col (content-dense). Topic chips
  wrap freely.
- **Community detail**: Single-column always (max-w-3xl). Detail sections stack. CTA zone
  stays near top on all breakpoints.
- **About**: Stats go 1-col on mobile, 3-col on sm+.
- **Map**: Full-viewport height minus header. No layout change needed.
- **Header nav**: On mobile, nav links may need to scroll horizontally (`overflow-x-auto`
  already in place) — keep this behavior.

## Accessibility Requirements

- All interactive elements keyboard-navigable (tab + enter/space).
- Focus visible: custom focus ring using accent color, minimum 3:1 contrast on non-text,
  4.5:1 on text.
- Warm neutrals must maintain sufficient contrast — sand/stone backgrounds with ink-dark text
  (near-black) rather than mid-gray.
- `lang` and `dir` attributes on `<html>` — already implemented, must be preserved.
- All icon-only affordances need `aria-label` or adjacent visible text.
- The topic chip `<label>` wrapping `<input type="checkbox">` pattern is screen-reader
  friendly — keep this structure.
- Language switcher `<details>` is acceptable; ensure `<summary>` has descriptive text or
  aria-label in each language.

## Out of Scope

- Admin interface — no changes.
- Map page visual redesign — keep Leaflet popup inline styles as-is.
- New features (user accounts, saved communities, comments).
- Animations or page transitions.
- Dark mode.
- PWA / offline support.
- The scraper pipeline, AI classification, or data model.
- Changing the i18n key structure or adding new translation strings (existing keys only).
