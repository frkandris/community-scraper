# Build Tasks: kozossegek.com Public Site Redesign

Generated from: .design/public-site-redesign/DESIGN_BRIEF.md
Date: 2026-05-09

Philosophy: **Warm Editorial Minimalism** — sand + terracotta palette, Notion/Craft feel.
All work is in `scraper/web/templates/` (Jinja2). No component abstraction layer exists;
"component" means a repeated HTML+Tailwind pattern within templates.

---

## Foundation

- [x] **Design tokens**: CSS custom properties + Tailwind config extensions. _Done: `input.css`, `tailwind.config.js`, `DESIGN_TOKENS.md`._

- [x] **Shared shell — `public_base.html`**: Replace slate/emerald palette with sand/terra throughout the sticky header and footer. Update logo mark background from `bg-emerald-600` to `bg-terra-500`. Nav link active state: `bg-sand-100 text-sand-900` (was `bg-slate-100`). Language switcher dropdown: warm border (`border-sand-200`), hover `bg-sand-50`. Footer: `bg-white border-sand-200`. Body background: `bg-sand-50` (was `bg-slate-50`). _Modifies: `public_base.html`._ This must land first — every page inherits from it and the palette shift will be immediately visible.

---

## Core UI

- [x] **Home — headline + stats strip**: Update the left-panel headline section. Overline label (`Community index`): use `.label-overline` utility class. `h1`: `text-3xl sm:text-4xl font-bold tracking-tight text-sand-900`. Body text: `text-sand-600`. Stats strip: each stat block `bg-white border border-sand-200 rounded-md` — number in `text-sand-900 font-extrabold`, label in `text-sand-500 text-xs`. _Modifies: `public_home.html` hero section._

- [x] **Home — popular cities grid**: City cards: `bg-white border border-sand-200 rounded-md hover:border-terra-400 hover:bg-terra-50` (was emerald). Badge: `text-terra-600 bg-terra-50 border border-terra-100`. Country overline: `.label-overline`. "View world map" link: `text-terra-600 hover:text-terra-800`. _Modifies: `public_home.html` cities section._

- [x] **Home — search form (right panel)**: Panel: `bg-white border border-sand-200 rounded-lg p-5 sm:p-6`. City input: `.input` component class + `pl-10` icon. Input focus: `border-terra-500 ring-2 ring-terra-500/20`. Topic grid chips: `.chip` / `.chip-active` component classes with `peer-checked` state wired up. Count badge: `text-terra-600 font-bold`. Explore button: `.btn-primary w-full py-3 text-base rounded-md`. Clear-all link: `text-terra-600`. City hint text: `text-sand-500 text-sm`. _Modifies: `public_home.html` form section._

- [x] **Explore — breadcrumb + sub-bar**: Sticky sub-bar `bg-white/95 border-b border-sand-200 backdrop-blur`. Breadcrumb links: `text-sand-400 hover:text-sand-800`. Active segment: `text-sand-900 font-semibold`. Topic segments: `text-terra-700`. "Change filters" button: `border border-terra-200 text-terra-700 hover:text-terra-900 rounded-md`. _Modifies: `public_explore.html` top bar section._

- [x] **Explore — topic filter chips**: Both variants (form-submit and JS-filter). Unselected: `.chip`. Selected: `.chip-active`. Count badge: `text-terra-600 font-bold`. Hover on unselected: terra-tinted. _Modifies: `public_explore.html` filter chip section._

- [x] **Explore — community card (primary)**: The most-used content unit. Card: `bg-white rounded-lg border border-sand-200 p-5 hover:border-terra-300 hover:bg-terra-50/40 transition-colors`. Name: `font-semibold text-sand-900 text-base`. Description: `text-sm text-sand-600 line-clamp-2`. Detail rows (location, schedule, contact): `text-sm text-sand-600 flex items-center gap-2`, icon `text-sand-400`. Meta chips (fee, members, skill level, year): update color classes to warm equivalents — `bg-moss-50 text-moss-700` for free, `bg-sand-50 text-sand-600` for neutral. Link buttons (website, social): update to `text-terra-700 hover:bg-terra-50` style. _Modifies: `public_explore.html` community card loop._

- [x] **Explore — worldwide grouped view**: Country section headings: `text-lg font-bold text-sand-900`. "Near you" badge: `bg-terra-50 text-terra-700 border border-terra-200`. City sub-headings: `text-sand-700 font-semibold`. Compact community cards (used in worldwide grid): same border/hover as primary card but `p-3`, `text-xs` name, `text-[11px]` description. "See all N" link: `text-terra-700`. _Modifies: `public_explore.html` country_sections loop._

- [x] **Explore — subscribe form**: Section `bg-white rounded-lg border border-sand-200 p-6 sm:p-8`. Heading: `text-lg font-bold text-sand-900` + bell icon `text-terra-600`. Body copy: `text-sm text-sand-500`. Email input: `.input flex-1`. Submit button: `.btn-primary px-6 py-2.5`. Fine print: `text-xs text-sand-400`. _Modifies: `public_explore.html` subscribe section._

- [x] **Explore — empty state + subscribed banner**: Empty state: icon `text-sand-300`, heading `text-sand-800`, softer than current. Subscribed success banner: `bg-terra-50 border border-terra-200 rounded-md` with check icon `text-terra-600`, heading `text-terra-900`, body `text-terra-700`. _Modifies: `public_explore.html` empty + subscribed blocks._

- [x] **Community detail — header zone**: Breadcrumb: `text-sm text-sand-400`, active `text-sand-700 font-medium`. Icon avatar: `bg-terra-50 border border-terra-100` with topic icon `text-terra-600`. Name: `text-2xl font-bold text-sand-900`. City badge: `bg-sand-100 text-sand-600 border border-sand-200`. Topic badge: `bg-terra-50 text-terra-700 border border-terra-200`. Description: `text-sand-700 leading-relaxed`. _Modifies: `public_community.html` header card, top section._

- [x] **Community detail — "Get involved" CTA zone**: Move website + primary social link from bottom of card to a dedicated zone directly below the description, inside the header card. Label: `.label-overline` ("Get involved" / i18n key). Primary link: `btn-primary`-styled `<a>` (website or first social). Secondary links below in a row, smaller. Add a visual divider (`border-t border-sand-100`) before detail rows. _Modifies: `public_community.html` — restructures link placement._

- [x] **Community detail — detail sections**: Group flat detail rows into 2–3 logical sections with `.label-overline` section headers: "When & where" (location, schedule, frequency), "Who" (leader, members, skill level, age range, language), "Joining" (fee, join process, contact, email, phone). Icon avatar: `bg-sand-50 border border-sand-100`, icon `text-sand-500`. Label: `text-xs text-sand-400 uppercase tracking-widest`. Value: `text-sand-700`. _Modifies: `public_community.html` detail_row section._

- [x] **About — trust-first redesign**: Reorder sections to: mission → how it works → stats → browse by interest → popular cities → data quality notice. Mission `h1`: `text-3xl font-bold text-sand-900`. How it works `h3`: `text-base font-semibold text-sand-800`. Stat cards: `text-4xl font-extrabold text-sand-900`, label `text-sand-500`. Data quality notice: replace amber warning panel with a calm `bg-sand-100 border border-sand-200` info panel — icon `text-sand-500`, text `text-sand-700`. CTA button: `.btn-primary`. All emerald → terra throughout. _Modifies: `public_about.html`._

- [x] **Map — popup style**: Update inline styles in `public_map.html` `<style>` block. `.city-popup-name`: `color: #1C1917`. `.city-popup-count`: `color: #C2613A`. `.city-popup-btn`: `background: #C2613A`, hover `#A8512F`. Circle markers: `fillColor: '#C2613A'`, `color: '#8A4226'`. _Modifies: `public_map.html` — inline style block and Leaflet config only._

---

## Interactions & States

- [x] **City autocomplete count hint**: The `#city-hint` paragraph currently `.hidden` toggled by JS. Make it visible by default with a min-height so the layout doesn't jump when counts appear. Ensure text uses `text-sand-500` (not slate). _Modifies: `public_home.html` JS + hint element styling._

- [x] **Topic chip JS filter (explore page)**: Update `filterTopic()` in `public_explore.html` to toggle `.chip-active` class instead of ad-hoc inline class manipulation. Visible count update: `text-sand-500` style. _Modifies: `public_explore.html` `<script>` block + chip class names._

- [x] **Nav hover + active states — all pages**: Confirm `public_base.html` nav links use the new sand active/hover classes. Test each page's active condition visually (Dashboard/Discover, Map, About). Language switcher hover: `bg-sand-100`. _Verifies: `public_base.html` nav section._

---

## Responsive & Polish

- [x] **Mobile layout pass — Home**: On mobile (`< lg`), right-panel form stacks below hero content, full width. City grid: 2-col. Topic grid: 2-col with `max-h` scroll preserved. Stats strip: `grid-cols-3` on mobile (hide cities+topics columns on xs, already `hidden sm:block`). Confirm form is not clipped on narrow viewports. _Breakpoints: default (mobile), sm, lg._

- [x] **Mobile layout pass — Explore**: Card grid 1-col on mobile → 2-col on md. Topic chips wrap freely. Sub-bar breadcrumb truncates gracefully on narrow screens. Worldwide grouped view: compact cards 2-col on sm, 3-col on md. _Breakpoints: default, sm, md._

- [x] **Mobile layout pass — Community detail**: Single-column always (already `max-w-3xl`). CTA zone buttons stack on very narrow viewports. Detail section grid: always 1-col. Breadcrumb truncates with `truncate` on community name. _Breakpoints: default, sm._

- [ ] **Accessibility pass**: (a) All interactive elements reach via Tab. (b) Focus ring visible on all inputs, buttons, links, chips — using `shadow-focus` token. (c) Contrast check: `text-sand-400` on `bg-sand-50` (tertiary text) — must be ≥ 3:1 for non-text; upgrade to `text-sand-500` if failing. (d) `aria-label` on icon-only buttons (language switcher `<summary>`, topic chip icon-only affordances). (e) RTL: re-check `public_base.html` with `dir="rtl"` — icon margins and flex directions. (f) Topic chip `<label>` wrapping `<input type="checkbox" class="sr-only">` pattern preserved throughout.

---

## Review

- [ ] **Design review**: Run `/design-review` against the brief. Check: visual hierarchy on all 5 pages, consistency of sand/terra palette, focus states, mobile layouts, community detail CTA prominence, About page trust flow.
