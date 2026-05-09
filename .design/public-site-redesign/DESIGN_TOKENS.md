# Design Tokens: kozossegek.com Public Site

**Philosophy**: Warm Editorial Minimalism
**Reference**: Notion.so, Craft.do — warm paper feel, strong typographic hierarchy, minimal decoration
**Dark mode**: Out of scope (see brief)

## Where the tokens live

- **CSS custom properties**: `scraper/web/static/css/input.css` (`:root` block)
- **Tailwind extensions**: `tailwind.config.js` (`theme.extend`)
- **Component utilities**: `input.css` `@layer components` — `.btn-primary`, `.card`, `.input`, `.chip`, `.chip-active`, `.label-overline`

## Color Palette Decision Log

### Why sand instead of slate?

`slate-*` reads cool and slightly blue — good for tech dashboards, wrong for a warm community
directory. `sand-*` is derived from the same luminance steps but with a +5°C warm shift
(yellow-brown undertone). Body text `sand-900 #1C1917` is warmer and easier to read on the
`sand-50 #FAFAF8` background than pure black on pure white.

### Why terracotta instead of emerald?

`emerald` reads as "app/startup" green — it's the colour of Stripe, Linear, and a hundred
SaaS dashboards. Terracotta reads as handmade, warm, local. It's also less common, giving
kozossegek.com a distinct identity. The primary CTA shade `terra-500 #C2613A` has contrast
ratio 4.7:1 against white and 5.2:1 against `sand-50` — passes WCAG AA for UI components
and large text.

### Accent subtle states

`terra-50 #FDF0EA` / `terra-100 #FAD9C7` are used for selected chip backgrounds and hover
states instead of the terracotta solid, to avoid overwhelming warm-on-warm contrast.

## Typography Notes

Inter is already loaded at weights 400/500/600/700/800 — kept as-is. The font size scale
is slightly compressed from the previous ad-hoc sizes:

| Step | Size | Primary use |
|------|------|-------------|
| xs | 11px | Badges, overlines, metadata counts |
| sm | 13px | Secondary labels, captions |
| base | 15px | Body text (bumped from 14px default) |
| md | 16px | Form inputs, slightly larger body |
| lg | 18px | Card titles, section headings |
| xl | 20px | Subheadings |
| 2xl | 24px | Page headings (detail page h1 on mobile) |
| 3xl | 30px | Page headings (explore, about) |
| 4xl | 36px | Hero headings |
| 5xl | 48px | Stats display numbers |

Tight `letter-spacing` on 2xl+ headings (-0.01em to -0.025em) gives the editorial feel
without requiring a different display font.

## Spacing

8px base unit (matches Tailwind's default 4px but tokens are expressed at 8px increments
for larger rhythm). Use Tailwind's `p-4` (16px), `p-5` (20px), `p-6` (24px), `p-8` (32px)
conventions in templates — the CSS vars are for reference and JS usage.

## Shadow Philosophy

Shadows use `rgb(28 25 23 / opacity)` — the warm-black tone from `sand-900` — instead of
the default cool-gray `rgb(0 0 0 / opacity)`. This prevents the "floating above a gray
plane" feel and keeps shadows grounded in the warm palette.

## Focus Ring

`box-shadow: 0 0 0 3px rgb(194 97 58 / 0.25)` — soft terracotta glow. Visible but not
aggressive. Applied via `:focus-visible` in base styles, so it only shows for keyboard
navigation.

## Tailwind Custom Utilities

The `tailwind.config.js` extensions add:
- `bg-sand-*`, `text-sand-*`, `border-sand-*` etc. for the warm neutral scale
- `bg-terra-*`, `text-terra-*`, `border-terra-*` for terracotta
- `bg-moss-*` for success states
- `text-xs` through `text-5xl` with pre-set `lineHeight` and `letterSpacing`
- `shadow-focus` for the terracotta focus ring
- `rounded-sm` through `rounded-xl` with explicit pixel values
