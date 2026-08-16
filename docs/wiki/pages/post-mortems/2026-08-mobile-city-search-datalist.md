---
type: Post-mortem
title: Mobile City Search Silently Refused to Run (2026-08-16)
description: The home search combined an iOS-invisible <datalist> with an exact-match submit guard, so phone users got no suggestions and no results.
tags: [incident, mobile, search, ux, datalist, ios]
timestamp: 2026-08-16
resource: scraper/web/templates/public_home.html
---

# Mobile City Search Silently Refused to Run (2026-08-16)

*Two independently reasonable choices — a native `datalist` and a strict submit guard — combined into a search box that could not be used on a phone.*

## Summary

Reported: "on kozossegek.com the home search does not work on mobile, it could
not find Szentendre." Szentendre was in `config/cities.yaml` and rendered into
the suggestion list, so the data was never the problem.

## Root cause

Two defects that only became fatal together:

1. **The suggestion list was invisible on iOS.** The city field used
   `<input list="cities-list">` plus a `<datalist>` of every site city. Safari
   on iOS does not implement `datalist` — the attribute is a no-op. iPhone users
   saw a plain text box with no hint of what it would accept.

2. **The submit handler demanded a byte-exact match.** On submit it ran
   `cityOptions.some(c => c.toLowerCase() === val.toLowerCase())` and called
   `preventDefault()` on any miss. Combined with mobile keyboards'
   autocapitalisation and autocorrect (the field set `autocomplete="off"` but
   never `autocapitalize`/`autocorrect`), a typed city name that differed by one
   character produced no navigation at all — the page just sat there. From the
   user's side the button was simply dead.

Guessing blind at an exact string is a UI that cannot succeed. The guard was
written for the desktop case, where the datalist made exact values easy to pick.

## Fix

`scraper/web/static/js/listing.js` — a shared, dependency-free widget layer:

- `MpText.norm()` folds NFD + strips combining marks, so `szentendré`,
  `Szentendre` and `SZENTENDRE` compare equal. See [[unicode-safe-identity-keys]]
  for the server-side equivalent of the same idea.
- `MpAutocomplete.attach()` renders its own touch-sized (`min-height: 44px`)
  suggestion panel, sets `autocapitalize`/`autocorrect`/`spellcheck` off, and
  picks on `pointerdown` so the choice survives the input's `blur`.
- `resolve()` accepts an exact normalised match, a *unique* prefix match, or a
  *unique* substring match. The submit handler now blocks only on a genuine
  no-match.

The form's hardcoded `action="/felfedezes"` also became `{{ explore_url }}` —
it was wrong on meetapedia.com, where the route is `/explore`.

Widget CSS lives in the `public_base.html` `<style>` block, not in
`input.css`: these classes only ever appear on JS-created nodes, which the
Tailwind CDN JIT never scans, and `app.css` is a gitignored build artifact.

## Lesson

A client-side validity guard must never be stricter than the affordance that
helps the user satisfy it. If the picker is unavailable on a platform, the guard
has to degrade with it — or the feature is dead on that platform, silently.

Regression tests in `tests/test_home_city_search.py` assert the absence of
`<datalist`, the presence of the shared script, and that the submit path calls
`resolve()` rather than an exact comparison.
