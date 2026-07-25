---
type: SEO
title: Sister-Site Cross Links
description: Wikipedia-style "also available in the other edition" links between kozossegek.com and meetapedia.com — same path, no redirect hop, suppressed where the twin would 302 home.
tags: [two-domain, i18n, internal-linking, sister-site, about-page]
timestamp: 2026-07-25
resource: scraper/web/i18n.py
---

# Sister-Site Cross Links

*The two domains are editions of one project, and now say so: a strip above the content
links the same page on the other site, the footer links the other site's home page, and
the About page explains the relationship.*

## Why same-path works

Both hosts are served by the same FastAPI app ([[two-domain-single-container]]) and no
route is host-scoped, so the twin of any public URL is the **identical path on the other
host**. `i18n.sister_url(request)` is therefore string concatenation — no path table to
maintain, no redirect hop, and query strings survive (a filtered explore page's twin is
the same filtered page).

## When there is no twin

meetapedia.com carries every city; kozossegek.com carries Hungarian ones and redirects
everything else to its home page (`public_city`). Linking there would drop the visitor
somewhere unrelated, so `app.py:_sister_url(request, city_name)` returns `None` for
non-Hungarian city content on meetapedia, and the strip is not rendered. The reverse
direction always has a twin — meetapedia is the superset.

City-scoped routes pass `sister_url` **after** `**lang_context(request)`, which would
otherwise overwrite the city-aware value with the unconditional default.

## Wording

Two i18n keys, not one language-dependent string: meetapedia.com can itself be read in
Hungarian, so a notice keyed on the *viewer's* language would tell a Hungarian-speaking
meetapedia reader that the page "is also available in Hungarian". `sister_key` (`hu` on
meetapedia, `en` on kozossegek) names the *other site's* language and picks
`sister_notice_hu` / `sister_notice_en`.

## Relationship to canonicals

These are visible navigation links, not indexing signals — the cross-domain canonical
policy is unchanged ([[seo-cross-domain-canonical]]): HU-city pages on meetapedia.com
still canonicalize to kozossegek.com. No `hreflang` pairs were added; a canonical and an
alternate pointing different directions on the same duplicate pair is a contradictory
signal, and the 2026-06 deindexing ([[2026-06-seo-traffic-collapse]]) is recent enough to
warrant leaving the indexing side alone.

## Where it renders

- **Strip above `<main>`** (`public_base.html`) — page-level twin, `sister_url`.
- **Footer** — always the sister *home page*, even where the page-level twin is
  suppressed.
- **About page** — the project relationship in prose, plus buttons to the sister site,
  the GitHub repo, and the author ([[web-app]]).
