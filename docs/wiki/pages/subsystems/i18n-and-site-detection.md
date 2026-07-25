---
type: Subsystem
title: i18n and Site Detection
description: _detect_site reads the Host header; lang_context injects an i18n + nav bundle into every template. English is the translation base; missing keys render as themselves.
tags: [i18n, translations, host-detection, lang-context]
timestamp: 2026-07-09
resource: scraper/web/i18n.py
---

# i18n and Site Detection

*`_detect_site(request)` picks the domain from the `Host` header; `lang_context(request)` returns the per-request site/i18n bundle spread into every public template.*

## Site detection defaults to kozossegek

`_detect_site` returns `"meetapedia"` only if `"meetapedia"` is a substring of the host; **everything else — localhost, tests, unknown hosts — resolves to `"kozossegek"`** (Hungarian, `lang="hu"`). Local dev is the Hungarian site unless you send a `meetapedia` Host header.

## lang_context branches

- **kozossegek**: `lang` hard-coded to `"hu"` (ignores the cookie), Hungarian nav URLs, Hungary-centered map.
- **meetapedia**: `lang` = visitor cookie (any of ~50) else `en`, English nav URLs, world map.

`_site_cities(request)` scopes the city list: kozossegek → Hungary only; meetapedia → all. A non-HU city URL on kozossegek 302s home.

## Translation model

`make_t(lang)` merges `{**_T["en"], **_T[lang]}` — English is the base, per-language dict overrides on top. **Missing keys render as their own key string** (no exception); a `.format()` with a missing kwarg leaves the raw `{...}` (wrapped in try/except). Missing translations are visible-but-safe.

Topic labels are a **separate** system (`get_topic_labels(lang)`, same override-merge), and there are *also* English-only `TOPIC_LABELS`/`TOPIC_ICONS` dicts in `app.py` passed explicitly to some templates — a real source of inconsistency. The `**lang_context` spread comes last, so the i18n version wins where both are present.

## Localized URL slugs

`_topic_url_slug(topic, locale)` slugifies the localized label; `_topic_from_url_slug` reverses it and falls back to the English label, so both localized and English topic slugs resolve.

## Flags

`lang_context` computes `lang_dir` from an inline `("ar","he","fa","ur")` set instead of the module-level `RTL_LANGS` (which also has `ps, sd`) — so `ps`/`sd` render LTR despite being marked RTL. HU-specific topics (`hagyomanyorzes` → "Folk Traditions") are left untranslated in non-HU languages.

## Sister-site context

`lang_context` also injects `sister_url` (same path on the other host), `sister_name`,
`sister_key` (the *other* site's language: `hu` on meetapedia, `en` on kozossegek) and
the project identity constants `PROJECT_AUTHOR` / `PROJECT_AUTHOR_URL` /
`PROJECT_REPO_URL` used by the About page. City-scoped routes override `sister_url`
after the spread. See [[sister-site-cross-links]].
