---
type: Runbook
title: Adding a City or Topic
description: The config files plus the app.py dicts and i18n labels you must update in lockstep.
tags: [operations, cities, topics, config, i18n]
timestamp: 2026-08-16
resource: config/cities.yaml
---

# Adding a City or Topic

*Config, routing metadata, coordinates, icons, and translations must move together.*

## New city

For a **whole country** rather than one city, use the importer instead of hand
editing — see [[importing-city-lists]]. It handles slug collisions, topic
tiering and coordinates, and it never rewrites existing entries.

1. Add to `config/cities.yaml`: `name`, `country`, `locale`, `search_variants` (accent-stripped alternates so ASCII search engines match, e.g. `[Győr, Gyor]`).
2. Add coordinates to `CITY_COORDS` in `app.py` for the map page.
3. `country == "Hungary"` puts it in the HU pass and on kozossegek; `country == "Sweden"` puts it in the Sweden pass; anything else lands in the international pass and only on meetapedia. See [[hungary-sweden-intl-three-passes]] and [[i18n-and-site-detection]].

## New topic

1. Add to `config/topics.yaml`: `name` (slug) + `search_terms` per locale (falls back to `en` if the city's locale is missing).
2. Add to `TOPIC_ICONS` and `TOPIC_LABELS` in `app.py`.
3. Add a label in `get_topic_labels()` in `i18n.py` for each supported language (English required, Hungarian primary; others fall back to English).

## Notes

- `locale` drives which `search_terms` set is used and the localized URL slug ([[i18n-and-site-detection]]).
- Watch for `topics.yaml` search-term typos — they become literal (broken) search queries.
- A new topic/city won't be searched until a run covers its pair; the done-pair pre-filter only skips already-covered pairs.
- A new **locale** needs three things or it degrades silently: `search_terms` in `topics.yaml` (otherwise English terms are used), an entry in `LOCALE_TO_DATAFORSEO_LOCATION` (otherwise `task_post` rejects the task — [[2026-07-search-provider-down-noise]]), and translations in `i18n.py`.
- Any page that lists the new entities should use the shared filter macro so it gets the same A-Z + search UX — see [[public-listing-widgets]].
