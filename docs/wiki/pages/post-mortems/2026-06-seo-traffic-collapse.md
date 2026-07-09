---
type: Post-mortem
title: Organic Traffic Collapse (2026-06)
description: kozossegek.com organic clicks fell from ~95/day to ~0 around 2026-06-01 as ~20K pages were devalued to "Crawled - currently not indexed."
tags: [seo, incident, deindexing, canonical, google]
timestamp: 2026-07-09
resource: scraper/web/app.py
---

# Organic Traffic Collapse (2026-06)

**Date:** ~2026-06-01 to 06-03 (diagnosed 2026-07-09).
**Symptom:** kozossegek.com organic search clicks ~95/day → ~0, impressions also → ~0; GA4 confirmed. Not recovered as of 2026-07-09.

## Diagnosis (Google Search Console)

- No manual action, no technical block: crawl OK, indexing allowed, sitemap read successfully (27,113 URLs).
- **~20,600 pages moved to "Crawled – currently not indexed"** (jumps ~06-03 and ~06-16); indexed pages fell from ~20K to ~4.9K. A pure algorithmic quality devaluation of the programmatic city/topic/community pages — the classic post-honeymoon reassessment of scaled/programmatic content.
- **Cross-domain duplication:** meetapedia.com served the same HU pages (identical paths, self-canonical, no hreflang). meetapedia's indexed count **grew** to ~19.7K in the same window while kozossegek's fell — Google consolidating toward meetapedia, which earns ~no HU traffic (avg position ~31).
- 737 `/kozosseg-bekuldes?city=X&topic=Y` param URLs flagged "Duplicate without user-selected canonical."

## Contributing changes (deployed 05-29..05-31, reverted 06-05, commit e7d373e)

- **Shuffle** on listing pages (05-29): the Googlebot saw a different community order every crawl → content-instability signal.
- **`/out` redirect outclick tracking** (05-30): every outbound link became an internal redirect; Google crawled 2,072 "Page with redirect" URLs.
- **Extract-fingerprint restamp** (05-31): the entire page corpus's content re-changed at once, right during the crawl wave.

The 06-05 revert was correct but late; traffic did not recover, because the quality devaluation had already happened and recovery from "Crawled – currently not indexed" is slow (weeks–months).

## Fix

- Cross-domain canonical for HU pages → kozossegek. See [[seo-cross-domain-canonical]].
- Thin-page noindex + sitemap scoping (meetapedia sitemap omits HU cities). See [[indexing-strategy]].
- Resubmitted both sitemaps in Search Console.

## Lessons

- AI Overviews were ruled out as the main cause — AIO depresses CTR, but here **impressions** collapsed too, which is index-level.
- Never churn the whole corpus at once (fingerprint restamp, shuffle) — content stability is a ranking signal for programmatic sites.
- Two domains serving identical content need an explicit canonical strategy from day one, not self-canonicals.
- Fewer, richer pages beat many thin ones; ~20K of 27K pages were judged low-value.
