---
type: Decision
title: Staged Description Enrichment — Plan (deferred, supervised)
description: Enriching thin community descriptions (~80→250 words) from cached raw_text is the biggest re-indexing lever, but must be run staged and supervised — not autonomously — because it costs LLM money at scale and risks re-triggering the 2026-06 corpus-churn devaluation.
tags: [seo, llm, enrichment, cost, deferred, thin-content]
timestamp: 2026-07-27
resource: scraper/extract.py
---

# Staged Description Enrichment — Plan (deferred, supervised)

*Community detail pages carry ~80–100 words of unique text; GSC (2026-07) shows
~97% of pages are "Crawled – currently not indexed". Thickening descriptions is the
single biggest lever to get them re-indexed. This page records the plan; the run is
**intentionally deferred to supervised execution** — see Why.*

## Why this was NOT run autonomously (2026-07-27 overnight batch)

Three properties make an unattended mass run irresponsible, so it was left for a
supervised session even under a broad "do everything" mandate:

1. **Cost at scale** — ~26K communities × an LLM call each is real money; the exact
   spend needs a human go/no-go, and should run inside DeepSeek's off-peak window.
2. **Corpus-churn risk** — the [[2026-06-seo-traffic-collapse]] was triggered partly
   by re-writing the whole page corpus at once (fingerprint restamp + shuffle).
   Re-writing every description simultaneously is the same anti-pattern. Enrichment
   MUST be gradual (small daily batches) so content stays stable.
3. **Unreviewable output** — AI-written descriptions that publish to real pages
   should be spot-checked by a human before scaling; that can't happen at 3am.

## The plan (when run supervised)

- **Target selection (pure, testable):** communities where
  `len(description.split()) < ~120` **and** the URL has cached `raw_text`
  (`cache_pages`). Skip anything already enriched (add an `enriched_at` / flag or a
  separate fingerprint so a page is enriched at most once per source change).
- **Prompt:** a dedicated "expand, don't invent" description prompt fed the cached
  `raw_text` + current fields; must not fabricate facts (schedule, contact, fees) not
  present in the source. Target ~200–250 words, same language as the community.
- **Batching / staging:** hard cap per run (e.g. 200–500 communities/day), off-peak
  (16:35→00:20 UTC, DeepSeek discount). Spread the corpus over ~2–4 weeks so
  `<lastmod>` (now content-change-aware — see [[persistence-layer]]) updates
  gradually, not all at once.
- **Write path:** update only the `description` field via the normal upsert so
  `updated_at`/`<lastmod>` advance for genuinely changed rows only.
- **Admin trigger:** an admin-only endpoint/card that runs one capped batch and
  reports counts — never an auto-run on deploy.
- **Monitoring:** watch GSC "Crawled – currently not indexed" → "Indexed" over the
  following weeks; if quality issues surface, stop and adjust the prompt before
  continuing.

## Related

Thin-content context: [[indexing-strategy]], [[2026-06-seo-traffic-collapse]].
Off-peak/cost model: [[cost-saver-schedule]], [[cost-optimization-2026-07]].
