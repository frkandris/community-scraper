---
type: Decision
title: Free-Tier Model Router
description: Extraction routes across six free LLM providers by measured quality under a persisted daily quota ledger, with paid DeepSeek parked behind a flag.
tags: [llm, routing, cost, providers, quota, decision]
timestamp: 2026-08-16
resource: scraper/router.py
---

# Free-Tier Model Router

*Spend free capacity first, pick the best model that still has budget before generating anything, and never re-do a page while new ones are waiting.*

## Decision

Extraction runs over a **fleet** of free-tier providers (Groq, Cerebras, Google
Gemini, Mistral, OpenRouter `:free`, GitHub Models) instead of a single paid one.
DeepSeek stays configured but **parked** behind `router.allow_paid: false`.

The catalogue lives in `config/providers.yaml`, not in code, because free
providers rename and retire models constantly — Groq deprecated two Llama models
in June 2026 and DeepSeek's July rename cost a whole extraction window
([[2026-07-deepseek-model-retired]]). `config/` is version-controlled and shipped in the image, NOT a persisted
volume in
production, so correcting a model name needs no deploy.

## Why these rules and not others

Grounded in the reading captured in
[the 2026-08-16 arXiv notes](/docs/wiki/sources/2026-08-16-llm-routing-arxiv-research.md):

**Route before generating; do not cascade.** The obvious design — cheap model
first, escalate when the answer looks weak — pays the cheap model's generation
*before* every escalation decision. arXiv:2605.06350 characterises this as the
structural cost of cascades and shows a lightweight pre-generation router beat
the best cascade policy on 4 of 5 datasets. So `ModelRouter.order()` picks the
best model with budget and calls it **once**.

**Budget at the window level, not per query.** arXiv:2603.26796 shows per-query
routing cannot control batch-level spend under non-uniform load; free tiers are
hard per-day capacities, which is exactly that paper's formulation. Hence
`QuotaLedger`, and hence it is consulted *before* each call rather than after a
429.

**Learn the ceiling from 429s — but only daily ones.** Several providers stopped
publishing their limits in 2026, so a config constant is a guess. A 429 whose
`Retry-After` is long (or that arrives near the configured `rpd`) records the
count at which the provider actually refused, and that number is only ever
lowered. A *short* 429 is the per-minute limit, which says nothing about the
daily ceiling: free tiers publish `rpm` of 10-30 next to `rpd` of 150-14400, so
reading a minute-limit hit as a daily one would collapse Gemini's 1500/day to 15.
`rpm` is enforced separately, per provider — a provider with two models has two
extractors, and each `_ApiExtractor` otherwise paces itself independently.

**Break ties on quota, not decimals.** arXiv:2606.13221 argues point-estimate
leaderboard scores hide ranking uncertainty. Two models three points apart are
not reliably ordered; remaining quota is measured, so it decides.

**Failures count against the budget.** A 429 or a 400 still consumed a request
slot at most providers. Undercounting is precisely how a router walks into a
hard block it should have predicted.

**"Callable now" and "callable today" are different questions.** `order()`
answers the first (it excludes providers inside an rpm cooldown) and is right
for picking a provider. Planning decisions — is there capacity left, should the
sweep run, is a gateway request servable — must use `with_budget()` /
`has_capacity()`, which ignore pacing. Conflating them made a two-second
cooldown read as "no quota left today".

**rpm is a wait; rpd is a veto.** Per-minute pacing suspends the call until the
window reopens (`_await_pacing`); a spent daily budget removes the provider.
Conflating them is a live grenade: pacing treated as unavailability makes `_call`
find nothing to try, and the breaker counts each of those as a failure — 24 a
minute at `rpm: 30`, so the run aborts within seconds of starting.

**Running out is not an outage.** When every provider is out of daily budget the
chain raises `ExtractorQuotaError` and sets `quota_exhausted`, and the pipeline
stops the window cleanly. The exception **type** stays
`ExtractorUnavailableError`: `ExtractorQuotaError` is not a subclass of it, so
raising that would sail past every `except ExtractorUnavailableError` in the
pipeline and fail the run outright — the very outcome the clean stop exists to
prevent. Without the distinction the circuit breaker counts 20 such pages, flips
`providers_down`, and reports a provider failure in the run banner and the daily
email, for the designed end state of a free-tier day.

**The chain holds the whole fleet, not today's survivors.** `build_extractor`
uses `all_extractors()`, not `order()`. The chain is built once for a run that
spans the off-peak window and crosses midnight; filtering on live quota at build
time would drop a spent provider for the entire run, and the ledger's day
rollover could never readmit it. Availability is re-checked per call instead.

**The ledger follows the clock.** The `ai_only` window crosses midnight UTC, so
it re-rolls onto the new day's row rather than spending yesterday's, and re-reads
the DB every 25 calls so the concurrently running enrichment job's spend is
visible.

## The fingerprint invariant

**Every extractor in the fleet is pinned to the same `fingerprint_model`.**

The extraction cache is keyed by `SHA-256[:12](prompt + model)`
([[extraction-fingerprint-cache]]). If the fingerprint tracked whichever model
the router happened to pick, the first routed run would invalidate ~74K cached
extractions and re-pay for all of them — and `get_fully_processed_pairs` would
stop recognising any pair as done.

So the fingerprint follows `deepseek_fingerprint_model` exactly as before, and
*which* model actually ran is recorded in `cache_pages.extract_model` /
`extract_quality` — deliberately outside every cache key. `pipeline.py`'s
`_served_by()` reads it from the chain, because `extractor.model` only names the
head of the chain and lies whenever failover or routing kicked in.

## Fleet preflight

With one provider, a bad model name fails the run immediately
([[extractor-circuit-breaker]]). With a fleet, failover *hides* it: every page
silently burns a wasted request on the dead model before falling through.
`_preflight_fleet()` probes each model once (~15 calls) and retires the broken
ones for the run. Only an entirely dead fleet aborts.

## The upgrade sweep

The operator's policy, and the one the research supports: **new work outranks
re-work.** Free allowances do not roll over, so an unspent request is lost at
midnight — but a request spent re-doing a page while unprocessed pages exist is
worse than lost, because it delays new coverage.

`_run_quality_upgrade()` therefore runs only when all four hold:

1. `ai_only` mode and the normal pass had nothing left to collect;
2. the caller passed `allow_upgrade` — `_cron_run` grants it to the **last**
   country group only, since `run_pipeline` runs once per group and an
   already-finished leading group would otherwise spend the window while later
   groups still have uncollected pages;
3. an available model beats the cached result by ≥ `upgrade_min_gain` points;
4. inside `upgrade_max_per_run` and the run window.

It re-checks the budget **per page**, because the fleet drains as it spends. A
failed re-extraction leaves the old result in place — a weaker answer beats no
answer.

Two exclusions the ordering alone cannot express:

* **`extract_quality IS NULL` is skipped, not ranked worst.** Those ~74K rows
  predate the router and came from the paid incumbent, which scores *above*
  every free model. Treating NULL as 0 would have the sweep overwrite good
  DeepSeek output with weaker free output — a downgrade wearing an upgrade's
  name.
* **Tier-frozen pairs stay frozen.** `topic_tier: core` cities run only
  `core_topics`; the sweep applies `_tier_allows` exactly as the main passes do.
  The *city* restriction goes into the SQL, before `LIMIT` — filtering a
  globally-ordered top-N afterwards returns nothing while eligible pages sit
  lower down.

Writes are batched per `(city, topic)` after the loop, never per page:
`save_results` ends in a full topic DELETE+reinsert, an O(n²) dedup and a
city-wide duplicate scan.

**Known limitation: the sweep can only add, never remove.** `save_results`
merges by `record_key` and rewrites the union, so a false positive a better
model correctly rejects survives. Dropping bad records is a genuine reason to
re-extract and this does not deliver it — removals still go through the admin
not-community flow.

The sweep is called from the `if not pairs_to_run:` branch of `run_pipeline` —
the only place where "nothing new to collect" is actually true. Calling it after
the pair loops would make it unreachable, since every path there has new work
pending by definition.

## Scoring

Shipped `quality:` values are seeded from public benchmarks and are explicitly a
**weak prior**: LLMStructBench found prompting strategy outweighs model size for
JSON extraction. `scripts/score_providers.py` and `POST /v1/score` replace them with measured
scores on a fixed golden set drawn from our own cached pages. How the matching
works, and the three ways it was wrong first, is in
[[measuring-extraction-quality]] — read it before trusting any number here.

## Status and rollback

`router.enabled: true` ships on, but every provider is keyless until its env var
is set, so the effective behaviour today is the unchanged single-provider
DeepSeek path. Bringing a provider online is one env var; turning the whole
thing off is `router.enabled: false`. Operational view:
[[ai-provider-quota-runbook]]. The same fleet is exposed to other software as an
OpenAI-compatible endpoint — [[router-gateway-api]].
