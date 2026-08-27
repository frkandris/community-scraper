---
type: PostMortem
title: The Paid Fallback Burned the Budget
description: allow_paid went on without a spend ceiling, the cheap provider it was switched on for had no account credit, and four days of extraction ran through a fallback costing four times as much — about $60 for pages that mostly failed.
tags: [providers, router, billing, quota, post-mortem, openrouter, deepseek]
timestamp: 2026-08-27
resource: scraper/router.py
---

# The Paid Fallback Burned the Budget

*Permission to spend money is not a decision until it has an amount attached.*

## What happened

`router.allow_paid` was switched on on 2026-08-24 with two paid providers
configured: `openrouter_paid`, serving DeepSeek V4 Flash 0423 at roughly a
quarter of the price and measured at 80 on 13 of 14 Hungarian pages; and
`deepseek`, the incumbent's own API, deliberately scored 23 and documented as
"a last-resort fallback for the day OpenRouter is unreachable; nothing should
ever route here first".

There was no credit on the OpenRouter account. Every `openrouter_paid` call was
refused. Every page therefore fell through to the fallback.

The 2026-08-26 report, read on the 27th:

| provider | calls | failures | tokens | cost |
|---|---|---|---|---|
| `openrouter_paid` | 41 | 41 | 0 | — |
| `deepseek` | 10,160 | 3,586 | 33.5M | ~$20 |

41 is not a coincidence: it is the number of runs that day. `preflight()`
probes each provider once per run, and the probe's skip condition asked only
about requests and tokens — never about whether the provider was refusing for
a reason a probe could not fix. So the provider was blocked for the day, then
probed again, all day, every day.

Four days cost about $60 of the $82.10 the account had spent in thirty. The
balance alert on the 27th is what surfaced it.

## Why it cost that much

The bill was not extraction. **10,319 extraction attempts produced 335
processed pages** — 31 attempts per page.

A failed extraction is deliberately never cached: caching it would record "0
communities" permanently under the current fingerprint and the page would never
be retried (see [[extraction-fingerprint-cache]]). The cost of that correct rule
is that a page which fails *deterministically* is re-attempted by every run
forever, and each attempt walks the whole fleet. The run list shows it plainly —
the same `112 pairs … 21 pages` in run after run, roughly thirty times a day.

Those pages failed for a reason we had already diagnosed and named: all three
paid candidates are reasoning models, the `reasoning` text is billed and counted
as output, and it spends the 1,500-token cap before the extraction JSON closes.
`max_output_tokens` was global, so raising it for a paid endpoint would have
handed the same number to Groq — whose free tier reserves `prompt + max_tokens`
against an 8,000-token minute window *before* generating. The comment in
`_warn_if_truncated` said what would happen: *"The page is then retried forever
against the same cap."* It was written before retrying cost money.

## Root causes

1. **No unit of money anywhere.** The quota ledger counted requests and tokens,
   both of which looked healthy — `deepseek` used 10,160 of 95,000. The
   constraint that was actually binding was denominated in dollars and nothing
   held that number.
2. **A permission without an amount.** `allow_paid: true` is a boolean. It
   cannot express "yes, up to this much".
3. **Silence on total failure.** A provider at 41 calls / 41 failures read as an
   ordinary row in a table of counts. The same shape had already been missed on
   2026-08-22 ([[2026-08-cerebras-free-tier-ended]]) — twice now, the report
   showed 100% failure and nobody's eye caught it.
4. **A correct rule with no bound.** "Never cache a failure" needed a companion:
   "and stop retrying one that never changes".

## What changed

- **A daily USD ceiling** (`router.daily_budget_usd`, `$2.00`). Paid providers
  become unavailable for the rest of the UTC day when the day's paid spend
  reaches it; free capacity is untouched. **0 means no paid calls at all**, and
  that is the default — `allow_paid` alone now spends nothing. See
  [[paid-spend-guard]].
- **Cost in the ledger.** `provider_usage.cost_usd`, accumulated from the
  provider's own reported usage and the catalogue's per-model price. Refused
  calls are counted too: a truncated answer is charged in full.
- **Fail closed on price.** `build_extractors` refuses to build a paid model
  with no `usd_per_1m_in`/`usd_per_1m_out` — an unpriced paid call would report
  $0.00 against the ceiling.
- **Preflight asks the right question.** `ModelRouter.done_for_today()` covers
  money, requests and tokens; a probe is only spent on a provider a probe could
  actually revive.
- **Per-model `max_output_tokens`.** The paid reasoning models get 4,000; Groq
  keeps the global 1,500.
- **An extraction quarantine.** After three *content* failures at one
  fingerprint a page stops being attempted. Only `ExtractorContentError` counts —
  a model answered and the answer was unusable. Outages, 429s and spent quotas
  say nothing about the page. See [[extraction-quarantine]].
- **The report states the money**: spend against ceiling every morning, and a
  paid provider whose calls all failed is named rather than tabulated.

## Lesson

Free capacity ends in a 429 that nobody has to read. Paid capacity ends when
somebody reads an invoice. Every guard the free fleet has — a persisted daily
budget, a learned ceiling, a blocked flag — existed because a provider enforces
it for us. Nothing enforces a paid budget except us, so switching paid
providers on is not one change; it is a change plus the ceiling that bounds it.

The second lesson is smaller and older: a rule that says "never record this
outcome" needs a partner that says "and stop asking". We had the first for
sixteen months and it was right; it was only ever free because the retries were.
