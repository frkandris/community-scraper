---
type: Concept
title: The Paid Spend Guard
description: A daily USD ceiling in the quota ledger that makes paid providers unavailable once the day's spend reaches it — the money equivalent of the free tier's 429, which nobody sends us.
tags: [router, billing, quota, providers, circuit-breaker]
timestamp: 2026-08-27
resource: scraper/router.py
---

# The Paid Spend Guard

*A free provider stops us when we have had enough; a paid one does not, so the
stop has to live here.*

## The asymmetry it exists for

The free fleet is bounded by things the provider enforces. Run out of requests
and it answers 429. Run out of tokens and it answers 429 with a different
reason. [[free-tier-model-router]] models both because both are real edges we
would otherwise walk into.

A paid provider has no such edge. It answers 200 until the card declines. Every
mechanism the router had for "you have had enough" was, underneath, a mechanism
for reading somebody else's refusal — so when paid providers were switched on it
had nothing to say at all
([[2026-08-paid-fallback-burned-the-budget]]).

## Shape

Three settings, one of which is the whole point:

```yaml
router:
  allow_paid: true          # paid providers MAY be used
  daily_budget_usd: 2.00    # ...up to this much per UTC day. 0 = not at all.
```

`allow_paid` is a permission; `daily_budget_usd` is the amount. Neither works
alone: `paid_allowed()` is false without both, so the historical failure mode —
a boolean switched on by itself — is now a no-op rather than an open tab.

Per model, the price the ceiling is measured in:

```yaml
- model: deepseek/deepseek-v4-flash
  usd_per_1m_in: 0.0795
  usd_per_1m_out: 0.159
```

`build_extractors` **refuses to build a paid model that has neither**. An
unpriced paid call spends real money and reports $0.00 against the budget, which
is precisely the runaway the guard exists to stop. Fail closed.

## How a call is priced

`_ApiExtractor.last_cost_usd` reads the split the provider itself reported
(`prompt_tokens`, `completion_tokens`) and multiplies by the model's rates. Both
live in ContextVars, per task, for the same reason `last_tokens` does: one
extractor instance serves several concurrent pages.

Three deliberate choices:

- **`_post` clears the counters before every attempt.** A connection error never
  reaches `_note_usage`, and the attempt would otherwise be charged with the
  previous call's numbers.
- **A provider reporting only a total is priced at the input rate.** Low, but
  never zero — a guard that reads a call as free is not a guard.
- **Failures are charged.** A truncated answer is billed in full, and truncation
  was most of what the 2026-08-24 experiment bought. Counting only successes
  would have hidden the runaway from its own guard.

`FallbackExtractor._note_router` reads the cost off the extractor rather than
taking it as an argument at each of fifteen call sites. A budget that depends on
every future `except` branch remembering to pass a price is a budget with holes.

## It is a circuit breaker, and follows that pattern's rules

Per <https://martinfowler.com/bliki/CircuitBreaker.html>:

- **State changes are logged** — `paid_daily_budget_spent`, once per router
  rather than once per refused call.
- **State is revealed** — `/admin/providers` shows spend against ceiling
  whether or not it has tripped, and the daily report states it every morning.
- **It resets itself** — at 00:00 UTC, the same boundary the free allowances
  use.
- **Operators can see and change it** — the ceiling is one line in
  `config/providers.yaml`, which is git, which is the only place a production
  config change survives a deploy.

## What it does not do

It does not stop a run. Paid providers simply leave the routing order, the fleet
finishes on free capacity, and when that is gone the run ends the way a spent
free window always ends: `quota_exhausted`, no `aborted` flag, nothing cached,
everything retried tomorrow. A budget ceiling is a normal end to a day, not an
outage.
