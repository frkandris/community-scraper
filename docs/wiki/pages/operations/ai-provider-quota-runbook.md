---
type: Runbook
title: Operating the AI Provider Fleet
description: How to bring a free LLM provider online, read the quota page, and react when one dies or changes its model names.
tags: [operations, llm, providers, quota, runbook]
timestamp: 2026-08-16
resource: config/providers.yaml
---

# Operating the AI Provider Fleet

*Adding capacity is one env var; every failure mode shows up on one admin page.*

Design rationale lives in [[free-tier-model-router]]. This page is the operator's view.

## Bringing a provider online

1. Sign up and create an API key.
2. Set the env var named in the **Key** column of `/admin/providers` (Coolify →
   Environment Variables, **runtime only** — a build-time secret broke the image
   build once, [[2026-07-ga4-env-buildtime-failure]]).
3. Redeploy. The provider appears as `ready` on the next page load.

Env vars per provider: `GROQ_API_KEY`, `CEREBRAS_API_KEY`, `GEMINI_API_KEY`,
`MISTRAL_API_KEY`, `OPENROUTER_API_KEY`, `GITHUB_MODELS_TOKEN`,
`DEEPSEEK_API_KEY`. Every one is optional and independent — a provider with no
key is skipped silently, which is the normal state, not an error.

## Reading /admin/providers

| Column | Means |
|---|---|
| Quality | 0-100 score on our extraction task. Seeded from public benchmarks until `scripts/score_providers.py` measures it. |
| Used today | Requests spent this UTC day, **including failed ones** — a rejected request still consumed a slot. |
| Budget | 95% of the effective daily limit, leaving headroom for enrichment and admin work. |
| `observed cap` | A 429 taught us the real limit. This number only ever goes down. |
| Status | `ready` / `rate limited` / `budget spent` / `no API key` / `disabled in config`. |

The **quality mix** panel shows cached pages grouped by the model that extracted
them. Rows below the current best score are what an upgrade sweep would revisit.

## Reading the logs

`GET /v1/logs?grep=model_retired` (Bearer `ROUTER_API_KEY`) returns the recent
in-memory log lines without a platform login — see [[router-gateway-api]]. It
needs a running app; a container that will not start is a Coolify deployment-log
job.

## When a provider starts failing

The run log names it (`extractor_model_retired`, `extractor_preflight_retired`,
`extractor_rate_limited`) and the row's `last_error` shows the message.

**Model names go stale fast — this is the normal failure, not an exception.** On
2026-08-16, within an hour of enabling six providers: Groq had deprecated
`qwen3-32b` and `llama-4-scout`, Gemini closed `gemini-2.5-flash` to new
projects, all three OpenRouter `:free` slugs had left the free tier, and GitHub
Models answered `410 github_models_retirement_brownout` — the service itself is
being retired. A 404/410 now retires that model for the run
(`ExtractorModelError`) instead of costing one wasted request per page.

- **Wrong model name** (`HTTP 400`) — the usual cause. Fix the `model:` line in
  `config/providers.yaml`; it is a mounted volume, so no deploy is needed. The
  fleet preflight retires such a model for the run rather than burning a wasted
  request on every page.
- **Revoked key** (`HTTP 401`) — rotate it in Coolify.
- **Persistent 429s** — the ledger has already lowered `observed_limit`; nothing
  to do. To force a re-probe, delete that day's row:
  `DELETE FROM provider_usage WHERE day='YYYY-MM-DD' AND provider='groq';`
- **Every model dead** — the run aborts with the reason attached, exactly as the
  single-provider path did ([[extractor-circuit-breaker]]).

## Changing priority or scoring

- Reorder by editing `quality:` values, or better: run
  `PYTHONPATH=. .venv/bin/python scripts/score_providers.py --apply` on the
  server, where the database with the golden set lives. Dry run first.
- `router.allow_paid: true` lets the fleet fall through to DeepSeek when free
  capacity is spent. Off by default — turning it on costs money.
- `router.enabled: false` disables routing entirely; extraction reverts to the
  single configured provider with no other behaviour change.
- `upgrade_min_gain` / `upgrade_max_per_run` bound the re-extraction sweep. Raise
  the gain to make upgrades rarer; set the max to 0 to stop them.

## What NOT to do

- **Do not give a provider its own `fingerprint_model`.** Every model in the
  fleet must share one, or the first routed run invalidates ~74K cached
  extractions and re-pays for them. See [[extraction-fingerprint-cache]].
- **Do not raise an `observed_limit` by hand** to "unlock" a provider. It was
  recorded because the provider refused at that count.
- **Do not enable a paid provider to clear a backlog.** The backlog is bounded
  by the run window, not by capacity; paying for it changes the cost model
  ([[cost-optimization-2026-07]]).
