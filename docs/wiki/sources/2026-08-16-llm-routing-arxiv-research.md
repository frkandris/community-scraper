# Arxiv research — multi-provider LLM routing for batch extraction (2026-08-16)

Raw research notes gathered before designing the free-tier AI router. Immutable
source input; the distilled decision lives in `pages/decisions/`.

## Question

We want to extract communities from ~thousands of cached pages per day using
several **free-tier** LLM providers (Groq, Google Gemini, Mistral, OpenRouter
`:free`, Cerebras, GitHub Models) instead of a paid one. Which routing policy is
scientifically defensible under hard per-provider quotas?

## Papers read

### 1. Is Escalation Worth It? A Decision-Theoretic Characterization of LLM Cascades
<https://arxiv.org/abs/2605.06350>

- Cascades (cheap model first, escalate on low confidence) pay **structural
  cost**: the cheap model's generation is always billed, even when the answer is
  discarded.
- Optimal thresholds sit on a piecewise-concave cost/quality frontier; a single
  shadow price equalizes marginal quality-per-cost across stage boundaries.
- **Key empirical result**: a lightweight *pre-generation* router beat the best
  cascade policy on 4 of 5 datasets, precisely because it skips the cheap
  model's wasted generation.

→ **Takeaway**: route *before* generating. Do not implement "cheap model first,
then re-run with the expensive model" as the default path.

### 2. Robust Batch-Level Query Routing for LLMs under Cost and Capacity Constraints
<https://arxiv.org/abs/2603.26796>

- Per-query routing cannot control batch-level cost; under non-uniform or
  adversarial batching it blows the budget.
- Their framework optimizes model assignment for a **whole batch** subject to
  hard per-model capacity limits, and is **uncertainty-aware** about its own
  quality predictions.
- Reported: up to 24% accuracy gain over per-query routing under adversarial
  batching; 1–14% from uncertainty-awareness alone.

→ **Takeaway**: our unit of decision is the batch (a run window), not the page.
Per-provider RPM/RPD/TPM are hard capacity constraints, exactly the paper's
formulation. Treat quality scores as intervals, not point estimates.

### 3. Task Cascades for Efficient Unstructured Data Processing
<https://arxiv.org/html/2601.05536>

- Generalizes model cascades along three axes: model, **operation** (a cheap
  surrogate instead of the full task), and **document scope** (pruned input).
- Cheap *surrogate* checks ("does this document mention X at all?") beat running
  the full task on a cheap model.
- Thresholds calibrated on a held-out validation split with variance-aware
  concentration bounds: `Pr[accuracy ≥ target] ≥ 1 − δ`.
- 48.5% cost saving vs. model cascades at a 90% accuracy target; savings
  concentrate on documents containing mostly irrelevant content.

→ **Takeaway**: most of our fetched pages yield zero communities. A cheap
surrogate gate is the single biggest available saving, and it is orthogonal to
which provider we route to.

### 4. LLMStructBench / ExtractBench — structured-extraction quality
<https://arxiv.org/abs/2602.14743>, <https://arxiv.org/pdf/2602.12247>

- LLMStructBench (22 models × 5 prompting strategies): the **prompting strategy
  matters more than model size** for JSON extraction quality.
- ExtractBench: frontier models degrade sharply as schema breadth grows — 0%
  valid output on a 369-field schema.

→ **Takeaway**: a public leaderboard rank is a weak prior for *our* task. Score
providers on our own prompt with a golden set; keep the published benchmark
number only as a tie-breaking prior. Our schema is narrow, which is the regime
where small models still work.

### 5. Conformal Elo Estimation / Rank Intervals for Leaderboards
<https://arxiv.org/abs/2606.13221>, <https://arxiv.org/html/2606.08679v1>

- Point-estimate Elo hides ranking uncertainty; calibrated methods report rank
  *intervals*.

→ **Takeaway**: when the router picks "the better free model", ties within the
uncertainty band should be broken by remaining quota, not by a 3-point score
difference.

## Free-tier limits observed (2026-08, secondary sources — verify in console)

| Provider | Published free limits |
|---|---|
| Groq | ~30 RPM, 6K TPM, up to 14.4K req/day (model-dependent; Llama 4 Maverick ~500 RPD) |
| Cerebras | ~30 RPM, ~1,000 RPD, ~1M tokens/day |
| Google Gemini (AI Studio) | ~1,500 req/day |
| Mistral | ~50K TPM (limits moved behind console login 2026-07) |
| OpenRouter `:free` | 20 RPM, 50 req/day (1,000/day after a $10 preload) |
| GitHub Models | published per-tier, not per-model; low RPD |

Sources: ianlpaterson.com, tokenmix.ai, klymentiev.com, merginit.com,
awesomeagents.ai (all 2026 round-ups). Several providers stopped publishing
numbers, so the router must **learn** the real limit from 429 responses rather
than trust a config constant.

## Synthesized design rules

1. **Pre-generation routing, not cascading** — pick the provider before the call.
2. **Batch/window-level budget accounting** — quotas are the constraint; track
   spend per provider per window.
3. **Learn limits from 429s** — published numbers are stale; back off and record.
4. **Quality = own golden-set score, prior = public benchmark** — never the
   reverse.
5. **Rank ties broken by remaining quota**, not by score decimals.
6. **New work outranks re-work** — re-extraction is only ever a filler for
   quota that would otherwise expire unused (matches the operator's policy).
