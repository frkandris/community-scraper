# Arxiv research — measuring extraction without labels, and why our throughput is capped (2026-08-16, round two)

Raw notes gathered after the first live day of the free-tier router, against the
two gaps the step-back identified: we cannot tell whether the extraction is
*good*, and the fleet's daily quota takes ~9 hours to spend because the chain is
serial.

## Question 1 — how do you measure extraction quality with no ground truth?

Our `POST /v1/score` compares a model's output against whatever model extracted
the page before. That answers "can this replace the incumbent" and nothing about
correctness: a model that finds a real club the incumbent missed is *penalised*.

### MINEA — Multiple Infused Needle Extraction Accuracy
<https://arxiv.org/html/2404.04068v1>

Inject fabricated but contextually plausible entities ("needles") into real
documents, then check whether the pipeline extracts them. Because you authored
the needle, **you own the ground truth without labelling anything**.

Method: an LLM generates a short paragraph describing a plausible new entity
matching the schema → it is inserted at a random position → the normal
extraction runs → matching decides whether it came back.

Four matching strategies, score = max across them:

| | mechanism |
|---|---|
| `n` | exact name match |
| `ns` | needle name appears anywhere in the output |
| `k` | keyword overlap above a threshold |
| `llm` | an LLM judges whether the needle was extracted |

**The number that matters for us**: exact-name matching scored persons at
**59.4%**, while allowing a full-text match took the same runs to **88.4%**. Our
`score_page` uses exact normalised-key matching only — so our measured scores are
biased *down*, and unevenly, because the bias depends on how each model phrases
a name.

Other findings worth keeping:

- **Lost in the middle**: needles placed mid-document are missed far more often
  than ones at the edges. Our pages are truncated to `max_text_chars` (8000)
  from the top, so anything in a long page's middle is doubly disadvantaged.
- **Iterating the extractor** helps up to ~3 passes, then hallucination rises
  (a "bias avoidance" score fell 0.56 → 0.42). Argues against naive re-extraction
  as a quality lever — relevant to our upgrade sweep.
- **Custom schema types score worse than schema.org ones** (67–75% vs 88–94%).
  Our `CommunityRecord` is custom and wide.
- Caveat the paper states plainly: MINEA measures retrieval of entities *you
  inserted*. It cannot tell you what you are missing in the original document.

### LLM-as-judge for semantic evaluation
<https://arxiv.org/html/2603.18652v1>

For table extraction, an LLM judge agreed with human ratings substantially
better than rule-based metrics (1,554 human ratings over 518 pairs). Supports
`llm` as the fallback matcher above rather than as the primary one.

## Question 2 — why is the fleet's throughput capped at ~9 hours of quota?

### Throughput-Optimal Scheduling for LLM Inference
<https://arxiv.org/abs/2504.07347>

Queueing-theoretic result: a broad class of **work-conserving** schedulers
achieves maximum throughput. Work-conserving means never leaving capacity idle
while work is pending.

**Our chain violates this directly.** `FallbackExtractor._call` tries providers
left to right and issues *one* request at a time. When the best model is inside
its rpm cooldown we `_await_pacing()` — sitting idle — while four other
providers with independent rate limits have free capacity. Five providers, ~16.5K
calls/day, and we serialise them into ~2s per call: 9.2 hours to spend a budget
that could be spent in about two.

The paper's framing makes this a known-cost decision rather than an oversight:
idle capacity under a solvable constraint is lost throughput, and the fix is
concurrency across independent resources, not faster single-stream calls.

## What follows

1. **Fix the matcher first** (cheap, and it corrects numbers we already acted
   on): add `ns`/`k` fallbacks to `score_page`, keep exact match as the primary.
   Our current scores are underestimates of unknown size.
2. **Then add needles** for a real recall measurement — the first metric we
   would have that is about correctness rather than agreement.
3. **Then parallelise the chain** across providers. Largest win, largest blast
   radius: it touches the code path every extraction goes through, so it wants
   its own change and its own review round, not a late-night edit before an
   unattended overnight run.
