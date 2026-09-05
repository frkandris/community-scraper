---
type: Concept
title: Measuring Extraction Quality
description: How model scores are computed, what they actually mean, and the three ways the measurement was wrong before it was right.
tags: [scoring, evaluation, llm, router, minea]
timestamp: 2026-08-17
resource: scraper/scoring.py
---

# Measuring Extraction Quality

*The scores that decide routing order are only as good as the matcher underneath them, and that matcher was wrong three times before it was right.*

## What the score is

`POST /v1/score` runs every routed model over a fixed sample of cached pages and
scores each page:

- **20** for returning parseable output at all,
- **50 × recall**, **30 × precision**, both under one-to-one matching.

Answering with nothing is worth 20 — cheap to produce and useless.

**What it measures**: agreement with the *incumbent* extraction, not truth. The
expected names come from whichever model processed the page before, so a model
that finds a real club the incumbent missed is scored down for it. The right
question it answers is "can this free model replace the one that built our
corpus". A real correctness metric needs [needles](#what-is-still-missing).

## The sample must also be the right workload

`golden_set(..., locale="hu")` restricts it to one market, and measuring
without that is how a ranking ends up describing the wrong question. This
corpus is roughly 30% Hungarian and 70% international, so an unfiltered sample
is mostly English pages — while Hungarian is the primary market and the one
the routing order should serve.

A sibling project paid for this lesson in 2026-08: replaying a *real* thirty
candidate request separated four model configurations that a three-candidate
synthetic prompt had shown as equivalent. The model our own catalogue scored
higher (67 vs 62) was the one that slipped into English mid-answer and dropped
half the required fields — because our 67 was measured on English extraction.

Two consequences worth holding on to:

- **Scores in `providers.yaml` are measured-for-English-extraction**, not
  universal. Re-measure per market before trusting the order on a new one.
- **Measure on the real request shape.** A smaller synthetic prompt does not
  separate models that a full one does, and the difference only appears under
  the real load.

## The sample must not move

`golden_set` orders by `url_hash`, never by `extracted_at`. The pipeline
rewrites `extracted_at` continuously, so a "most recently extracted" sample is a
different set of pages every run — and scores taken an hour apart differ for
reasons that have nothing to do with models. This produced an apparent
`mistral-small: 80 → 55` that was pure sampling noise.

Each result carries a `sample` fingerprint. Two runs with different fingerprints
are not comparable.

That ordering is also what makes the sample portable: copying the `url_hash`
prefix out of the production database yields the identical pages the full one
would, so a score measured off-server is still comparable with the catalogue.
See [[exporting-a-golden-set]].

## Matching: lenient about phrasing, strict about identity

MINEA (arXiv:2404.04068) measured the same extractions at **59.4%** with exact
name matching and **88.4%** once containment was allowed. Exact matching
therefore understates every model. But loosening it has a worse failure mode: a
single generic word can sweep a page, and since `--apply` writes these numbers
into `providers.yaml`, a degenerate model gets promoted to the head of the
routing order.

Both mistakes were made, in that order:

| Attempt | Result |
|---|---|
| Token overlap thresholded on the *shorter* name | bare `"Sakk"` scored **100** on a page of chess clubs |
| Two-token floor + 8-character containment guard | `SV Musterstadt` ≠ `Sportverein Musterstadt`, a perfect extraction scored **20** |
| Strip generic tokens, then compare | every club in a town matched every other, so any `"<Town> <word>"` scored ~100 |

The first two shared an error: using a token's **shape** to guess whether it is
generic. That cannot work across the corpus's languages — Hungarian generics are
short (`klub`, `SE`), German ones are long compounds (`Schachverein`), so any
threshold is right for one and wrong for the other. The third was subtler and
worse: genericness was measured correctly by then, but *applying* it too early
threw away the very token that separates two clubs in one town.

### What works instead

Compare **full** token sets and look at what the *difference* contains:

    equal sets                                    -> same club
    one is a subset, everything the larger adds
      is generic, and the smaller still stands
      for a club on its own                       -> same club, spelled out
    each side has something the other lacks       -> different clubs

Removing generic tokens *before* comparing was the third mistake, and the worst:
it collapsed "Szentendrei Futóklub" and "Szentendrei Kajak Klub" both to
`{szentendrei}`, so **every club in a town matched every other**. Golden pages
are single city×topic pages, where every expected name shares a town — so a
model emitting any plausible `"<Town> <word>"` strings scored near 100. The club
type is precisely what distinguishes them; it is ignorable only when it is the
*sole* difference, which is what inspecting the difference expresses and
deleting it up front destroys.

"Stands for a club on its own" needs one more signal, because two structurally
identical cases must part ways: `{musterstadt}` from "SV Musterstadt" is a club,
`{szentendrei}` from a bare "Szentendrei" is not. The marker that separates them
("SV") is too short to survive tokenisation, so it is looked for in the raw
string — and only counts when something else stands beside it, or bare
"Futóklub" would qualify.

A token is generic if either holds:

1. **It ends in a club-type suffix** — `-verein`, `-klubb`, `-förening`,
   `-egyesület`, `-klub`. Compound languages build these productively, so no
   finite word list keeps up, but the ending does.
2. **It is frequent in the corpus** — document frequency over the *whole*
   `communities` table (tens of thousands of names). This is what catches the
   topic word: `sakk` is not a club type, it is the subject, so no rule can know
   it identifies nothing — but every chess club's name carries it.

Frequency must be measured on the full corpus, not the sample. A dozen pages
would never make `sakk` look common.

## Pairing is one-to-one and maximum

Counting matches independently let one answer satisfy several expected clubs,
and three phrasings of one club all count as precise. Greedy pairing fixed that
but introduced order-dependence: the same answer set scored 60 or 90 depending
on how the model happened to list clubs — reintroducing exactly the run-to-run
variance the fixed sample removed. `_pair_up` computes a maximum matching via
augmenting paths; the lists are a handful of names.

`expected` is deduplicated: with one-to-one pairing, a repeated name in a cached
extraction caps recall, scoring a correct distinct answer *below* a model that
repeats itself.

## Unmeasured is not zero

A model that never answered — rate limited, erroring — scores `null`, sorts
last, and is never written to the catalogue. Reporting 0 conflated "rate limited
for 1197 seconds" with "produced garbage", and `--apply` would have buried a
working model. `coverage` reports how many pages actually got an answer; the
score averages over those only, so reliability does not silently fold into
quality.

## What is still missing

Needle-based measurement. MINEA's actual method is to **inject** fabricated but
plausible entities into real pages and check whether they come back — you own
the ground truth without labelling anything. That would be the first metric here
about *correctness* rather than agreement. See
[the research notes](/docs/wiki/sources/2026-08-16-evaluation-and-throughput-arxiv.md).

Caveat the paper states plainly, and it applies to us: needles measure retrieval
of what you inserted. They cannot tell you what you are missing in the original
page.

## The score does not include the answer rate (2026-08-24)

Measuring the paid candidates on 14 Hungarian pages produced two models with
**the same score and a tenfold difference in usefulness**:

| model | score | answered | truncations |
|---|---:|---:|---:|
| `deepseek/deepseek-v4-flash` (0423, OpenRouter) | 80 | 13/14 | 2 |
| `deepseek-v4-flash` (0731, DeepSeek's own API) | 80 | 4/14 | 28 |
| `qwen/qwen3.7-flash` | — | 0/14 | 18 |

`score_fleet` grades the answers a model *gave*. A model that answers four
pages out of fourteen and gets them right scores exactly as well as one that
answers thirteen. Effective quality is `score × answered/pages` — 74, 23 and 0
here — and nothing in the catalogue records it, so the `quality:` values were
written by hand as 80 and 23 with the arithmetic in a comment.

**Reasoning models fail this workload at our settings.** All three of the
failures are `llm_output_truncated`: the `reasoning` text is billed and counted
as output, and it spends the 1,500-token cap before the extraction JSON closes.
Qwen writes eighty tokens of reasoning for a one-word question. The cheapest
list price on the market ($0.03/$0.13) was 100% waste — and a truncated call is
charged in full, so this is not "cheaper but weaker", it is money for nothing.

`max_output_tokens` is global, not per-provider, so a reasoning model cannot be
given a larger cap without also handing one to Groq, whose free tier reserves
`prompt + max_tokens` against an 8,000-token minute window
([[free-tier-model-router]]). Making it per-provider would be the prerequisite
for re-measuring these two.

The lesson from the sibling project held again, in the other direction: the
**four times more expensive** snapshot lost to the cheaper one on the real
Hungarian workload.
