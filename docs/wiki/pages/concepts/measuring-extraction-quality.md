---
type: Concept
title: Measuring Extraction Quality
description: How model scores are computed, what they actually mean, and the three ways the measurement was wrong before it was right.
tags: [scoring, evaluation, llm, router, minea]
timestamp: 2026-08-17
resource: scraper/scoring.py
---

# Measuring Extraction Quality

*The scores that decide routing order are only as good as the matcher underneath them, and that matcher was wrong three times in one evening.*

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

## The sample must not move

`golden_set` orders by `url_hash`, never by `extracted_at`. The pipeline
rewrites `extracted_at` continuously, so a "most recently extracted" sample is a
different set of pages every run — and scores taken an hour apart differ for
reasons that have nothing to do with models. This produced an apparent
`mistral-small: 80 → 55` that was pure sampling noise.

Each result carries a `sample` fingerprint. Two runs with different fingerprints
are not comparable.

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

The error common to both: using a token's **shape** to guess whether it is
generic. That cannot work across the corpus's languages — Hungarian generics are
short (`klub`, `SE`), German ones are long compounds (`Schachverein`). Any
threshold is right for one and wrong for the other.

### What works instead

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

Two names then match when their *distinctive* token sets are equal, or one is a
subset of the other and the smaller side still carries something distinctive.
Names that each carry a distinctive token the other lacks are different clubs —
`SV Grün-Weiß Musterstadt` vs `… Beispielstadt`.

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
