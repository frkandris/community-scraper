#!/usr/bin/env python3
"""Measure each free model on OUR extraction task and rewrite its quality score.

Why this exists
---------------
The `quality:` numbers shipped in config/providers.yaml are seeded from public
leaderboards, and a leaderboard rank is a weak proxy for what we actually need.
LLMStructBench (arXiv:2602.14743) evaluated 22 models across five prompting
strategies and found the *prompting strategy* matters more than model size for
JSON extraction; ExtractBench (arXiv:2602.12247) found frontier models collapse
to 0% valid output as schema breadth grows. Neither result survives being
compressed into "model X ranks above model Y".

So: score every model on the real prompt, against a small golden set of pages
whose correct answer we already know, and write the measured number back.

Usage
-----
    PYTHONPATH=. .venv/bin/python scripts/score_providers.py                # dry run
    PYTHONPATH=. .venv/bin/python scripts/score_providers.py --apply        # rewrite yaml
    PYTHONPATH=. .venv/bin/python scripts/score_providers.py --only groq

The golden set is built from the local database: recently cached pages that
previously yielded at least one community. Be clear about what that measures —
the "expected" names come from whichever model extracted the page before, so
this scores **agreement with the incumbent extraction**, not ground truth. It is
our own distribution and costs nothing to maintain, which is why it beats a
public benchmark here; it is not an accuracy oracle. To make it one, curate a
hand-checked page set and point --db at it.

Scoring (0-100), per page:
    20  the model returned parseable output at all
    50  recall — fraction of expected community names it found
    30  precision — penalty for names it invented
A model that errors or returns unparseable output scores 0 for that page.
Answering with an empty result is therefore worth 20, not a passing grade:
"I found nothing" is cheap to produce and useless to us.
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scraper.providers import (PROVIDERS_FILE, build_extractors,  # noqa: E402
                               load_catalogue)
from scraper.scoring import score_fleet  # noqa: E402

DB = ROOT / "data" / "scraper.db"
PROVIDERS_YAML = ROOT / "config" / PROVIDERS_FILE


def rewrite_yaml(scores: dict[tuple[str, str], int]) -> int:
    """Update `quality:` in place, keyed by (provider, model).

    Keyed by the pair, not the model id alone: two providers can list the same
    model (OpenRouter and a first-party API both serve `deepseek-*`), and a
    model-only key would write one provider's measured score into the other's
    block.

    Line-oriented rather than a YAML round-trip so every comment in the file —
    which is most of its value — survives untouched.
    """
    lines = PROVIDERS_YAML.read_text(encoding="utf-8").splitlines(keepends=True)
    provider = model = None
    changed = 0
    for i, line in enumerate(lines):
        p = re.match(r"^\s*- name:\s*(\S+)\s*$", line)
        if p:
            provider, model = p.group(1), None
            continue
        m = re.match(r"^\s*- model:\s*(\S+)\s*$", line)
        if m:
            model = m.group(1)
            continue
        q = re.match(r"^(\s*)quality:\s*(\d+)\s*$", line)
        if q and (provider, model) in scores:
            new = scores[(provider, model)]
            if int(q.group(2)) != new:
                lines[i] = f"{q.group(1)}quality: {new}\n"
                changed += 1
            model = None
    PROVIDERS_YAML.write_text("".join(lines), encoding="utf-8")
    return changed


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pages", type=int, default=12,
                    help="golden-set size (default 12; every model runs all of them)")
    ap.add_argument("--only", help="score just this provider")
    ap.add_argument("--apply", action="store_true", help="write scores back to the YAML")
    ap.add_argument("--db", type=Path, default=DB)
    args = ap.parse_args()

    catalogue = load_catalogue()
    fleet = build_extractors(catalogue, fingerprint_model="scoring", allow_paid=True)
    if args.only:
        fleet = [e for e in fleet if e.provider == args.only]
    if not fleet:
        raise SystemExit("no model has an API key set — nothing to score")

    out = await score_fleet(args.db, fleet, pages=args.pages)
    if out.get("error"):
        raise SystemExit(out["error"])

    print(f"golden set: {out['pages']} pages, "
          f"{out['expected_communities']} expected communities\n")
    print(f"{'score':>5}  {'prior':>5}  {'ans':>4} {'fail':>4}  model")
    for r in out["results"]:
        shown = "  n/a" if r["score"] is None else f"{r['score']:5}"
        print(f"{shown}  {r['prior']:5}  {r['answered']:4} {r['failed']:4}  "
              f"{r['provider']}:{r['model']}")
        for e in r["errors"]:
            print(f"{'':>24}! {e}")
    if out.get("unmeasured"):
        print("\nUNMEASURED (rate limited or erroring — not scored, not written):")
        for m in out["unmeasured"]:
            print(f"  {m}")
        print("Re-run these when the fleet is idle; a rate limit is not a quality signal.")
    print(f"\n{out['note']}")

    if args.apply:
        # Only measured models are written. A null would otherwise land in the
        # catalogue as a 0 and bury a good model at the bottom of the order.
        scores = {(r["provider"], r["model"]): r["score"]
                  for r in out["results"] if r["measured"]}
        skipped = len(out["results"]) - len(scores)
        print(f"\nupdated {rewrite_yaml(scores)} quality values in {PROVIDERS_YAML}"
              + (f" ({skipped} unmeasured left untouched)" if skipped else ""))
    else:
        print("\n(dry run — pass --apply to write these into providers.yaml)")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
