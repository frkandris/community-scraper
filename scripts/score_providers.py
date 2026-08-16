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
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scraper.db import _connect  # noqa: E402
from scraper.identity import normalized_match_key  # noqa: E402
from scraper.providers import (PROVIDERS_FILE, build_extractors,  # noqa: E402
                               load_catalogue)

DB = ROOT / "data" / "scraper.db"
PROVIDERS_YAML = ROOT / "config" / PROVIDERS_FILE


def golden_set(db_path: Path, limit: int) -> list[dict]:
    """Cached pages plus the community names we believe they contain.

    Only pages that yielded at least one surviving community are used: a page
    with zero expected results cannot distinguish a careful model from one that
    always answers "nothing here".
    """
    if not db_path.exists():
        raise SystemExit(f"no database at {db_path} — run this where the data lives")
    out: list[dict] = []
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT url, city, topic, data
              FROM cache_pages
             WHERE extracted_at IS NOT NULL
             ORDER BY extracted_at DESC
             LIMIT ?
            """,
            (limit * 8,),
        ).fetchall()
    for url, city, topic, blob in rows:
        try:
            entry = json.loads(blob)
        except (TypeError, ValueError):
            continue
        text = entry.get("raw_text")
        expected = {normalized_match_key(r.get("name", ""))
                    for r in entry.get("records") or [] if r.get("name")}
        if not text or not expected:
            continue
        out.append({"url": url, "city": city or "", "topic": topic or "",
                    "text": text, "expected": expected})
        if len(out) >= limit:
            break
    return out


def score_page(expected: set[str], got: set[str]) -> float:
    """20 for answering at all, 50 × recall, 30 × precision.

    Answering is worth little on its own: a model that reliably returns `{}` is
    parseable and worthless, and weighting mere responsiveness highly would rank
    it near a model that actually reads the page.
    """
    if not expected:
        return 0.0
    recall = len(expected & got) / len(expected)
    precision = (len(expected & got) / len(got)) if got else 0.0
    return 20.0 + 50.0 * recall + 30.0 * precision


async def score_model(extractor, pages: list[dict]) -> tuple[int, int, int]:
    """(score 0-100, pages answered, pages failed)."""
    total, answered, failed = 0.0, 0, 0
    for page in pages:
        try:
            records = await extractor.extract(
                text=page["text"], city=page["city"], topic=page["topic"],
                locale="hu", source_url=page["url"],
            )
        except Exception as exc:
            failed += 1
            print(f"      ! {type(exc).__name__}: {str(exc)[:90]}")
            continue
        got = {normalized_match_key(r.name) for r in records if r.name}
        total += score_page(page["expected"], got)
        answered += 1
    if not answered:
        return 0, 0, failed
    # Unanswered pages score zero: an unreliable model is a worse model.
    return round(total / len(pages)), answered, failed


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
                    help="golden-set size (default 12; each model runs all of them)")
    ap.add_argument("--only", help="score just this provider")
    ap.add_argument("--apply", action="store_true", help="write scores back to the YAML")
    ap.add_argument("--db", type=Path, default=DB)
    args = ap.parse_args()

    pages = golden_set(args.db, args.pages)
    if not pages:
        raise SystemExit("no usable golden pages — need cached pages with records")
    print(f"golden set: {len(pages)} pages, "
          f"{sum(len(p['expected']) for p in pages)} expected communities\n")

    catalogue = load_catalogue()
    fleet = build_extractors(catalogue, fingerprint_model="scoring", allow_paid=True)
    if args.only:
        fleet = [e for e in fleet if e.provider == args.only]
    if not fleet:
        raise SystemExit("no model has an API key set — nothing to score")

    scores: dict[tuple[str, str], int] = {}
    for extractor in fleet:
        print(f"  {extractor.provider}:{extractor.model} (prior {extractor.quality})")
        score, answered, failed = await score_model(extractor, pages)
        scores[(extractor.provider, extractor.model)] = score
        print(f"      → {score}/100  ({answered} answered, {failed} failed)\n")

    print("measured scores:")
    for (provider, model), score in sorted(scores.items(), key=lambda kv: -kv[1]):
        print(f"  {score:3d}  {provider}:{model}")

    if args.apply:
        changed = rewrite_yaml(scores)
        print(f"\nupdated {changed} quality values in {PROVIDERS_YAML}")
    else:
        print("\n(dry run — pass --apply to write these into providers.yaml)")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
