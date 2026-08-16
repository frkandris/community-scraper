"""Measure each routed model on our own extraction task.

The `quality:` numbers in config/providers.yaml start as a prior read off public
leaderboards, and 2026-08-16 showed how weak that prior is: every shipped model
name was stale within hours, and a leaderboard rank says nothing about JSON
extraction on Hungarian club pages. LLMStructBench (arXiv:2602.14743) found the
prompting strategy outweighs model size for exactly this task.

So score the fleet on pages whose answer we already hold.

**What this measures, precisely**: agreement with the *incumbent* extraction, not
ground truth. The "expected" names come from whichever model processed the page
before. That is the right yardstick for "can this free model replace DeepSeek
here" and the wrong one for "is this model correct" — a model that finds a real
club the incumbent missed is scored down for it.

Shared by `scripts/score_providers.py` (CLI) and `POST /v1/score` (remote), so
the two cannot drift.
"""
from __future__ import annotations

import json
from pathlib import Path

import structlog

from .db import _connect
from .identity import normalized_match_key

log = structlog.get_logger()

#: Per page: 20 for answering at all, 50 × recall, 30 × precision. Answering is
#: worth little on its own — a model that reliably returns `{}` is parseable and
#: useless, and weighting responsiveness highly would rank it near one that
#: actually reads the page.
_ANSWER_POINTS = 20.0
_RECALL_POINTS = 50.0
_PRECISION_POINTS = 30.0


def score_page(expected: set[str], got: set[str]) -> float:
    if not expected:
        return 0.0
    recall = len(expected & got) / len(expected)
    precision = (len(expected & got) / len(got)) if got else 0.0
    return _ANSWER_POINTS + _RECALL_POINTS * recall + _PRECISION_POINTS * precision


def golden_set(db_path: Path, limit: int = 12) -> list[dict]:
    """Cached pages plus the community names we believe they contain.

    Only pages that yielded at least one community are used: a page with zero
    expected results cannot separate a careful model from one that always
    answers "nothing here".
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"no database at {db_path}")
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


async def score_model(extractor, pages: list[dict]) -> dict:
    """Run one model over the golden set. Unanswered pages score zero — an
    unreliable model is a worse model, not an unmeasured one."""
    total, answered, failed, errors = 0.0, 0, 0, []
    for page in pages:
        try:
            records = await extractor.extract(
                text=page["text"], city=page["city"], topic=page["topic"],
                locale="hu", source_url=page["url"],
            )
        except Exception as exc:
            failed += 1
            if len(errors) < 3:
                errors.append(f"{type(exc).__name__}: {str(exc)[:120]}")
            continue
        got = {normalized_match_key(r.name) for r in records if r.name}
        total += score_page(page["expected"], got)
        answered += 1
    return {
        "provider": getattr(extractor, "provider", "?"),
        "model": getattr(extractor, "model", "?"),
        "prior": getattr(extractor, "quality", 0),
        "score": round(total / len(pages)) if pages else 0,
        "answered": answered,
        "failed": failed,
        "errors": errors,
    }


async def score_fleet(db_path: Path, extractors: list, pages: int = 8) -> dict:
    """Score every extractor over one shared golden set."""
    gs = golden_set(db_path, limit=pages)
    if not gs:
        return {"error": "no usable golden pages (need cached pages with records)",
                "pages": 0, "results": []}
    log.info("scoring_start", pages=len(gs), models=len(extractors))
    results = []
    for ex in extractors:
        r = await score_model(ex, gs)
        log.info("scoring_model_done", **{k: r[k] for k in
                                          ("provider", "model", "score", "answered", "failed")})
        results.append(r)
    results.sort(key=lambda r: -r["score"])
    return {
        "pages": len(gs),
        "expected_communities": sum(len(p["expected"]) for p in gs),
        "results": results,
        "note": ("Scores measure agreement with the incumbent extraction, not "
                 "ground truth: expected names come from whichever model "
                 "processed each page before."),
    }
