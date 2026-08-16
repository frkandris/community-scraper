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
import re
import unicodedata
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


def _key(name: str) -> str:
    """Exact-identity key — the same collapse the database uses."""
    return normalized_match_key(name)


def _tokens(name: str) -> set[str]:
    """Word tokens for fuzzy comparison.

    Deliberately not `normalized_match_key`, which strips spaces entirely
    ("Szentendrei Futóklub" -> "szentendreifutóklub") and therefore makes token
    overlap impossible. Filler words that appear in half the club names in the
    corpus carry no identifying information and are dropped.
    """
    folded = "".join(
        c for c in unicodedata.normalize("NFD", name.lower())
        if unicodedata.category(c) != "Mn"
    )
    return {t for t in re.split(r"[^a-z0-9]+", folded)
            if len(t) > 2 and t not in _STOPWORDS}


#: Words too common in Hungarian club names to identify anything on their own.
_STOPWORDS = frozenset({
    "egyesulet", "klub", "kor", "csoport", "sport", "sportegyesulet",
    "kozhasznu", "alapitvany", "tarsasag", "szakosztaly", "es", "az",
})


def _matches(want: str, got: list[str]) -> bool:
    """Did the model return this name, under any reasonable reading?

    Exact matching alone under-counts badly: MINEA (arXiv:2404.04068) scored the
    same extractions at 59.4% on exact name match and 88.4% once a containment
    match was allowed. A model returning "Szentendrei Futóklub Egyesület" where
    we recorded "Szentendrei Futóklub" is right, and scoring it as a miss
    measures phrasing rather than extraction.

    Three strategies, most conservative first — MINEA's escalation minus the LLM
    judge, which would cost more calls than the measurement itself.
    """
    wk = _key(want)
    keys = [_key(g) for g in got]
    if wk in keys:
        return True
    if any(wk and k and (wk in k or k in wk) for k in keys):
        return True
    wt = _tokens(want)
    if not wt:
        return False
    for g in got:
        gt = _tokens(g)
        if not gt:
            continue
        shared = len(wt & gt)
        # Two-thirds of the shorter name's distinctive tokens, rounded up.
        need = (2 * min(len(wt), len(gt)) + 2) // 3
        if shared >= max(1, need):
            return True
    return False


def score_page(expected, got) -> float:
    """Score one page. Both arguments are raw names, not identity keys —
    matching needs the words, and the key form has no spaces."""
    expected, got = list(expected), list(got)
    if not expected:
        return 0.0
    recall = sum(1 for w in expected if _matches(w, got)) / len(expected)
    # Precision is judged with the same tolerance as recall, so a model is not
    # punished for phrasing differences that recall forgives.
    precision = (sum(1 for g in got if _matches(g, expected)) / len(got)) if got else 0.0
    return (_ANSWER_POINTS + _RECALL_POINTS * recall
            + _PRECISION_POINTS * min(1.0, precision))


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
        expected = [r["name"] for r in entry.get("records") or [] if r.get("name")]
        if not text or not expected:
            continue
        out.append({"url": url, "city": city or "", "topic": topic or "",
                    "text": text, "expected": expected})
        if len(out) >= limit:
            break
    return out


async def score_model(extractor, pages: list[dict]) -> dict:
    """Run one model over the golden set.

    The score is the mean over pages the model *answered*, with `coverage`
    reporting how many that was. Averaging over all pages instead would fold
    reliability into quality, and at free-tier rate limits that mostly measures
    how recently the fleet ran, not how well the model reads a page.
    """
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
        got = [r.name for r in records if r.name]
        total += score_page(page["expected"], got)
        answered += 1
    # A model that never answered is UNMEASURED, not bad. Reporting 0 conflates
    # "rate limited for 20 minutes" with "produced garbage", and writing that 0
    # into providers.yaml would drop a good model to the bottom of the routing
    # order — seen on the very first live run, where two of three Groq models
    # scored 0 purely because of a rate limit and an HTTP 400.
    measured = answered > 0
    return {
        "provider": getattr(extractor, "provider", "?"),
        "model": getattr(extractor, "model", "?"),
        "prior": getattr(extractor, "quality", 0),
        "score": round(total / answered) if measured else None,
        "coverage": round(answered / len(pages), 2) if pages else 0.0,
        "answered": answered,
        "failed": failed,
        "measured": measured,
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
    # Measured first (best score first), then the unmeasured — which are last
    # because they are unknown, not because they are bad.
    results.sort(key=lambda r: (0 if r["measured"] else 1, -(r["score"] or 0)))
    unmeasured = [f"{r['provider']}:{r['model']}" for r in results if not r["measured"]]
    return {
        "pages": len(gs),
        "expected_communities": sum(len(p["expected"]) for p in gs),
        "results": results,
        "unmeasured": unmeasured,
        "note": ("Scores measure agreement with the incumbent extraction, not "
                 "ground truth: expected names come from whichever model "
                 "processed each page before. score=null means the model could "
                 "not be measured (rate limit, error) — NOT that it scored "
                 "zero; never write a null into providers.yaml."),
    }
