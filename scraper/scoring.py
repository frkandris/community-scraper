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


#: Words that appear in a large fraction of club names and identify nothing on
#: their own. Multilingual because the corpus is: Hungarian, German and Swedish
#: names all reach the scorer, and a Hungarian-only list leaves "Sportverein"
#: and "Idrottsförening" counting as distinctive.
_STOPWORDS = frozenset({
    # Hungarian
    "egyesulet", "klub", "kor", "csoport", "sport", "sportegyesulet",
    "kozhasznu", "alapitvany", "tarsasag", "szakosztaly", "se", "sc", "es", "az",
    # German
    "verein", "sportverein", "turnverein", "abteilung", "gemeinschaft",
    "gruppe", "der", "die", "das", "und", "ev",
    # Swedish
    "forening", "klubb", "idrottsforening", "sallskap", "och",
    # English
    "club", "society", "association", "group", "the", "and",
})

#: Shortest identity key that may be used for a containment match. Below this a
#: key is almost always a generic word ("klub", "se", "sport") that is a
#: substring of half the corpus.
_MIN_CONTAINMENT_LEN = 8


def _matches(want: str, got: list[str]) -> bool:
    """Did the model return this name, under any reasonable reading?

    Exact matching alone under-counts badly: MINEA (arXiv:2404.04068) scored the
    same extractions at 59.4% on exact name match and 88.4% once containment was
    allowed. A model returning "Szentendrei Futóklub Egyesület" where we
    recorded "Szentendrei Futóklub" is right.

    The hard part is being lenient about *phrasing* without becoming lenient
    about *identity*. Two rules do that work:

    * containment requires a substantial key on both sides, so "Klub" no longer
      matches "Szentendrei Futóklub" merely by being a substring of it;
    * token overlap requires one name's distinctive tokens to be a **subset** of
      the other's, plus at least two shared tokens. Without the subset rule,
      "SV Grün-Weiß Musterstadt" matches "SV Grün-Weiß Beispielstadt" — same
      club name, different town, which is the commonest shape in the German
      corpus. Without the two-token floor, the bare topic word "Sakk" matches
      every chess club and scores 100.
    """
    wk = _key(want)
    keys = [_key(g) for g in got]
    if wk in keys:
        return True

    wt = _tokens(want)
    for g, k in zip(got, keys):
        if not wk or not k:
            continue
        # Containment, guarded by length and by carrying real content.
        if (wk in k or k in wk) and min(len(wk), len(k)) >= _MIN_CONTAINMENT_LEN:
            shorter_tokens = wt if len(wk) <= len(k) else _tokens(g)
            if shorter_tokens:
                return True
        gt = _tokens(g)
        if not wt or not gt:
            continue
        shared = wt & gt
        # Subset either way: one name may be more specific, but neither may
        # carry a distinctive token the other contradicts.
        if len(shared) >= 2 and (wt <= gt or gt <= wt):
            return True
    return False


def _pair_up(left: list[str], right: list[str]) -> int:
    """Greedy one-to-one matching; returns how many pairs were made.

    Counting matches independently lets one name satisfy several: three
    phrasings of the same club all counted as precise, and a single generic
    answer counted as recall for every expected club. Each side may be used
    once.
    """
    used: set[int] = set()
    pairs = 0
    for item in left:
        for i, candidate in enumerate(right):
            if i in used:
                continue
            if _matches(item, [candidate]):
                used.add(i)
                pairs += 1
                break
    return pairs


def score_page(expected, got) -> float:
    """Score one page. Both arguments are raw names, not identity keys —
    matching needs the words, and the key form has no spaces."""
    expected, got = list(expected), list(got)
    if not expected:
        return 0.0
    # One-to-one on both sides: a model cannot earn recall for two clubs with a
    # single answer, nor precision for repeating one club three ways.
    recall = _pair_up(expected, got) / len(expected)
    precision = (_pair_up(got, expected) / len(got)) if got else 0.0
    return (_ANSWER_POINTS + _RECALL_POINTS * recall
            + _PRECISION_POINTS * min(1.0, precision))


def golden_set(db_path: Path, limit: int = 12) -> list[dict]:
    """Cached pages plus the community names we believe they contain.

    Only pages that yielded at least one community are used: a page with zero
    expected results cannot separate a careful model from one that always
    answers "nothing here".

    The sample is **deterministic** (ordered by url_hash). A sample that moves
    between runs makes scores incomparable, and silently so — the numbers still
    look like numbers.
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"no database at {db_path}")
    out: list[dict] = []
    with _connect(db_path) as conn:
        # ORDER BY url_hash, NOT extracted_at: the pipeline rewrites
        # extracted_at continuously, so a "most recently extracted" sample is a
        # different set of pages on every run. Two measurements an hour apart
        # then differ for reasons that have nothing to do with the models —
        # which is exactly what happened on 2026-08-16, where mistral-small
        # appeared to fall 80 -> 55 between runs. url_hash is stable, so the
        # sample only drifts as the corpus itself grows.
        rows = conn.execute(
            """
            SELECT url, city, topic, data
              FROM cache_pages
             WHERE extracted_at IS NOT NULL
             ORDER BY url_hash
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
    import hashlib
    # Identifies the sample. Two runs with different fingerprints measured
    # different pages and their scores are not comparable.
    sample_fp = hashlib.sha256(
        "|".join(sorted(p["url"] for p in gs)).encode()).hexdigest()[:12]
    return {
        "pages": len(gs),
        "sample": sample_fp,
        # Deduplicated: a cached extraction can hold the same name twice, and
        # counting it twice overstates how much the sample actually covers.
        "expected_communities": sum(len({_key(n) for n in p["expected"]}) for p in gs),
        "results": results,
        "unmeasured": unmeasured,
        "note": ("Scores measure agreement with the incumbent extraction, not "
                 "ground truth: expected names come from whichever model "
                 "processed each page before. score=null means the model could "
                 "not be measured (rate limit, error) — NOT that it scored "
                 "zero; never write a null into providers.yaml."),
    }
