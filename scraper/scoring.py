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
    # Tokens of one or two characters are club-type markers, not names — SV, IF,
    # FC, SE. Dropping them here means "SV Musterstadt" and "Sportverein
    # Musterstadt" both reduce to the town, which is the point.
    # Genericness filtering is the caller's job: it depends on the corpus.
    return {t for t in re.split(r"[^a-z0-9]+", folded) if len(t) > 2}


#: A token is "generic" when it appears in at least this share of the corpus's
#: names. Measured, not listed: a hand-written stopword list cannot keep up with
#: three languages, and length is not a proxy for genericness — Hungarian
#: generics are short ("klub", "SE") while German ones are long compounds
#: ("Schachverein", "Idrottssällskap"). Both must be caught by the same rule.
_GENERIC_DF = 0.10

#: Endings that make a word a club-type rather than a name. Compound languages
#: build these productively — Schachverein, Schwimmverein, Turnverein,
#: Simklubb, Idrottsförening — so no finite list of whole words keeps up, but
#: the suffix does. "Futóklub" also matches, which is correct: it is the club
#: type, and "Szentendrei" is what identifies the club.
_GENERIC_SUFFIXES = (
    "verein", "vereinigung", "gemeinschaft",
    "klubb", "forening", "sallskap", "idrottsforening",
    "egyesulet", "klub", "club",
)

#: Fallback for the tiny-corpus case, where document frequency is meaningless.
_SEED_GENERIC = frozenset({
    "egyesulet", "klub", "kor", "csoport", "sport", "sportegyesulet", "se", "sc",
    "verein", "sportverein", "gruppe", "ev", "forening", "klubb", "club",
    "society", "association", "group",
})


def _generic_tokens(names: list[str], threshold: float = _GENERIC_DF) -> frozenset[str]:
    """Tokens too widespread in this corpus to identify a club.

    Derived from the names themselves, so it adapts to whichever market the
    sample is drawn from without anyone maintaining a list per language.
    """
    from collections import Counter

    if len(names) < 20:
        return frozenset(_SEED_GENERIC)
    df: Counter = Counter()
    for n in names:
        df.update(_tokens(n))
    cutoff = max(2, int(len(names) * threshold))
    return frozenset(_SEED_GENERIC | {t for t, c in df.items() if c >= cutoff})


def _matches(want: str, got: list[str], generic: frozenset[str] = frozenset()) -> bool:
    """Did the model return this name, under any reasonable reading?

    Lenient about *phrasing*, strict about *identity* — the two are easy to
    conflate and each mistake is costly in its own direction. MINEA
    (arXiv:2404.04068) measured the same extractions at 59.4% with exact name
    matching and 88.4% once containment was allowed, so being too strict
    understates every model; being too loose lets a single generic word sweep a
    page and promote a degenerate model to the head of the routing order.

    The rule is about *distinctive* tokens — the ones left after removing words
    that are widespread in this corpus:

    * identical distinctive sets match, whatever their size. "SV Musterstadt"
      and "Sportverein Musterstadt" both reduce to {musterstadt}: same club,
      spelled out. This is the dominant German shape, so a size floor here
      wrecks measurement on the largest market.
    * a subset matches only if the smaller side still carries a distinctive
      token, which is what stops bare "Schachverein" or "Klub".
    * two names that each carry a distinctive token the other lacks are
      different clubs — "SV Grün-Weiß Musterstadt" vs "… Beispielstadt".
    """
    generic = generic or _SEED_GENERIC

    def distinctive(name: str) -> set[str]:
        return {t for t in _tokens(name)
                if t not in generic and not t.endswith(_GENERIC_SUFFIXES)}

    wk = _key(want)
    if wk in [_key(g) for g in got]:
        return True

    wt = distinctive(want)
    for g in got:
        gt = distinctive(g)
        if not wt or not gt:
            continue  # nothing distinctive on one side — cannot confirm identity
        if wt == gt:
            return True
        if wt <= gt or gt <= wt:
            return True
    return False


def _pair_up(left: list[str], right: list[str],
             generic: frozenset[str] = frozenset()) -> int:
    """Maximum one-to-one matching between two name lists.

    Greedy pairing consumes a candidate a later item needed, so the same answer
    set scored differently depending on the order the model happened to list
    clubs in — which reintroduces exactly the run-to-run variance the
    deterministic golden set removed. Augmenting paths (Kuhn's algorithm) give
    the true maximum; n is a handful of names per page.
    """
    adj = [[j for j, cand in enumerate(right) if _matches(item, [cand], generic)]
           for item in left]
    match_r: dict[int, int] = {}

    def _try(i: int, seen: set[int]) -> bool:
        for j in adj[i]:
            if j in seen:
                continue
            seen.add(j)
            if j not in match_r or _try(match_r[j], seen):
                match_r[j] = i
                return True
        return False

    return sum(1 for i in range(len(left)) if _try(i, set()))


def score_page(expected, got, generic: frozenset[str] = frozenset()) -> float:
    """Score one page. Both arguments are raw names, not identity keys —
    matching needs the words, and the key form has no spaces.

    `expected` is deduplicated: a cached extraction can hold the same club
    twice, and with one-to-one pairing that would cap recall below 1.0 for a
    model returning the correct *distinct* set — scoring it below one that
    repeats itself.
    """
    seen: dict[str, str] = {}
    for n in expected:
        seen.setdefault(_key(n), n)
    expected = list(seen.values())
    got = list(dict.fromkeys(got))
    if not expected:
        return 0.0
    recall = _pair_up(expected, got, generic) / len(expected)
    precision = (_pair_up(got, expected, generic) / len(got)) if got else 0.0
    return (_ANSWER_POINTS + _RECALL_POINTS * recall
            + _PRECISION_POINTS * min(1.0, precision))


def corpus_names(db_path: Path, limit: int = 40_000) -> list[str]:
    """Every visible community name, for measuring which tokens are generic.

    Deliberately the full corpus rather than the golden set: genericness is a
    property of the domain ("sakk" appears in every chess club's name), and a
    dozen sample pages cannot show that.
    """
    if not Path(db_path).exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT json_extract(data, '$.name') FROM communities"
            " WHERE hidden=0 LIMIT ?", (limit,)).fetchall()
    return [r[0] for r in rows if r[0]]


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


async def score_model(extractor, pages: list[dict],
                      generic: frozenset[str] = frozenset()) -> dict:
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
        total += score_page(page["expected"], got, generic)
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
    # Genericness is measured from the WHOLE corpus, not the sample. A golden
    # set is a dozen pages and a few dozen names — far too small for document
    # frequency to mean anything, and the topic word ("sakk", "futás") would
    # never look common enough to discount. The communities table has tens of
    # thousands of names and answers the question properly.
    generic = _generic_tokens(corpus_names(db_path))
    log.info("scoring_start", pages=len(gs), models=len(extractors),
             generic_tokens=len(generic))
    results = []
    for ex in extractors:
        r = await score_model(ex, gs, generic)
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
