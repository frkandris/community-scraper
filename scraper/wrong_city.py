"""Wrong-city detection: flag communities whose text mentions another known city.

A community filed under city A whose description (or other text field) mentions
city B is a strong signal the record landed in the wrong city — e.g. a
"Szentendre" seniors club whose description talks about Szentgotthárd.
Candidates go to `wrong_city_candidates` for admin review at /admin/wrong-city.
"""
from __future__ import annotations

import re
from pathlib import Path

import structlog

from .db import (
    _community_record_key,
    get_all_communities,
    get_community_by_record_key,
    get_wrong_city_candidates,
    insert_wrong_city_candidate,
    resolve_wrong_city_candidate,
)

log = structlog.get_logger()

# Text fields scanned for foreign city mentions.
SCANNED_FIELDS = (
    "name", "description", "location", "meeting_schedule",
    "contact", "history", "join_process",
)

_SNIPPET_RADIUS = 60


def _build_city_pattern(cities: list[str]) -> re.Pattern | None:
    """One alternation over all city names, longest first so 'Vácrátót' wins
    over 'Vác'. A short inflection tail (max 4 word chars) covers Hungarian
    suffix forms: 'Szentgotthárdon', 'szentgotthárdi', 'Szegedről'."""
    names = sorted({c.strip() for c in cities if c and len(c.strip()) >= 3},
                   key=len, reverse=True)
    if not names:
        return None
    alternation = "|".join(re.escape(n) for n in names)
    return re.compile(rf"(?<!\w)({alternation})\w{{0,4}}(?!\w)",
                      re.IGNORECASE | re.UNICODE)


def _is_own_city(mentioned: str, own: str) -> bool:
    """True when the mention is the community's own city or a prefix relative
    of it (e.g. own 'Vácrátót' vs mention 'Vác' — the tail already matched)."""
    m, o = mentioned.lower(), own.lower()
    return m == o or m.startswith(o) or o.startswith(m)


def _snippet(text: str, start: int, end: int) -> str:
    lo = max(0, start - _SNIPPET_RADIUS)
    hi = min(len(text), end + _SNIPPET_RADIUS)
    prefix = "…" if lo > 0 else ""
    suffix = "…" if hi < len(text) else ""
    return f"{prefix}{text[lo:hi].strip()}{suffix}"


def detect_wrong_city_candidates(db_path: Path, cities: list[str]) -> int:
    """Scan all visible communities for mentions of another known city in
    their text fields. Returns the number of new candidates inserted."""
    pattern = _build_city_pattern(cities)
    if pattern is None:
        return 0
    canonical = {c.lower(): c for c in cities}

    inserted = 0
    for r in get_all_communities(db_path):
        own_city = r.get("city", "")
        if not own_city:
            continue
        record_key = _community_record_key(r.get("name", ""), own_city, r.get("topic", ""))
        flagged: set[str] = set()
        for field in SCANNED_FIELDS:
            value = r.get(field)
            if not value or not isinstance(value, str):
                continue
            for m in pattern.finditer(value):
                mentioned = canonical.get(m.group(1).lower(), m.group(1))
                if _is_own_city(mentioned, own_city) or mentioned in flagged:
                    continue
                flagged.add(mentioned)
                if insert_wrong_city_candidate(
                    db_path, record_key, r.get("community_id", ""),
                    mentioned, field,
                    _snippet(value, m.start(), m.end()), m.group(0),
                ):
                    inserted += 1
                    log.info("wrong_city_candidate_found",
                             name=r.get("name"), city=own_city,
                             mentioned=mentioned, field=field)
    return inserted


def cleanup_stale_wrong_city_candidates(db_path: Path) -> int:
    """Auto-dismiss pending candidates whose record is gone, hidden, or no
    longer mentions the flagged city. Returns number dismissed."""
    dismissed = 0
    for c in get_wrong_city_candidates(db_path, resolved=False):
        record = get_community_by_record_key(db_path, c["record_key"])
        stale = record is None
        if record is not None:
            text = " ".join(str(record.get(f) or "") for f in SCANNED_FIELDS)
            stale = c["mentioned_city"].lower() not in text.lower()
        if stale:
            resolve_wrong_city_candidate(db_path, c["id"], "auto_dismissed")
            dismissed += 1
    return dismissed


def scan(db_path: Path, cities: list[str]) -> int:
    """Cleanup then detect. Returns new candidates inserted."""
    cleanup_stale_wrong_city_candidates(db_path)
    count = detect_wrong_city_candidates(db_path, cities)
    log.info("wrong_city_scan_complete", new_candidates=count)
    return count
