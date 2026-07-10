import re
from difflib import SequenceMatcher
from pathlib import Path

import structlog

from .db import _community_record_key, get_communities, replace_communities_for_topic
from .duplicates import detect_community_candidates
from .models import CommunityRecord

log = structlog.get_logger()


def _record_key(r: CommunityRecord) -> str:
    return _community_record_key(r.name, r.city, r.topic)


def _strip_articles(name: str) -> str:
    return re.sub(r"^(a |az |the |die |le |la |el |los |las )", "", name.lower().strip())


def _richness(r: CommunityRecord) -> int:
    return sum(1 for f in [r.description, r.meeting_schedule, r.location, r.contact, r.website] if f) + len(r.social_links)


def _is_duplicate(a: CommunityRecord, b: CommunityRecord) -> bool:
    if a.website and b.website and a.website.rstrip("/") == b.website.rstrip("/"):
        return True
    an = _strip_articles(a.name)
    bn = _strip_articles(b.name)
    if an and bn and (an in bn or bn in an):
        return True
    if an and bn and SequenceMatcher(None, an, bn).ratio() > 0.88:
        return True
    return False


def _dedup(records: list[CommunityRecord]) -> list[CommunityRecord]:
    result: list[CommunityRecord] = []
    for r in records:
        idx = next((i for i, e in enumerate(result) if _is_duplicate(r, e)), None)
        if idx is not None:
            if _richness(r) > _richness(result[idx]):
                result[idx] = r
        else:
            result.append(r)
    return result


_PATCHABLE_FIELDS = [
    "description", "history", "tags", "meeting_schedule", "location",
    "fee", "age_range", "skill_level", "join_process", "leader",
    "language", "frequency", "founding_year", "member_count",
    "email", "phone", "website", "contact",
]


def _patch_record(existing: CommunityRecord, new: CommunityRecord) -> CommunityRecord:
    """Return existing with null fields filled from new. Non-null fields are never overwritten."""
    data = existing.model_dump()
    patch = new.model_dump()
    for field in _PATCHABLE_FIELDS:
        if not data.get(field) and patch.get(field):
            data[field] = patch[field]
    existing_links = set(data.get("social_links") or [])
    for link in patch.get("social_links") or []:
        existing_links.add(link)
    data["social_links"] = sorted(existing_links)
    return CommunityRecord.model_validate(data)


def patch_results(
    city: str,
    topic: str,
    new_records: list[CommunityRecord],
    db_path: Path,
) -> int:
    """Fill in null fields on existing communities from new_records. Never overwrites non-null values."""
    existing_data = get_communities(db_path, city, topic)
    existing: dict[str, CommunityRecord] = {}
    for item in existing_data:
        try:
            r = CommunityRecord.model_validate(item)
            existing[_record_key(r)] = r
        except Exception:
            pass

    patched = 0
    for new in new_records:
        key = _record_key(new)
        if key in existing:
            existing[key] = _patch_record(existing[key], new)
            patched += 1

    deduped = _dedup(sorted(existing.values(), key=lambda r: r.name))
    replace_communities_for_topic(db_path, city, topic, [r.model_dump() for r in deduped])
    log.info("patch_results", city=city, topic=topic, patched=patched)
    return patched


def save_results(
    city: str,
    topic: str,
    records: list[CommunityRecord],
    db_path: Path,
) -> int:
    existing_data = get_communities(db_path, city, topic)
    existing: list[CommunityRecord] = []
    for item in existing_data:
        try:
            existing.append(CommunityRecord.model_validate(item))
        except Exception as exc:
            log.warning("failed_to_load_existing_record", error=str(exc))

    merged: dict[str, CommunityRecord] = {_record_key(r): r for r in existing}
    for r in records:
        merged[_record_key(r)] = r

    deduped = _dedup(sorted(merged.values(), key=lambda r: r.name))

    replace_communities_for_topic(db_path, city, topic, [r.model_dump() for r in deduped])

    before = len(merged)
    log.info("saved_results", city=city, topic=topic, total=len(deduped),
             new=len(records), deduped=before - len(deduped))
    try:
        detect_community_candidates(db_path, city=city)
    except Exception as exc:
        log.warning("duplicate_detection_failed", error=str(exc))
    return len(deduped)
