import re
from difflib import SequenceMatcher
from pathlib import Path

import structlog

from .db import bulk_upsert_communities, delete_communities_for_topic, get_communities
from .models import CommunityRecord

log = structlog.get_logger()


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _record_key(r: CommunityRecord) -> str:
    return f"{_normalize(r.name)}|{_normalize(r.city)}|{_normalize(r.topic)}"


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

    # Replace all records for this city/topic atomically
    delete_communities_for_topic(db_path, city, topic)
    bulk_upsert_communities(db_path, [r.model_dump() for r in deduped])

    before = len(merged)
    log.info("saved_results", city=city, topic=topic, total=len(deduped),
             new=len(records), deduped=before - len(deduped))
    return len(deduped)


def update_metadata(run_stats: dict, db_path: Path) -> None:
    pass
