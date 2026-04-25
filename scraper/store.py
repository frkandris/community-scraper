import json
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import structlog

from .models import CommunityRecord, RunMetadata

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
    data_dir: Path,
) -> int:
    city_dir = data_dir / _normalize(city) / _normalize(topic)
    city_dir.mkdir(parents=True, exist_ok=True)
    output_file = city_dir / "communities.json"

    existing: list[CommunityRecord] = []
    if output_file.exists():
        try:
            raw = json.loads(output_file.read_text(encoding="utf-8"))
            existing = [CommunityRecord.model_validate(item) for item in raw]
        except Exception as exc:
            log.warning("failed_to_load_existing", path=str(output_file), error=str(exc))

    merged: dict[str, CommunityRecord] = {_record_key(r): r for r in existing}
    for r in records:
        merged[_record_key(r)] = r

    deduped = _dedup(sorted(merged.values(), key=lambda r: r.name))
    output_file.write_text(
        json.dumps([r.model_dump() for r in deduped], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    before = len(merged)
    log.info("saved_results", city=city, topic=topic, total=len(deduped),
             new=len(records), deduped=before - len(deduped))
    return len(deduped)


def update_metadata(
    run_stats: dict[str, dict[str, int]],
    data_dir: Path,
) -> None:
    metadata_file = data_dir / "metadata.json"
    total = sum(count for topics in run_stats.values() for count in topics.values())
    metadata = RunMetadata(
        last_run=datetime.now(timezone.utc).isoformat(),
        records_by_city_topic=run_stats,
        total_records=total,
    )
    metadata_file.write_text(
        json.dumps(metadata.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
