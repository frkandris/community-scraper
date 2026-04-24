import json
import re
from datetime import datetime, timezone
from pathlib import Path

import structlog

from .models import CommunityRecord, RunMetadata

log = structlog.get_logger()


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _record_key(r: CommunityRecord) -> str:
    return f"{_normalize(r.name)}|{_normalize(r.city)}|{_normalize(r.topic)}"


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

    ordered = sorted(merged.values(), key=lambda r: r.name)
    output_file.write_text(
        json.dumps([r.model_dump() for r in ordered], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("saved_results", city=city, topic=topic, total=len(ordered), new=len(records))
    return len(ordered)


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
