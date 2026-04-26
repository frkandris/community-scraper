"""
Migrate existing JSON file data into SQLite.

Usage (on the server, inside the container):
    python -m scraper.migrate_json

Safe to run multiple times — uses upsert (ON CONFLICT DO UPDATE).
"""

import json
from pathlib import Path

import structlog

from .db import (
    append_prompt_history,
    bulk_upsert_communities,
    get_prompt_history,
    init_db,
    save_cache_page,
    upsert_false_positive,
)
from .models import CommunityRecord

log = structlog.get_logger()

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "scraper.db"


def migrate_communities() -> int:
    total = 0
    for json_file in sorted(DATA_DIR.glob("*/*/communities.json")):
        parts = json_file.parts
        city_norm  = parts[-3]
        topic_norm = parts[-2]
        try:
            raw = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("skip_communities", file=str(json_file), error=str(exc))
            continue
        records = []
        for item in raw:
            try:
                r = CommunityRecord.model_validate(item)
                records.append(r.model_dump())
            except Exception as exc:
                log.warning("skip_record", file=str(json_file), error=str(exc))
        if records:
            bulk_upsert_communities(DB_PATH, records)
            log.info("communities_imported", file=str(json_file), count=len(records))
            total += len(records)
    return total


def migrate_cache() -> int:
    cache_dir = DATA_DIR / "cache" / "pages"
    if not cache_dir.exists():
        log.info("cache_dir_not_found", path=str(cache_dir))
        return 0
    total = 0
    for json_file in sorted(cache_dir.glob("*.json")):
        try:
            entry = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("skip_cache_entry", file=str(json_file), error=str(exc))
            continue
        if not entry.get("url_hash"):
            entry["url_hash"] = json_file.stem
        save_cache_page(DB_PATH, entry)
        total += 1
    log.info("cache_imported", count=total)
    return total


def migrate_false_positives() -> int:
    fp_file = DATA_DIR / "false_positives.json"
    if not fp_file.exists():
        log.info("fp_file_not_found")
        return 0
    try:
        fps = json.loads(fp_file.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("skip_false_positives", error=str(exc))
        return 0
    for fp in fps:
        upsert_false_positive(
            DB_PATH,
            name=fp["name"],
            city=fp["city"],
            topic=fp["topic"],
            reason=fp.get("reason", ""),
            source_url=fp.get("source_url", ""),
            fp_type=fp.get("fp_type", "extraction"),
        )
    log.info("fps_imported", count=len(fps))
    return len(fps)


def migrate_prompt_history() -> int:
    total = 0
    for fp_type in ("extraction", "enrichment"):
        hist_file = DATA_DIR / f"prompt_history_{fp_type}.json"
        if not hist_file.exists():
            continue
        try:
            history = json.loads(hist_file.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("skip_prompt_history", fp_type=fp_type, error=str(exc))
            continue
        existing = get_prompt_history(DB_PATH, fp_type)
        existing_versions = {e["version"] for e in existing}
        added = 0
        for entry in history:
            if entry["version"] in existing_versions:
                continue
            append_prompt_history(
                DB_PATH,
                version=entry["version"],
                timestamp=entry["timestamp"],
                content=entry["content"],
                fp_type=fp_type,
                fp_count=entry.get("fp_count", 0),
            )
            added += 1
        log.info("prompt_history_imported", fp_type=fp_type, added=added, skipped=len(existing_versions))
        total += added
    return total


def main() -> None:
    structlog.configure(processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ])

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db(DB_PATH)
    log.info("migration_start", db=str(DB_PATH))

    c = migrate_communities()
    p = migrate_cache()
    f = migrate_false_positives()
    h = migrate_prompt_history()

    log.info("migration_done",
             communities=c, cache_pages=p,
             false_positives=f, prompt_history_entries=h)


if __name__ == "__main__":
    main()
