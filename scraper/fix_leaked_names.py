"""One-off: strip leaked JSON artifacts from community name fields in the DB.

Run on the server:
  docker exec <container> python -m scraper.fix_leaked_names
"""
import json
import re
import sqlite3
from pathlib import Path

_PAT = re.compile(r'["”]\s*,\s*(?:\d[\d.]*\s*[",]|true\b|false\b).*$', re.DOTALL)

DB = Path(__file__).parent.parent / "data" / "scraper.db"


def _clean(name: str) -> str:
    return _PAT.sub("", name).strip(' ",')


def main() -> None:
    if not DB.exists():
        print(f"DB not found at {DB}")
        return

    conn = sqlite3.connect(DB)
    rows = conn.execute("SELECT record_key, data FROM communities").fetchall()
    fixed = 0
    for key, data_json in rows:
        data = json.loads(data_json)
        name = data.get("name", "")
        cleaned = _clean(name)
        if cleaned != name:
            data["name"] = cleaned
            conn.execute(
                "UPDATE communities SET data=? WHERE record_key=?",
                (json.dumps(data, ensure_ascii=False), key),
            )
            print(f"  fixed: {name[:80]!r}")
            print(f"      -> {cleaned!r}")
            fixed += 1

    conn.commit()
    conn.close()
    print(f"\nDone: {fixed} / {len(rows)} records fixed.")


if __name__ == "__main__":
    main()
