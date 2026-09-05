#!/usr/bin/env python3
"""Extract a portable golden-set database from the production one.

Why this exists
---------------
`scraper/scoring.py` measures a model against pages whose answer we already
hold, and those pages live only in the production database — 8.7 GB of it, on a
server whose only shell is a browser terminal. Neither half of that is movable:
the file is too large to copy off, and a terminal that wraps pasted input cannot
receive a script long enough to slim it in place. So the slimming lives here, in
the image, where running it is one short command.

What it copies, and why exactly this
------------------------------------
Scoring reads two tables and nothing else:

  golden_set()   cache_pages, ordered by url_hash, first rows with extracted_at
  corpus_names() communities, to decide which name tokens are too generic to
                 identify a club

The `ORDER BY url_hash` is what makes the golden set deterministic — a sample
that moves between runs makes scores incomparable, and silently so, because the
numbers still look like numbers. Copying the url_hash *prefix* therefore
reproduces the identical sample the full database would yield, which is the
whole point: a score measured on the copy stays comparable with the `quality:`
values in config/providers.yaml.

`communities` is copied whole (names and cities only). Genericness is a property
of the corpus — "sakk" appears in every chess club's name — and a dozen sample
pages cannot show that; the full table can.

Nothing else comes along. No source URLs beyond the sampled pages, no run
history, no provider usage.

Usage
-----
    python3 /app/scripts/make_golden_db.py                  # -> /tmp/golden.db.gz
    python3 /app/scripts/make_golden_db.py OUT.gz [PAGES]
"""
from __future__ import annotations

import gzip
import os
import shutil
import sqlite3
import sys
import tempfile

SRC = "/app/data/scraper.db"
#: 400, for a golden set of 12. `golden_set()` reads `limit * 8` rows and then
#: discards those with no cached text or no extracted records, so the prefix has
#: to be comfortably longer than the sample it must yield.
DEFAULT_PAGES = 400


def main() -> int:
    out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/golden.db.gz"
    pages = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PAGES
    src_path = os.environ.get("GOLDEN_SRC", SRC)

    # Read-only: this runs against the live database of a running app.
    src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
    tmp = tempfile.mktemp(suffix=".db")
    dst = sqlite3.connect(tmp)
    dst.executescript(
        "CREATE TABLE cache_pages (url_hash TEXT PRIMARY KEY, url TEXT,"
        " city TEXT, topic TEXT, extracted_at TEXT, data TEXT);"
        "CREATE TABLE communities (record_key TEXT PRIMARY KEY, city TEXT,"
        " data TEXT, hidden INTEGER DEFAULT 0);"
    )
    rows = src.execute(
        "SELECT url_hash, url, city, topic, extracted_at, data"
        "  FROM cache_pages WHERE extracted_at IS NOT NULL"
        " ORDER BY url_hash LIMIT ?", (pages,)).fetchall()
    dst.executemany("INSERT INTO cache_pages VALUES (?,?,?,?,?,?)", rows)

    # json_object rather than the stored blob: corpus_names() reads only
    # $.name, and the rest of a community record is not needed to decide
    # whether a token is generic.
    comm = src.execute(
        "SELECT record_key, city,"
        " json_object('name', json_extract(data, '$.name')), hidden"
        "  FROM communities WHERE hidden=0 ORDER BY record_key").fetchall()
    dst.executemany("INSERT INTO communities VALUES (?,?,?,?)", comm)
    dst.commit()
    dst.close()
    src.close()

    with open(tmp, "rb") as f, gzip.open(out, "wb") as g:
        shutil.copyfileobj(f, g)
    os.remove(tmp)
    print(f"pages={len(rows)} communities={len(comm)} "
          f"-> {out} ({os.path.getsize(out) / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
