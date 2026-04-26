import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _community_record_key(name: str, city: str, topic: str) -> str:
    return f"{_norm(name)}|{_norm(city)}|{_norm(topic)}"


def init_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at  TEXT NOT NULL,
                finished_at TEXT,
                run_mode    TEXT NOT NULL DEFAULT 'full',
                success     INTEGER NOT NULL DEFAULT 1,
                search_log  TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE runs ADD COLUMN search_log TEXT")
        except sqlite3.OperationalError:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                email      TEXT NOT NULL,
                city       TEXT NOT NULL,
                topic      TEXT NOT NULL,
                token      TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_sub_uniq
            ON subscriptions(email, city, topic)
        """)

        # Communities — one row per unique (name, city, topic), full record as JSON
        conn.execute("""
            CREATE TABLE IF NOT EXISTS communities (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                record_key   TEXT NOT NULL UNIQUE,
                community_id TEXT NOT NULL,
                city         TEXT NOT NULL,
                topic        TEXT NOT NULL,
                data         TEXT NOT NULL,
                updated_at   TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_comm_city_topic ON communities(city, topic)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_comm_community_id ON communities(community_id)"
        )

        # Cache pages — full JSON entry per scraped URL
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_pages (
                url_hash            TEXT PRIMARY KEY,
                url                 TEXT NOT NULL,
                city                TEXT NOT NULL DEFAULT '',
                topic               TEXT NOT NULL DEFAULT '',
                domain              TEXT NOT NULL DEFAULT '',
                scraped_at          TEXT,
                extracted_at        TEXT,
                extract_fingerprint TEXT,
                data                TEXT NOT NULL
            )
        """)
        try:
            conn.execute("ALTER TABLE cache_pages ADD COLUMN extract_fingerprint TEXT")
        except sqlite3.OperationalError:
            pass

        # False positives
        conn.execute("""
            CREATE TABLE IF NOT EXISTS false_positives (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                city       TEXT NOT NULL,
                topic      TEXT NOT NULL,
                reason     TEXT,
                source_url TEXT,
                fp_type    TEXT NOT NULL DEFAULT 'extraction',
                marked_at  TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fp_uniq
            ON false_positives(name, city, topic, fp_type)
        """)

        # Prompt version history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prompt_history (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                version   INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                content   TEXT NOT NULL,
                fp_type   TEXT NOT NULL,
                fp_count  INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.commit()


# ── Runs ──────────────────────────────────────────────────────────────────────

def record_run(
    db_path: Path,
    started_at: datetime,
    finished_at: datetime,
    run_mode: str,
    success: bool,
    search_log: str | None = None,
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, finished_at, run_mode, success, search_log) "
            "VALUES (?, ?, ?, ?, ?)",
            (started_at.isoformat(), finished_at.isoformat(),
             run_mode, int(success), search_log),
        )
        conn.commit()
        return cur.lastrowid


def get_last_run(db_path: Path) -> datetime | None:
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT finished_at FROM runs WHERE success=1 ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0])
    except Exception:
        pass
    return None


def get_run_history(db_path: Path, limit: int = 20) -> list[dict]:
    if not db_path.exists():
        return []
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT id, started_at, finished_at, run_mode, success "
                "FROM runs ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {
                "id": r[0],
                "started_at": r[1],
                "finished_at": r[2],
                "run_mode": r[3],
                "success": bool(r[4]),
            }
            for r in rows
        ]
    except Exception:
        return []


def get_run_detail(db_path: Path, run_id: int) -> dict | None:
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT id, started_at, finished_at, run_mode, success, search_log "
                "FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "started_at": row[1],
                "finished_at": row[2],
                "run_mode": row[3],
                "success": bool(row[4]),
                "search_log": row[5],
            }
    except Exception:
        return None


# ── Subscriptions ─────────────────────────────────────────────────────────────

def save_subscription(db_path: Path, email: str, city: str, topic: str) -> str:
    token = str(uuid.uuid4())
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute(
                "INSERT INTO subscriptions (email, city, topic, token, created_at) VALUES (?,?,?,?,?)",
                (email.strip().lower(), city, topic, token, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT token FROM subscriptions WHERE email=? AND city=? AND topic=?",
                (email.strip().lower(), city, topic),
            ).fetchone()
            token = row[0] if row else token
    return token


def delete_subscription(db_path: Path, token: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("DELETE FROM subscriptions WHERE token=?", (token,))
        conn.commit()
        return cur.rowcount > 0


def get_subscriptions(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, email, city, topic, created_at FROM subscriptions ORDER BY id DESC"
        ).fetchall()
    return [{"id": r[0], "email": r[1], "city": r[2], "topic": r[3], "created_at": r[4]}
            for r in rows]


# ── Communities ───────────────────────────────────────────────────────────────

def bulk_upsert_communities(db_path: Path, records: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        for record in records:
            key = _community_record_key(record["name"], record["city"], record["topic"])
            existing = conn.execute(
                "SELECT data FROM communities WHERE record_key=?", (key,)
            ).fetchone()
            if existing:
                existing_data = json.loads(existing[0])
                prev_urls: list[str] = existing_data.get("source_urls") or []
                if existing_data.get("source_url") and existing_data["source_url"] not in prev_urls:
                    prev_urls = [existing_data["source_url"]] + prev_urls
                new_urls: list[str] = record.get("source_urls") or []
                if record.get("source_url") and record["source_url"] not in new_urls:
                    new_urls = [record["source_url"]] + new_urls
                merged = list(dict.fromkeys(new_urls + prev_urls))
                record = {**record, "source_urls": merged}
            conn.execute("""
                INSERT INTO communities (record_key, community_id, city, topic, data, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    data=excluded.data,
                    community_id=excluded.community_id,
                    updated_at=excluded.updated_at
            """, (key, record.get("community_id", ""), record["city"], record["topic"],
                  json.dumps(record, ensure_ascii=False), now))
        conn.commit()


def delete_communities_for_topic(db_path: Path, city: str, topic: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM communities WHERE city=? AND topic=?", (city, topic))
        conn.commit()


def get_communities(db_path: Path, city: str, topic: str) -> list[dict]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? AND topic=? ORDER BY id",
            (city, topic)
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_communities_for_city(db_path: Path, city: str) -> list[dict]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? ORDER BY topic, id",
            (city,)
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def find_community_by_id(db_path: Path, community_id: str) -> dict | None:
    if not db_path.exists():
        return None
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE community_id=? LIMIT 1",
            (community_id,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def get_topic_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT topic, COUNT(*) FROM communities GROUP BY topic"
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_city_topic_counts(db_path: Path) -> dict[str, dict[str, int]]:
    if not db_path.exists():
        return {}
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT city, topic, COUNT(*) FROM communities GROUP BY city, topic"
        ).fetchall()
    result: dict[str, dict[str, int]] = {}
    for city, topic, count in rows:
        result.setdefault(city, {})[topic] = count
    return result


def get_city_totals(db_path: Path) -> list[tuple[str, int]]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT city, COUNT(*) as cnt FROM communities GROUP BY city ORDER BY cnt DESC"
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_total_community_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM communities").fetchone()
    return row[0] if row else 0


def delete_all_communities(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("DELETE FROM communities")
        conn.commit()
        return cur.rowcount


# ── Cache pages ───────────────────────────────────────────────────────────────

def save_cache_page(db_path: Path, entry: dict) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO cache_pages
                (url_hash, url, city, topic, domain, scraped_at, extracted_at, extract_fingerprint, data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url_hash) DO UPDATE SET
                city=excluded.city,
                topic=excluded.topic,
                domain=excluded.domain,
                scraped_at=excluded.scraped_at,
                extracted_at=excluded.extracted_at,
                extract_fingerprint=excluded.extract_fingerprint,
                data=excluded.data
        """, (
            entry["url_hash"],
            entry.get("url", ""),
            entry.get("city", ""),
            entry.get("topic", ""),
            entry.get("domain", ""),
            entry.get("scraped_at"),
            entry.get("extracted_at"),
            entry.get("extract_fingerprint"),
            json.dumps(entry, ensure_ascii=False),
        ))
        conn.commit()


def load_cache_page(db_path: Path, url_hash: str) -> dict | None:
    if not db_path.exists():
        return None
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM cache_pages WHERE url_hash=?", (url_hash,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def delete_cache_page(db_path: Path, url_hash: str) -> bool:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("DELETE FROM cache_pages WHERE url_hash=?", (url_hash,))
        conn.commit()
        return cur.rowcount > 0


def clear_all_cache_pages(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("DELETE FROM cache_pages")
        conn.commit()
        return cur.rowcount


def get_cache_index(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM cache_pages ORDER BY url_hash"
        ).fetchall()
    entries = []
    for (data_json,) in rows:
        try:
            entry = json.loads(data_json)
            entries.append({
                "url_hash":                  entry.get("url_hash", ""),
                "url":                       entry.get("url", ""),
                "domain":                    entry.get("domain", ""),
                "city":                      entry.get("city", ""),
                "topic":                     entry.get("topic", ""),
                "scraped_at":                entry.get("scraped_at"),
                "scrape_duration_s":         entry.get("scrape_duration_s"),
                "extracted_at":              entry.get("extracted_at"),
                "extract_duration_s":        entry.get("extract_duration_s"),
                "enrich_scraped_at":         entry.get("enrich_scraped_at"),
                "enrich_scrape_duration_s":  entry.get("enrich_scrape_duration_s"),
                "enrich_extracted_at":       entry.get("enrich_extracted_at"),
                "enrich_extract_duration_s": entry.get("enrich_extract_duration_s"),
                "enrich_count":              entry.get("enrich_count"),
                "record_count":              len(entry.get("records") or []),
                "has_text":                  bool(entry.get("raw_text")),
                "extract_fingerprint":       entry.get("extract_fingerprint"),
                "extract_model":             entry.get("extract_model"),
            })
        except Exception:
            continue

    def _sort_key(e: dict) -> tuple:
        complete = 1 if (e.get("scraped_at") and e.get("extracted_at")) else 0
        ts = e.get("scraped_at") or "0000-00-00T00:00:00+00:00"
        return (complete, tuple(-ord(c) for c in ts))

    return sorted(entries, key=_sort_key)


def get_all_scraped_cache(db_path: Path) -> list[tuple[str, str, str, str]]:
    """Returns (url, raw_text, city, topic) for all cached pages with raw_text."""
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM cache_pages WHERE scraped_at IS NOT NULL"
        ).fetchall()
    result = []
    for (data_json,) in rows:
        try:
            entry = json.loads(data_json)
            if entry.get("raw_text"):
                result.append((
                    entry["url"],
                    entry["raw_text"],
                    entry.get("city", ""),
                    entry.get("topic", ""),
                ))
        except Exception:
            continue
    return result


# ── False positives ───────────────────────────────────────────────────────────

def get_false_positives(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name, city, topic, reason, source_url, fp_type, marked_at "
            "FROM false_positives ORDER BY id"
        ).fetchall()
    return [
        {"name": r[0], "city": r[1], "topic": r[2], "reason": r[3] or "",
         "source_url": r[4] or "", "fp_type": r[5], "marked_at": r[6]}
        for r in rows
    ]


def upsert_false_positive(db_path: Path, name: str, city: str, topic: str,
                          reason: str, source_url: str, fp_type: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO false_positives (name, city, topic, reason, source_url, fp_type, marked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name, city, topic, fp_type) DO UPDATE SET
                reason=excluded.reason,
                source_url=excluded.source_url,
                marked_at=excluded.marked_at
        """, (name, city, topic, reason, source_url, fp_type, now))
        conn.commit()


def delete_false_positive(db_path: Path, name: str, city: str, topic: str, fp_type: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM false_positives WHERE name=? AND city=? AND topic=? AND fp_type=?",
            (name, city, topic, fp_type)
        )
        conn.commit()


# ── Prompt history ────────────────────────────────────────────────────────────

def get_prompt_history(db_path: Path, fp_type: str) -> list[dict]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT version, timestamp, content, fp_type, fp_count "
            "FROM prompt_history WHERE fp_type=? ORDER BY version",
            (fp_type,)
        ).fetchall()
    return [
        {"version": r[0], "timestamp": r[1], "content": r[2], "fp_type": r[3], "fp_count": r[4]}
        for r in rows
    ]


def append_prompt_history(db_path: Path, version: int, timestamp: str,
                          content: str, fp_type: str, fp_count: int) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO prompt_history (version, timestamp, content, fp_type, fp_count)
            VALUES (?, ?, ?, ?, ?)
        """, (version, timestamp, content, fp_type, fp_count))
        conn.commit()
