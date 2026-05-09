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


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
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

        # Search result cache — URL lists per city+topic
        conn.execute("""
            CREATE TABLE IF NOT EXISTS search_cache (
                city       TEXT NOT NULL,
                topic      TEXT NOT NULL,
                urls       TEXT NOT NULL,
                queries    TEXT NOT NULL,
                cached_at  TEXT NOT NULL,
                PRIMARY KEY (city, topic)
            )
        """)

        # Venues — physical locations that host communities
        conn.execute("""
            CREATE TABLE IF NOT EXISTS venues (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                record_key TEXT NOT NULL UNIQUE,
                venue_id   TEXT NOT NULL,
                city       TEXT NOT NULL,
                data       TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venues_city ON venues(city)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_venues_venue_id ON venues(venue_id)")

        # Persons — leaders, instructors, speakers linked to communities
        conn.execute("""
            CREATE TABLE IF NOT EXISTS persons (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                record_key  TEXT NOT NULL UNIQUE,
                person_id   TEXT NOT NULL,
                city        TEXT NOT NULL,
                topic       TEXT NOT NULL,
                role        TEXT NOT NULL,
                data        TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_city_topic ON persons(city, topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_person_id ON persons(person_id)")

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
    with _connect(db_path) as conn:
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
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT finished_at FROM runs WHERE success=1 ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0])
    except Exception:
        return None
    return None


def get_run_history(db_path: Path, limit: int = 20) -> list[dict]:
    if not db_path.exists():
        return []
    try:
        with _connect(db_path) as conn:
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
        with _connect(db_path) as conn:
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
    with _connect(db_path) as conn:
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
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM subscriptions WHERE token=?", (token,))
        conn.commit()
        return cur.rowcount > 0


def get_subscriptions(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, email, city, topic, created_at FROM subscriptions ORDER BY id DESC"
        ).fetchall()
    return [{"id": r[0], "email": r[1], "city": r[2], "topic": r[3], "created_at": r[4]}
            for r in rows]


# ── Communities ───────────────────────────────────────────────────────────────

def _bulk_upsert_communities(conn: sqlite3.Connection, records: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
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


def bulk_upsert_communities(db_path: Path, records: list[dict]) -> None:
    with _connect(db_path) as conn:
        _bulk_upsert_communities(conn, records)
        conn.commit()


def replace_communities_for_topic(
    db_path: Path,
    city: str,
    topic: str,
    records: list[dict],
) -> None:
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM communities WHERE city=? AND topic=?", (city, topic))
        _bulk_upsert_communities(conn, records)
        conn.commit()


def delete_communities_for_topic(db_path: Path, city: str, topic: str) -> None:
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM communities WHERE city=? AND topic=?", (city, topic))
        conn.commit()


def get_communities(db_path: Path, city: str, topic: str) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? AND topic=? ORDER BY id",
            (city, topic)
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_communities_for_city(db_path: Path, city: str) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? ORDER BY topic, id",
            (city,)
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def find_community_by_id(db_path: Path, community_id: str) -> dict | None:
    if not db_path.exists():
        return None
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE community_id=? LIMIT 1",
            (community_id,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def get_communities_by_ids(db_path: Path, community_ids: list[str]) -> list[dict]:
    """Bulk fetch communities by a list of community_id values."""
    if not db_path.exists() or not community_ids:
        return []
    placeholders = ",".join("?" * len(community_ids))
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT data FROM communities WHERE community_id IN ({placeholders})",
            community_ids,
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_topic_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT topic, COUNT(*) FROM communities GROUP BY topic"
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_city_topic_counts(db_path: Path) -> dict[str, dict[str, int]]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
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
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT city, COUNT(*) as cnt FROM communities GROUP BY city ORDER BY cnt DESC"
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_total_community_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    with _connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM communities").fetchone()
    return row[0] if row else 0


def delete_all_communities(db_path: Path) -> int:
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM communities")
        conn.commit()
        return cur.rowcount


# ── Cache pages ───────────────────────────────────────────────────────────────

def save_cache_page(db_path: Path, entry: dict) -> None:
    with _connect(db_path) as conn:
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
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM cache_pages WHERE url_hash=?", (url_hash,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def delete_cache_page(db_path: Path, url_hash: str) -> bool:
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM cache_pages WHERE url_hash=?", (url_hash,))
        conn.commit()
        return cur.rowcount > 0


def clear_all_cache_pages(db_path: Path) -> int:
    with _connect(db_path) as conn:
        cur = conn.execute("DELETE FROM cache_pages")
        conn.commit()
        return cur.rowcount


def get_cache_index(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM cache_pages ORDER BY url_hash"
        ).fetchall()
    entries = []
    for (data_json,) in rows:
        try:
            entry = json.loads(data_json)
        except Exception:
            entry = None
        if not isinstance(entry, dict):
            continue
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
            "enrich_model":              entry.get("enrich_model") or (entry.get("extract_model") if entry.get("enrich_extracted_at") else None),
        })

    def _sort_key(e: dict) -> tuple:
        complete = 1 if (e.get("scraped_at") and e.get("extracted_at")) else 0
        ts = e.get("scraped_at") or "0000-00-00T00:00:00+00:00"
        return (complete, tuple(-ord(c) for c in ts))

    return sorted(entries, key=_sort_key)


def get_all_scraped_cache(db_path: Path) -> list[tuple[str, str, str, str]]:
    """Returns (url, raw_text, city, topic) for all cached pages with raw_text."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM cache_pages WHERE scraped_at IS NOT NULL"
        ).fetchall()
    result = []
    for (data_json,) in rows:
        try:
            entry = json.loads(data_json)
        except Exception:
            entry = None
        if isinstance(entry, dict) and entry.get("raw_text"):
            result.append((
                entry["url"],
                entry["raw_text"],
                entry.get("city", ""),
                entry.get("topic", ""),
            ))
    return result


# ── False positives ───────────────────────────────────────────────────────────

def get_false_positives(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
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
    with _connect(db_path) as conn:
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
    with _connect(db_path) as conn:
        conn.execute(
            "DELETE FROM false_positives WHERE name=? AND city=? AND topic=? AND fp_type=?",
            (name, city, topic, fp_type)
        )
        conn.commit()


# ── Prompt history ────────────────────────────────────────────────────────────

def get_prompt_history(db_path: Path, fp_type: str) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
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
    with _connect(db_path) as conn:
        conn.execute("""
            INSERT INTO prompt_history (version, timestamp, content, fp_type, fp_count)
            VALUES (?, ?, ?, ?, ?)
        """, (version, timestamp, content, fp_type, fp_count))
        conn.commit()


# ── Search cache ───────────────────────────────────────────────────────────────

def save_search_cache(db_path: Path, city: str, topic: str,
                      urls: list[str], queries: list[str]) -> None:
    import json
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute("""
            INSERT INTO search_cache (city, topic, urls, queries, cached_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(city, topic) DO UPDATE SET
                urls=excluded.urls, queries=excluded.queries, cached_at=excluded.cached_at
        """, (city, topic, json.dumps(urls), json.dumps(queries), now))
        conn.commit()


def get_search_cache(db_path: Path, city: str, topic: str,
                     ttl_days: int) -> list[str] | None:
    import json
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT urls FROM search_cache WHERE city=? AND topic=? AND cached_at>=?",
            (city, topic, cutoff)
        ).fetchone()
    return json.loads(row[0]) if row else None


def get_cache_cost_stats(db_path: Path) -> dict:
    """Return counts of work done: search queries issued, pages fetched, AI extractions run."""
    empty = {"search_queries": 0, "web_reads": 0, "ai_calls": 0, "search_pairs": 0}
    if not db_path.exists():
        return empty
    with _connect(db_path) as conn:
        search_pairs = conn.execute("SELECT COUNT(*) FROM search_cache").fetchone()[0]
        search_q = conn.execute(
            "SELECT COALESCE(SUM(json_array_length(queries)), 0) FROM search_cache"
        ).fetchone()[0]
        web_reads = conn.execute(
            "SELECT COUNT(*) FROM cache_pages WHERE scraped_at IS NOT NULL"
        ).fetchone()[0]
        ai_calls = conn.execute(
            "SELECT COUNT(*) FROM cache_pages WHERE extracted_at IS NOT NULL"
        ).fetchone()[0]
    return {
        "search_pairs": int(search_pairs),
        "search_queries": int(search_q),
        "web_reads": int(web_reads),
        "ai_calls": int(ai_calls),
    }


# ── Venues ────────────────────────────────────────────────────────────────────

def _venue_record_key(name: str, city: str) -> str:
    return f"{_norm(name)}|{_norm(city)}"


def upsert_venues(db_path: Path, records: list[dict]) -> int:
    if not records:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    with _connect(db_path) as conn:
        for record in records:
            key = _venue_record_key(record["name"], record["city"])
            existing = conn.execute(
                "SELECT data FROM venues WHERE record_key=?", (key,)
            ).fetchone()
            if existing:
                ex = json.loads(existing[0])
                prev_urls: list[str] = ex.get("source_urls") or []
                new_urls: list[str] = record.get("source_urls") or []
                merged = list(dict.fromkeys(new_urls + prev_urls))
                record = {**record, "source_urls": merged}
                # Merge community_ids
                prev_cids = ex.get("community_ids") or []
                new_cids = record.get("community_ids") or []
                record["community_ids"] = list(dict.fromkeys(new_cids + prev_cids))
            conn.execute("""
                INSERT INTO venues (record_key, venue_id, city, data, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    data=excluded.data, venue_id=excluded.venue_id, updated_at=excluded.updated_at
            """, (key, record.get("venue_id", ""), record["city"],
                  json.dumps(record, ensure_ascii=False), now))
            count += 1
        conn.commit()
    return count


def get_venues(db_path: Path, city: str) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM venues WHERE city=? ORDER BY id", (city,)
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_all_venues(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT data FROM venues ORDER BY city, id").fetchall()
    return [json.loads(r[0]) for r in rows]


def get_venue_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT city, COUNT(*) FROM venues GROUP BY city").fetchall()
    return {r[0]: r[1] for r in rows}


def get_venue_person_counts_by_url(db_path: Path) -> dict[str, dict]:
    """Return {url: {venues: n, persons: n}} for the progress page."""
    if not db_path.exists():
        return {}
    result: dict[str, dict] = {}
    with _connect(db_path) as conn:
        for (src_url, cnt) in conn.execute(
            "SELECT json_extract(data,'$.source_url'), COUNT(*) FROM venues"
            " WHERE json_extract(data,'$.source_url') IS NOT NULL"
            " GROUP BY json_extract(data,'$.source_url')"
        ).fetchall():
            result.setdefault(src_url, {"venues": 0, "persons": 0})["venues"] = cnt
        for (src_url, cnt) in conn.execute(
            "SELECT json_extract(data,'$.source_url'), COUNT(*) FROM persons"
            " WHERE json_extract(data,'$.source_url') IS NOT NULL"
            " GROUP BY json_extract(data,'$.source_url')"
        ).fetchall():
            result.setdefault(src_url, {"venues": 0, "persons": 0})["persons"] = cnt
    return result


# ── Persons ───────────────────────────────────────────────────────────────────

def _person_record_key(name: str, city: str, role: str, community_name: str) -> str:
    return f"{_norm(name)}|{_norm(city)}|{_norm(role)}|{_norm(community_name)}"


def upsert_persons(db_path: Path, records: list[dict]) -> int:
    if not records:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    with _connect(db_path) as conn:
        for record in records:
            key = _person_record_key(
                record["name"], record["city"],
                record.get("role", "leader"), record.get("community_name", "")
            )
            existing = conn.execute(
                "SELECT data FROM persons WHERE record_key=?", (key,)
            ).fetchone()
            if existing:
                ex = json.loads(existing[0])
                prev_urls = ex.get("source_urls") or []
                new_urls = record.get("source_urls") or []
                record = {**record, "source_urls": list(dict.fromkeys(new_urls + prev_urls))}
            conn.execute("""
                INSERT INTO persons (record_key, person_id, city, topic, role, data, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    data=excluded.data, person_id=excluded.person_id, updated_at=excluded.updated_at
            """, (key, record.get("person_id", ""), record["city"],
                  record.get("topic", ""), record.get("role", "leader"),
                  json.dumps(record, ensure_ascii=False), now))
            count += 1
        conn.commit()
    return count


def get_persons(db_path: Path, city: str, topic: str | None = None) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        if topic:
            rows = conn.execute(
                "SELECT data FROM persons WHERE city=? AND topic=? ORDER BY role, id",
                (city, topic)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT data FROM persons WHERE city=? ORDER BY topic, role, id", (city,)
            ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_persons_for_community(db_path: Path, community_name: str, city: str) -> list[dict]:
    if not db_path.exists():
        return []
    key_prefix = f"{_norm(community_name)}|{_norm(city)}|"
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM persons WHERE record_key LIKE ? ORDER BY role, id",
            (f"%|{_norm(community_name)}|{_norm(city)}",)
        ).fetchall()
        if not rows:
            # fallback: match by community_name in JSON
            rows = conn.execute(
                "SELECT data FROM persons WHERE city=? AND json_extract(data,'$.community_name')=?",
                (city, community_name)
            ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_person_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT city, COUNT(*) FROM persons GROUP BY city").fetchall()
    return {r[0]: r[1] for r in rows}
