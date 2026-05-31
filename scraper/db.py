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
        try:
            conn.execute("ALTER TABLE runs ADD COLUMN new_records INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS city_requests (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                city_name  TEXT NOT NULL,
                email      TEXT,
                created_at TEXT NOT NULL
            )
        """)
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
        try:
            conn.execute("ALTER TABLE communities ADD COLUMN revalidate_fingerprint TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE communities ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        # Back-fill: hide communities already in not_community_reports
        try:
            conn.execute("""
                UPDATE communities SET hidden=1
                WHERE hidden=0
                  AND community_id IN (
                    SELECT community_id FROM not_community_reports
                    WHERE community_id IS NOT NULL AND community_id != ''
                  )
            """)
        except sqlite3.OperationalError:
            pass

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
        try:
            conn.execute("ALTER TABLE cache_pages ADD COLUMN venue_fingerprint TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE cache_pages ADD COLUMN person_fingerprint TEXT")
        except sqlite3.OperationalError:
            pass
        # Backfill fingerprint columns from JSON blob (runs once, skips already-set rows)
        conn.execute("""
            UPDATE cache_pages
            SET venue_fingerprint  = json_extract(data, '$.venue_fingerprint'),
                person_fingerprint = json_extract(data, '$.person_fingerprint')
            WHERE venue_fingerprint IS NULL AND person_fingerprint IS NULL
              AND (json_extract(data, '$.venue_fingerprint') IS NOT NULL
                OR json_extract(data, '$.person_fingerprint') IS NOT NULL)
        """)
        conn.commit()

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

        # Editable prompt overrides (key = prompt identifier)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prompt_overrides (
                key        TEXT PRIMARY KEY,
                content    TEXT NOT NULL,
                updated_at TEXT NOT NULL
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

        # Field-level change history for communities
        conn.execute("""
            CREATE TABLE IF NOT EXISTS community_history (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                community_id TEXT NOT NULL,
                changed_at   TEXT NOT NULL,
                changed_by   TEXT NOT NULL DEFAULT 'scraper',
                field        TEXT NOT NULL,
                old_value    TEXT,
                new_value    TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_community_id ON community_history(community_id)"
        )

        # Field-level change history for venues
        conn.execute("""
            CREATE TABLE IF NOT EXISTS venue_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                venue_id   TEXT NOT NULL,
                changed_at TEXT NOT NULL,
                changed_by TEXT NOT NULL DEFAULT 'scraper',
                field      TEXT NOT NULL,
                old_value  TEXT,
                new_value  TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_venue_history_venue_id ON venue_history(venue_id)"
        )

        # Field-level change history for persons
        conn.execute("""
            CREATE TABLE IF NOT EXISTS person_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id  TEXT NOT NULL,
                changed_at TEXT NOT NULL,
                changed_by TEXT NOT NULL DEFAULT 'scraper',
                field      TEXT NOT NULL,
                old_value  TEXT,
                new_value  TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_person_history_person_id ON person_history(person_id)"
        )

        # User-submitted "not a community" flags — pending admin review
        conn.execute("""
            CREATE TABLE IF NOT EXISTS not_community_reports (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                community_id   TEXT,
                community_name TEXT NOT NULL,
                city           TEXT,
                topic          TEXT,
                source_url     TEXT,
                page_url       TEXT,
                reported_at    TEXT NOT NULL
            )
        """)

        # Duplicate candidate pairs detected by fuzzy matching
        conn.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_candidates (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type  TEXT NOT NULL,
                winner_id    TEXT NOT NULL,
                loser_id     TEXT NOT NULL,
                winner_key   TEXT NOT NULL,
                loser_key    TEXT NOT NULL,
                similarity   REAL NOT NULL,
                signal       TEXT NOT NULL,
                detected_at  TEXT NOT NULL,
                resolved_at  TEXT,
                resolution   TEXT
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_dup_pair
            ON duplicate_candidates(entity_type, winner_key, loser_key)
            WHERE resolution IS NULL
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS edit_requests (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type  TEXT NOT NULL,
                entity_id    TEXT NOT NULL,
                entity_name  TEXT NOT NULL,
                entity_city  TEXT NOT NULL,
                entity_topic TEXT,
                record_key   TEXT NOT NULL,
                change_type  TEXT NOT NULL,
                new_value    TEXT,
                notes        TEXT NOT NULL,
                email        TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                submitted_at TEXT NOT NULL,
                reviewed_at  TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS community_submissions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL,
                city            TEXT NOT NULL,
                topic           TEXT NOT NULL,
                source_url      TEXT NOT NULL,
                submitter_email TEXT,
                submitted_at    TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS recategorize_suggestions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                record_key       TEXT NOT NULL UNIQUE,
                community_name   TEXT,
                city             TEXT,
                description      TEXT,
                suggested_topic  TEXT,
                confidence       REAL,
                reasoning        TEXT,
                status           TEXT NOT NULL DEFAULT 'pending',
                created_at       TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS outclick_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                community_id TEXT NOT NULL,
                url          TEXT NOT NULL,
                link_type    TEXT NOT NULL DEFAULT 'website',
                clicked_at   TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outclick_community ON outclick_events(community_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_outclick_clicked_at ON outclick_events(clicked_at)"
        )

        conn.commit()


# ── Runs ──────────────────────────────────────────────────────────────────────

def start_run(db_path: Path, started_at: datetime, run_mode: str) -> int:
    """Insert a run row immediately (finished_at=NULL). Call finish_run when done."""
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, run_mode, success) VALUES (?, ?, 0)",
            (started_at.isoformat(), run_mode),
        )
        conn.commit()
        return cur.lastrowid


def finish_run(
    db_path: Path,
    run_id: int,
    finished_at: datetime,
    success: bool,
    search_log: str | None = None,
    new_records: int = 0,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE runs SET finished_at=?, success=?, search_log=?, new_records=? WHERE id=?",
            (finished_at.isoformat(), int(success), search_log, new_records, run_id),
        )
        conn.commit()


def record_run(
    db_path: Path,
    started_at: datetime,
    finished_at: datetime,
    run_mode: str,
    success: bool,
    search_log: str | None = None,
    new_records: int = 0,
) -> int:
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, finished_at, run_mode, success, search_log, new_records) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (started_at.isoformat(), finished_at.isoformat(),
             run_mode, int(success), search_log, new_records),
        )
        conn.commit()
        return cur.lastrowid


def get_last_run_row(db_path: Path) -> dict | None:
    """Return the most recent run row regardless of success/completion."""
    if not db_path.exists():
        return None
    try:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT id, run_mode, finished_at, success FROM runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                return {"id": row[0], "run_mode": row[1], "finished_at": row[2], "success": bool(row[3])}
    except Exception:
        return None
    return None


def get_last_run_mode(db_path: Path) -> str | None:
    if not db_path.exists():
        return None
    try:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT run_mode FROM runs WHERE success=1 ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return row[0] if row else None
    except Exception:
        return None


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
                "SELECT id, started_at, finished_at, run_mode, success, COALESCE(new_records, 0) "
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
                "new_records": r[5],
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


# ── Shared history helpers ────────────────────────────────────────────────────

def _log_changes(
    conn: sqlite3.Connection,
    table: str,
    id_col: str,
    record_id: str,
    fields: list[str],
    old_data: dict | None,
    new_data: dict,
    changed_by: str = "scraper",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    if old_data is None:
        conn.execute(
            f"INSERT INTO {table} ({id_col}, changed_at, changed_by, field, old_value, new_value)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (record_id, now, changed_by, "__created__", None, new_data.get("name", "")),
        )
        return
    for field in fields:
        old_v = _val_str(old_data.get(field))
        new_v = _val_str(new_data.get(field))
        if old_v != new_v:
            conn.execute(
                f"INSERT INTO {table} ({id_col}, changed_at, changed_by, field, old_value, new_value)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (record_id, now, changed_by, field, old_v, new_v),
            )


def _get_history(db_path: Path, table: str, id_col: str, record_id: str, limit: int = 100) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT changed_at, changed_by, field, old_value, new_value"
            f" FROM {table} WHERE {id_col}=?"
            f" ORDER BY changed_at DESC, id DESC LIMIT ?",
            (record_id, limit),
        ).fetchall()
    return [{"changed_at": r[0], "changed_by": r[1], "field": r[2],
             "old_value": r[3], "new_value": r[4]} for r in rows]


# ── Communities ───────────────────────────────────────────────────────────────

_HISTORY_FIELDS = [
    "name", "description", "history", "website", "tags", "social_links",
    "meeting_schedule", "location", "contact", "fee", "age_range", "skill_level",
    "join_process", "leader", "language", "frequency", "founding_year", "member_count",
    "email", "phone", "confidence", "joinable",
]


def _val_str(v) -> str | None:
    if v is None or v == "":
        return None
    if isinstance(v, (list, dict)):
        s = json.dumps(v, ensure_ascii=False)
        return None if s in ("[]", "{}") else s
    return str(v)


def _log_community_changes(
    conn: sqlite3.Connection,
    community_id: str,
    old_data: dict | None,
    new_data: dict,
    changed_by: str = "scraper",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    if old_data is None:
        conn.execute(
            "INSERT INTO community_history"
            " (community_id, changed_at, changed_by, field, old_value, new_value)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (community_id, now, changed_by, "__created__", None, new_data.get("name", "")),
        )
        return
    for field in _HISTORY_FIELDS:
        old_v = _val_str(old_data.get(field))
        new_v = _val_str(new_data.get(field))
        if old_v != new_v:
            conn.execute(
                "INSERT INTO community_history"
                " (community_id, changed_at, changed_by, field, old_value, new_value)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (community_id, now, changed_by, field, old_v, new_v),
            )


def _merge_source_urls(old_data: dict | None, record: dict) -> dict:
    if not old_data:
        return record
    prev_urls: list[str] = old_data.get("source_urls") or []
    if old_data.get("source_url") and old_data["source_url"] not in prev_urls:
        prev_urls = [old_data["source_url"]] + prev_urls
    new_urls: list[str] = record.get("source_urls") or []
    if record.get("source_url") and record["source_url"] not in new_urls:
        new_urls = [record["source_url"]] + new_urls
    return {**record, "source_urls": list(dict.fromkeys(new_urls + prev_urls))}


def _bulk_upsert_communities(
    conn: sqlite3.Connection,
    records: list[dict],
    previous: dict[str, dict] | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    for record in records:
        key = _community_record_key(record["name"], record["city"], record["topic"])

        # Prefer pre-delete snapshot; fall back to live row (used by bulk_upsert_communities)
        old_data = (previous or {}).get(key)
        if old_data is None:
            existing_row = conn.execute(
                "SELECT data FROM communities WHERE record_key=?", (key,)
            ).fetchone()
            if existing_row:
                old_data = json.loads(existing_row[0])

        record = _merge_source_urls(old_data, record)

        conn.execute("""
            INSERT INTO communities (record_key, community_id, city, topic, data, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_key) DO UPDATE SET
                data=excluded.data,
                community_id=excluded.community_id,
                updated_at=excluded.updated_at
        """, (key, record.get("community_id", ""), record["city"], record["topic"],
              json.dumps(record, ensure_ascii=False), now))

        _log_community_changes(conn, record.get("community_id", ""), old_data, record)


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
        # Snapshot existing records before delete so history can diff against them
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? AND topic=?", (city, topic)
        ).fetchall()
        previous: dict[str, dict] = {}
        for (data_str,) in rows:
            d = json.loads(data_str)
            previous[_community_record_key(d["name"], d["city"], d["topic"])] = d

        conn.execute("DELETE FROM communities WHERE city=? AND topic=?", (city, topic))
        _bulk_upsert_communities(conn, records, previous)
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
            "SELECT data FROM communities WHERE city=? AND topic=? AND hidden=0 ORDER BY id",
            (city, topic)
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def search_communities_by_tag(db_path: Path, tag: str, city: str = "") -> list[dict]:
    """Return communities whose tags array contains `tag` (exact match, case-sensitive)."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        if city:
            rows = conn.execute(
                "SELECT data FROM communities WHERE city=? AND hidden=0 AND EXISTS ("
                "  SELECT 1 FROM json_each(json_extract(data,'$.tags')) WHERE value=?"
                ") ORDER BY id",
                (city, tag)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT data FROM communities WHERE hidden=0 AND EXISTS ("
                "  SELECT 1 FROM json_each(json_extract(data,'$.tags')) WHERE value=?"
                ") ORDER BY city, id",
                (tag,)
            ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_all_communities(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM communities WHERE hidden=0 ORDER BY city, topic, id"
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_community_by_record_key(db_path: Path, record_key: str) -> dict | None:
    """Get a single community by record_key, including hidden records."""
    if not db_path or not db_path.exists():
        return None
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (record_key,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def get_communities_needing_revalidation(
    db_path: Path,
    fingerprint: str,
    city: str = "",
    topic: str = "",
) -> list[dict]:
    """Return communities previously revalidated but with a now-stale fingerprint."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        if city and topic:
            rows = conn.execute(
                "SELECT data FROM communities WHERE city=? AND topic=?"
                " AND revalidate_fingerprint IS NOT NULL AND revalidate_fingerprint != ?"
                " ORDER BY id",
                (city, topic, fingerprint),
            ).fetchall()
        elif city:
            rows = conn.execute(
                "SELECT data FROM communities WHERE city=?"
                " AND revalidate_fingerprint IS NOT NULL AND revalidate_fingerprint != ?"
                " ORDER BY topic, id",
                (city, fingerprint),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT data FROM communities"
                " WHERE revalidate_fingerprint IS NOT NULL AND revalidate_fingerprint != ?"
                " ORDER BY city, topic, id",
                (fingerprint,),
            ).fetchall()
    return [json.loads(r[0]) for r in rows]


def count_communities_needing_revalidation(
    db_path: Path,
    fingerprint: str,
    cities: list[str] | None = None,
) -> int:
    """COUNT of communities previously revalidated but with a now-stale fingerprint."""
    if not db_path.exists():
        return 0
    with _connect(db_path) as conn:
        if cities:
            placeholders = ",".join("?" * len(cities))
            row = conn.execute(
                f"SELECT COUNT(*) FROM communities"
                f" WHERE city IN ({placeholders})"
                f" AND revalidate_fingerprint IS NOT NULL AND revalidate_fingerprint != ?",
                (*cities, fingerprint),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) FROM communities"
                " WHERE revalidate_fingerprint IS NOT NULL AND revalidate_fingerprint != ?",
                (fingerprint,),
            ).fetchone()
    return row[0] if row else 0


def set_community_revalidate_fingerprint(
    db_path: Path, record_key: str, fingerprint: str
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE communities SET revalidate_fingerprint=? WHERE record_key=?",
            (fingerprint, record_key),
        )
        conn.commit()


def get_community_history(db_path: Path, community_id: str, limit: int = 100) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT changed_at, changed_by, field, old_value, new_value"
            " FROM community_history WHERE community_id=?"
            " ORDER BY changed_at DESC, id DESC LIMIT ?",
            (community_id, limit),
        ).fetchall()
    return [
        {"changed_at": r[0], "changed_by": r[1], "field": r[2],
         "old_value": r[3], "new_value": r[4]}
        for r in rows
    ]


_VENUE_HISTORY_FIELDS = [
    "name", "description", "venue_type", "address", "website",
    "social_links", "email", "phone", "contact", "welcomed_topics",
]

_PERSON_HISTORY_FIELDS = [
    "name", "role", "bio", "email", "website", "social_links", "community_name",
]


def get_venue_history(db_path: Path, venue_id: str, limit: int = 100) -> list[dict]:
    return _get_history(db_path, "venue_history", "venue_id", venue_id, limit)


def get_person_history(db_path: Path, person_id: str, limit: int = 100) -> list[dict]:
    return _get_history(db_path, "person_history", "person_id", person_id, limit)


def set_community_hidden(db_path: Path, record_key: str, hidden: bool) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE communities SET hidden=? WHERE record_key=?",
            (1 if hidden else 0, record_key),
        )
        conn.commit()


def get_communities_for_city(db_path: Path, city: str) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? AND hidden=0 ORDER BY topic, id",
            (city,)
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_communities_missing_description(db_path: Path) -> list[dict]:
    """Return visible communities where description is NULL or empty."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM communities WHERE hidden=0 "
            "AND (json_extract(data,'$.description') IS NULL OR json_extract(data,'$.description') = '') "
            "ORDER BY city, id"
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def find_community_by_id(db_path: Path, community_id: str) -> dict | None:
    if not db_path.exists():
        return None
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE community_id=? AND hidden=0 LIMIT 1",
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
            f"SELECT data FROM communities WHERE community_id IN ({placeholders}) AND hidden=0",
            community_ids,
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_communities_for_venue(
    db_path: Path,
    community_ids: list[str],
    venue_name: str,
    city: str,
) -> list[dict]:
    """Return communities associated with a venue.
    Tries community_ids first; falls back to location-text match."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        if community_ids:
            placeholders = ",".join("?" * len(community_ids))
            rows = conn.execute(
                f"SELECT data FROM communities WHERE community_id IN ({placeholders}) AND hidden=0",
                community_ids,
            ).fetchall()
            if rows:
                return [json.loads(r[0]) for r in rows]
        safe_name = venue_name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        rows = conn.execute(
            "SELECT data FROM communities WHERE city=? AND hidden=0"
            " AND json_extract(data,'$.location') LIKE ? ESCAPE '\\'",
            (city, f"%{safe_name}%"),
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_venue_for_community(db_path: Path, community_id: str, city: str) -> dict | None:
    """Return the first venue in city whose community_ids list contains community_id."""
    if not db_path.exists() or not community_id:
        return None
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM venues WHERE city=? AND EXISTS ("
            "  SELECT 1 FROM json_each(json_extract(data,'$.community_ids')) WHERE value=?"
            ") LIMIT 1",
            (city, community_id),
        ).fetchone()
    return json.loads(row[0]) if row else None


def get_topic_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT topic, COUNT(*) FROM communities WHERE hidden=0 GROUP BY topic"
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_topic_counts_for_cities(db_path: Path, city_names: set[str]) -> dict[str, int]:
    """Topic counts restricted to a specific set of city names (single query)."""
    if not db_path.exists() or not city_names:
        return {}
    placeholders = ",".join("?" * len(city_names))
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT topic, COUNT(*) FROM communities WHERE city IN ({placeholders}) AND hidden=0 GROUP BY topic",
            tuple(city_names),
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_city_topic_counts(db_path: Path) -> dict[str, dict[str, int]]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT city, topic, COUNT(*) FROM communities WHERE hidden=0 GROUP BY city, topic"
        ).fetchall()
    result: dict[str, dict[str, int]] = {}
    for city, topic, count in rows:
        result.setdefault(city, {})[topic] = count
    return result


def get_city_topic_states(db_path: Path, current_fp: str) -> dict[str, dict[str, dict]]:
    """Return per-(city, topic) state dict for the coverage page.

    State keys per cell:
      community_count: int
      page_count: int   (search_cache URLs that have been scraped)
      current_fp_count: int  (scraped URLs extracted with current_fp)

    Uses url_hash lookup (same as get_fully_processed_pairs) so that
    page_count/current_fp_count are consistent with the done-pairs check.
    The old city/topic JOIN was unreliable because cache_pages.city/topic
    is last-write-wins and gets overwritten when a URL appears in multiple
    search results.
    """
    import hashlib as _hashlib

    def _url_hash(url: str) -> str:
        return _hashlib.sha256(url.encode()).hexdigest()[:16]

    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        # url_hash → extract_fingerprint for all successfully scraped pages
        fp_by_hash: dict[str, str | None] = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT url_hash, extract_fingerprint FROM cache_pages WHERE scraped_at IS NOT NULL"
            )
        }
        comm_counts: dict[tuple[str, str], int] = {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                "SELECT city, topic, COUNT(*) FROM communities WHERE hidden=0 GROUP BY city, topic"
            )
        }
        search_rows = conn.execute("SELECT city, topic, urls FROM search_cache").fetchall()

    result: dict[str, dict[str, dict]] = {}
    for city, topic, urls_json in search_rows:
        urls: list[str] = json.loads(urls_json) if urls_json else []
        hashes = [_url_hash(u) for u in urls]
        page_count = sum(1 for h in hashes if h in fp_by_hash)
        current_fp_count = sum(1 for h in hashes if fp_by_hash.get(h) == current_fp)
        result.setdefault(city, {})[topic] = {
            "page_count": page_count,
            "current_fp_count": current_fp_count,
            "community_count": comm_counts.get((city, topic), 0),
        }
    return result


def get_fully_processed_pairs(db_path: Path, current_fp: str) -> set[tuple[str, str]]:
    """Return (city, topic) pairs that need no pipeline work this run.

    cache_pages is keyed by url_hash and city/topic columns are overwritten on
    every save (last-write-wins), so city/topic-based joins are unreliable.
    Instead we check by url_hash, exactly as cache.py does at fetch time.

    A pair is done if it has a search_cache entry AND:
    - The URL list is empty (search found nothing), OR
    - Communities already exist for the pair (green circle), OR
    - Every URL in the search result has been extracted with current fingerprint
      (all url_hashes appear in cache_pages with extract_fingerprint = current_fp)
    """
    import hashlib

    def _url_hash(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    if not db_path.exists():
        return set()
    with _connect(db_path) as conn:
        # Pairs that already have at least one community
        community_pairs: set[tuple[str, str]] = {
            (r[0], r[1]) for r in conn.execute(
                "SELECT DISTINCT city, topic FROM communities WHERE hidden = 0"
            )
        }
        # url_hashes successfully scraped (scraped_at IS NOT NULL)
        scraped_hashes: set[str] = {
            r[0] for r in conn.execute(
                "SELECT url_hash FROM cache_pages WHERE scraped_at IS NOT NULL"
            )
        }
        # url_hashes extracted with the current fingerprint (global, city/topic-agnostic)
        current_fp_hashes: set[str] = {
            r[0] for r in conn.execute(
                "SELECT url_hash FROM cache_pages WHERE extract_fingerprint = ?", (current_fp,)
            )
        }
        search_rows = conn.execute("SELECT city, topic, urls FROM search_cache").fetchall()

    # Green pairs are always done — communities already exist, no need to re-process
    result: set[tuple[str, str]] = set(community_pairs)
    # Blue pairs: searched, all SCRAPED pages extracted with current fp, 0 communities.
    # Unscraped urls (blocked/failed fetches) are excluded — consistent with get_city_topic_states.
    for city, topic, urls_json in search_rows:
        if (city, topic) in result:
            continue  # already green, skip
        urls: list[str] = json.loads(urls_json) if urls_json else []
        if not urls:
            result.add((city, topic))
        else:
            processable = [u for u in urls if _url_hash(u) in scraped_hashes]
            if processable and all(_url_hash(u) in current_fp_hashes for u in processable):
                result.add((city, topic))
    return result


def get_city_totals(db_path: Path) -> list[tuple[str, int]]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT city, COUNT(*) as cnt FROM communities WHERE hidden=0 GROUP BY city ORDER BY cnt DESC"
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_total_community_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    with _connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM communities WHERE hidden=0").fetchone()
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
                (url_hash, url, city, topic, domain, scraped_at, extracted_at,
                 extract_fingerprint, venue_fingerprint, person_fingerprint, data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url_hash) DO UPDATE SET
                city=excluded.city,
                topic=excluded.topic,
                domain=excluded.domain,
                scraped_at=excluded.scraped_at,
                extracted_at=excluded.extracted_at,
                extract_fingerprint=excluded.extract_fingerprint,
                venue_fingerprint=excluded.venue_fingerprint,
                person_fingerprint=excluded.person_fingerprint,
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
            entry.get("venue_fingerprint"),
            entry.get("person_fingerprint"),
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


def clear_person_cache(db_path: Path) -> int:
    """Strip person extraction fields from all cache entries, forcing re-extraction."""
    with _connect(db_path) as conn:
        cur = conn.execute("""
            UPDATE cache_pages SET data = json_remove(
                json_remove(json_remove(json_remove(data,
                    '$.person_extracted_at'),
                    '$.person_fingerprint'),
                    '$.person_model'),
                    '$.persons_data')
            WHERE json_extract(data, '$.person_extracted_at') IS NOT NULL
        """)
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


# ── Prompt overrides ──────────────────────────────────────────────────────────

def get_prompt_overrides(db_path: Path) -> dict[str, str]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT key, content FROM prompt_overrides").fetchall()
    return {r[0]: r[1] for r in rows}


def upsert_prompt_override(db_path: Path, key: str, content: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO prompt_overrides (key, content, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(key) DO UPDATE SET content=excluded.content, updated_at=excluded.updated_at",
            (key, content, now),
        )
        conn.commit()


def delete_prompt_override(db_path: Path, key: str) -> None:
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM prompt_overrides WHERE key=?", (key,))
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


def count_cached_search_pairs(db_path: Path, cities: list[str], ttl_days: int) -> int:
    """Count (city, topic) pairs that have a non-expired search cache entry."""
    if not db_path.exists() or not cities:
        return 0
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
    placeholders = ",".join("?" * len(cities))
    with _connect(db_path) as conn:
        row = conn.execute(
            f"SELECT COUNT(*) FROM search_cache WHERE city IN ({placeholders}) AND cached_at>=?",
            (*cities, cutoff),
        ).fetchone()
    return row[0] if row else 0


def count_cities_with_pending_search(db_path: Path, cities: list[str], topics_count: int, ttl_days: int) -> int:
    """Count cities that have at least one (city, topic) pair with no valid search cache."""
    if not db_path.exists() or not cities or topics_count == 0:
        return len(cities)
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
    placeholders = ",".join("?" * len(cities))
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT city, COUNT(DISTINCT topic) FROM search_cache"
            f" WHERE city IN ({placeholders}) AND cached_at>=? GROUP BY city",
            (*cities, cutoff),
        ).fetchall()
    fully_cached = sum(1 for _, cnt in rows if cnt >= topics_count)
    return len(cities) - fully_cached


def get_covered_pairs(db_path: Path) -> set[tuple[str, str]]:
    """Return all (city, topic) pairs that have a search cache entry."""
    if not db_path.exists():
        return set()
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT city, topic FROM search_cache").fetchall()
    return {(r[0], r[1]) for r in rows}


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
            old_data = json.loads(existing[0]) if existing else None
            if old_data:
                prev_urls: list[str] = old_data.get("source_urls") or []
                new_urls: list[str] = record.get("source_urls") or []
                merged = list(dict.fromkeys(new_urls + prev_urls))
                record = {**record, "source_urls": merged}
                # Merge community_ids
                prev_cids = old_data.get("community_ids") or []
                new_cids = record.get("community_ids") or []
                record["community_ids"] = list(dict.fromkeys(new_cids + prev_cids))
            conn.execute("""
                INSERT INTO venues (record_key, venue_id, city, data, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    data=excluded.data, venue_id=excluded.venue_id, updated_at=excluded.updated_at
            """, (key, record.get("venue_id", ""), record["city"],
                  json.dumps(record, ensure_ascii=False), now))
            _log_changes(conn, "venue_history", "venue_id",
                         record.get("venue_id", ""), _VENUE_HISTORY_FIELDS, old_data, record)
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


def get_venues_by_city_topic(db_path: Path, city: str, topic: str) -> list[dict]:
    """Return venues in city whose welcomed_topics includes topic."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT v.data FROM venues v, json_each(json_extract(v.data, '$.welcomed_topics')) t
            WHERE v.city = ? AND t.value = ?
            ORDER BY v.id
            """,
            (city, topic),
        ).fetchall()
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


def delete_leader_persons_for_community(db_path: Path, community_name: str, city: str) -> int:
    """Delete all role='leader' persons for a community before re-inserting clean parsed ones."""
    if not db_path.exists():
        return 0
    with _connect(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM persons WHERE city=? AND role='leader' "
            "AND json_extract(data,'$.community_name')=?",
            (city, community_name)
        )
        conn.commit()
        return cur.rowcount


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
            old_data = json.loads(existing[0]) if existing else None
            if old_data:
                prev_urls = old_data.get("source_urls") or []
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
            _log_changes(conn, "person_history", "person_id",
                         record.get("person_id", ""), _PERSON_HISTORY_FIELDS, old_data, record)
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


def get_all_persons(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT data FROM persons ORDER BY city, id").fetchall()
    return [json.loads(r[0]) for r in rows]


def get_person_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT city, COUNT(*) FROM persons GROUP BY city").fetchall()
    return {r[0]: r[1] for r in rows}


def search_all(
    db_path: Path,
    query: str,
    limit: int = 20,
) -> dict[str, list[dict]]:
    """Search communities, venues, and persons by name or description.

    Returns a dict with lists of matching records from each table.
    Empty query returns empty results. Hidden communities are excluded.
    """
    if not db_path.exists() or not query.strip():
        return {"communities": [], "venues": [], "persons": []}

    pattern = f"%{query}%"
    with _connect(db_path) as conn:
        community_rows = conn.execute(
            "SELECT data FROM communities WHERE hidden=0 AND ("
            "  json_extract(data,'$.name') LIKE ? OR"
            "  json_extract(data,'$.description') LIKE ?"
            ") ORDER BY city, id LIMIT ?",
            (pattern, pattern, limit),
        ).fetchall()
        venue_rows = conn.execute(
            "SELECT data FROM venues WHERE ("
            "  json_extract(data,'$.name') LIKE ? OR"
            "  json_extract(data,'$.description') LIKE ?"
            ") ORDER BY city, id LIMIT ?",
            (pattern, pattern, limit),
        ).fetchall()
        person_rows = conn.execute(
            "SELECT data FROM persons WHERE"
            "  json_extract(data,'$.name') LIKE ? ORDER BY city, id LIMIT ?",
            (pattern, limit),
        ).fetchall()

    return {
        "communities": [json.loads(r[0]) for r in community_rows],
        "venues": [json.loads(r[0]) for r in venue_rows],
        "persons": [json.loads(r[0]) for r in person_rows],
    }


# ── Not-community reports ─────────────────────────────────────────────────────

def save_not_community_report(
    db_path: Path,
    community_id: str,
    community_name: str,
    city: str,
    topic: str,
    source_url: str,
    page_url: str,
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO not_community_reports"
            " (community_id, community_name, city, topic, source_url, page_url, reported_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (community_id, community_name, city, topic, source_url, page_url, now),
        )
        conn.commit()
        return cur.lastrowid


def get_not_community_reports(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, community_id, community_name, city, topic, source_url, page_url, reported_at"
            " FROM not_community_reports ORDER BY reported_at DESC"
        ).fetchall()
    return [
        {
            "id": r[0], "community_id": r[1], "community_name": r[2],
            "city": r[3], "topic": r[4], "source_url": r[5],
            "page_url": r[6], "reported_at": r[7],
        }
        for r in rows
    ]


def delete_not_community_report(db_path: Path, report_id: int) -> None:
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM not_community_reports WHERE id=?", (report_id,))
        conn.commit()


# ── City requests ──────────────────────────────────────────────────────────────

def get_scope_stats(
    db_path: Path,
    extract_fp: str,
    venue_fp: str,
    person_fp: str,
    cities: list[str] | None = None,
) -> dict:
    """Count pages that need each type of AI processing given current fingerprints."""
    if not db_path.exists():
        return {"with_text": 0, "extract_match": 0, "venue_match": 0, "person_match": 0}
    city_filter = ""
    city_params: list = []
    if cities:
        placeholders = ",".join("?" * len(cities))
        city_filter = f" WHERE city IN ({placeholders})"
        city_params = list(cities)
    with _connect(db_path) as conn:
        row = conn.execute(f"""
            SELECT
                SUM(CASE WHEN scraped_at IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN scraped_at IS NOT NULL
                         AND extract_fingerprint = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN scraped_at IS NOT NULL
                         AND venue_fingerprint = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN scraped_at IS NOT NULL
                         AND person_fingerprint = ? THEN 1 ELSE 0 END),
                SUM(CASE WHEN scraped_at IS NOT NULL
                         AND extract_fingerprint = ?
                         AND venue_fingerprint = ?
                         AND person_fingerprint = ? THEN 1 ELSE 0 END)
            FROM cache_pages{city_filter}
        """, (extract_fp, venue_fp, person_fp, extract_fp, venue_fp, person_fp, *city_params)).fetchone()
    return {
        "with_text":     int(row[0] or 0),
        "extract_match": int(row[1] or 0),
        "venue_match":   int(row[2] or 0),
        "person_match":  int(row[3] or 0),
        "fully_matched": int(row[4] or 0),
    }


def save_city_request(db_path: Path, city_name: str, email: str = "") -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO city_requests (city_name, email, created_at) VALUES (?, ?, ?)",
            (city_name.strip(), email.strip(), now),
        )
        conn.commit()


def get_city_requests(db_path: Path) -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, city_name, email, created_at FROM city_requests ORDER BY created_at DESC"
        ).fetchall()
    return [{"id": r[0], "city_name": r[1], "email": r[2], "created_at": r[3]} for r in rows]


# ── Duplicate candidates ───────────────────────────────────────────────────────

def insert_duplicate_candidate(
    db_path: Path,
    entity_type: str,
    winner_id: str,
    loser_id: str,
    winner_key: str,
    loser_key: str,
    similarity: float,
    signal: str,
) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        existing = conn.execute(
            "SELECT id FROM duplicate_candidates"
            " WHERE entity_type=? AND winner_key=? AND loser_key=?",
            (entity_type, winner_key, loser_key),
        ).fetchone()
        if existing:
            return False
        cursor = conn.execute("""
            INSERT INTO duplicate_candidates
              (entity_type, winner_id, loser_id, winner_key, loser_key, similarity, signal, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (entity_type, winner_id, loser_id, winner_key, loser_key, similarity, signal, now))
        conn.commit()
        return cursor.rowcount > 0


def get_duplicate_candidates(
    db_path: Path,
    entity_type: str | None = None,
    resolved: bool = False,
) -> list[dict]:
    if not db_path.exists():
        return []
    clauses = []
    params: list = []
    if resolved:
        clauses.append("resolution IS NOT NULL")
    else:
        clauses.append("resolution IS NULL")
    if entity_type:
        clauses.append("entity_type = ?")
        params.append(entity_type)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with _connect(db_path) as conn:
        cursor = conn.execute(
            f"SELECT * FROM duplicate_candidates {where} ORDER BY similarity DESC, detected_at DESC",
            params,
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def resolve_duplicate_candidate(db_path: Path, candidate_id: int, resolution: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE duplicate_candidates SET resolution=?, resolved_at=? WHERE id=?",
            (resolution, now, candidate_id),
        )
        conn.commit()


def delete_duplicate_candidate(db_path: Path, candidate_id: int) -> None:
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM duplicate_candidates WHERE id=?", (candidate_id,))
        conn.commit()


def merge_community_into(
    db_path: Path,
    winner_key: str,
    loser_key: str,
    candidate_id: int | None = None,
) -> None:
    with _connect(db_path) as conn:
        winner_row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (winner_key,)
        ).fetchone()
        loser_row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (loser_key,)
        ).fetchone()
        if not winner_row or not loser_row:
            return
        winner_data = json.loads(winner_row[0])
        loser_data = json.loads(loser_row[0])
        # Merge source_urls
        w_urls = list(winner_data.get("source_urls") or [])
        if winner_data.get("source_url") and winner_data["source_url"] not in w_urls:
            w_urls = [winner_data["source_url"]] + w_urls
        l_urls = list(loser_data.get("source_urls") or [])
        if loser_data.get("source_url") and loser_data["source_url"] not in l_urls:
            l_urls = [loser_data["source_url"]] + l_urls
        merged_urls = list(dict.fromkeys(w_urls + l_urls))
        winner_data["source_urls"] = merged_urls
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE communities SET data=?, updated_at=? WHERE record_key=?",
            (json.dumps(winner_data, ensure_ascii=False), now, winner_key),
        )
        conn.execute(
            "UPDATE communities SET hidden=1 WHERE record_key=?",
            (loser_key,),
        )
        if candidate_id is not None:
            now2 = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "UPDATE duplicate_candidates SET resolution=?, resolved_at=? WHERE id=?",
                ("merged", now2, candidate_id),
            )
        conn.commit()


def save_community_data(db_path: Path, record_key: str, data: dict) -> None:
    """Overwrite the data blob for a community record."""
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE communities SET data=?, updated_at=? WHERE record_key=?",
            (json.dumps(data, ensure_ascii=False), now, record_key),
        )
        conn.commit()


# ── Edit requests ──────────────────────────────────────────────────────────────

def save_edit_request(
    db_path: Path,
    entity_type: str,
    entity_id: str,
    entity_name: str,
    entity_city: str,
    entity_topic: str,
    record_key: str,
    change_type: str,
    new_value: str | None,
    notes: str,
    email: str,
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO edit_requests"
            " (entity_type, entity_id, entity_name, entity_city, entity_topic,"
            "  record_key, change_type, new_value, notes, email, submitted_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (entity_type, entity_id, entity_name, entity_city, entity_topic,
             record_key, change_type, new_value, notes, email, now),
        )
        conn.commit()
        return cur.lastrowid


def get_edit_requests(db_path: Path, status: str = "pending") -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT * FROM edit_requests WHERE status=? ORDER BY submitted_at DESC",
            (status,),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def resolve_edit_request(db_path: Path, request_id: int, status: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE edit_requests SET status=?, reviewed_at=? WHERE id=?",
            (status, now, request_id),
        )
        conn.commit()


def apply_community_edit(
    db_path: Path,
    record_key: str,
    change_type: str,
    new_value: str | None,
) -> bool:
    """Apply an approved edit to a community record. Returns True if found and applied."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (record_key,)
        ).fetchone()
        if not row:
            return False
        data = json.loads(row[0])
        now = datetime.now(timezone.utc).isoformat()
        if change_type in ("archive", "delete"):
            conn.execute(
                "UPDATE communities SET hidden=1, updated_at=? WHERE record_key=?",
                (now, record_key),
            )
        elif change_type == "wrong_city":
            data["city"] = new_value
            conn.execute(
                "UPDATE communities SET city=?, data=?, updated_at=? WHERE record_key=?",
                (new_value, json.dumps(data, ensure_ascii=False), now, record_key),
            )
        elif change_type == "wrong_topic":
            data["topic"] = new_value
            conn.execute(
                "UPDATE communities SET topic=?, data=?, updated_at=? WHERE record_key=?",
                (new_value, json.dumps(data, ensure_ascii=False), now, record_key),
            )
        elif change_type == "name_correction":
            data["name"] = new_value
            conn.execute(
                "UPDATE communities SET data=?, updated_at=? WHERE record_key=?",
                (json.dumps(data, ensure_ascii=False), now, record_key),
            )
        else:
            return False
        conn.commit()
    return True


# ── Community Submissions ─────────────────────────────────────────────────────

def save_community_submission(
    db_path: Path,
    name: str,
    city: str,
    topic: str,
    source_url: str,
    submitter_email: str | None,
) -> int:
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO community_submissions (name, city, topic, source_url, submitter_email, submitted_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending')",
            (name, city, topic, source_url, submitter_email,
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return cur.lastrowid


def get_community_submissions(db_path: Path, status: str = "pending") -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT id, name, city, topic, source_url, submitter_email, submitted_at, status "
            "FROM community_submissions WHERE status=? ORDER BY submitted_at DESC",
            (status,),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def resolve_community_submission(db_path: Path, sub_id: int, status: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE community_submissions SET status=? WHERE id=?",
            (status, sub_id),
        )
        conn.commit()


# ── Recategorize suggestions ───────────────────────────────────────────────────

def get_other_communities(db_path: Path) -> list[dict]:
    """Return all non-hidden communities with topic='other'."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT record_key, data FROM communities "
            "WHERE json_extract(data,'$.topic')='other' AND hidden=0 "
            "ORDER BY json_extract(data,'$.city'), json_extract(data,'$.name')"
        ).fetchall()
    return [{"record_key": r[0], **json.loads(r[1])} for r in rows]


def upsert_recategorize_suggestion(
    db_path: Path,
    record_key: str,
    community_name: str,
    city: str,
    description: str,
    suggested_topic: str,
    confidence: float,
    reasoning: str,
    status: str,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """INSERT INTO recategorize_suggestions
               (record_key, community_name, city, description,
                suggested_topic, confidence, reasoning, status, created_at)
               VALUES (?,?,?,?,?,?,?,?,datetime('now'))
               ON CONFLICT(record_key) DO UPDATE SET
                 suggested_topic=excluded.suggested_topic,
                 confidence=excluded.confidence,
                 reasoning=excluded.reasoning,
                 status=excluded.status,
                 created_at=excluded.created_at""",
            (record_key, community_name, city, description or "",
             suggested_topic, confidence, reasoning, status),
        )
        conn.commit()


def get_recategorize_suggestions(db_path: Path, status: str = "pending") -> list[dict]:
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT id, record_key, community_name, city, description, "
            "suggested_topic, confidence, reasoning, status, created_at "
            "FROM recategorize_suggestions WHERE status=? "
            "ORDER BY confidence DESC",
            (status,),
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def apply_recategorize_suggestion(db_path: Path, record_key: str, new_topic: str) -> None:
    """Update the community's topic and mark the suggestion as applied."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (record_key,)
        ).fetchone()
        if not row:
            return
        data = json.loads(row[0])
        old_name = data.get("name", "")
        old_city = data.get("city", "")
        new_key = _community_record_key(old_name, old_city, new_topic)
        data["topic"] = new_topic
        conn.execute(
            "UPDATE communities SET record_key=?, data=? WHERE record_key=?",
            (new_key, json.dumps(data, ensure_ascii=False), record_key),
        )
        conn.execute(
            "UPDATE recategorize_suggestions SET status='applied' WHERE record_key=?",
            (record_key,),
        )
        conn.commit()


def update_recategorize_status(db_path: Path, suggestion_id: int, status: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE recategorize_suggestions SET status=? WHERE id=?",
            (status, suggestion_id),
        )
        conn.commit()


def get_data_quality_stats(db_path: Path) -> dict:
    empty: dict = {
        "total": 0, "visible": 0, "hidden": 0,
        "cities": 0, "topics": 0,
        "has_website": 0, "has_contact": 0, "has_description": 0, "has_any": 0,
        "city_rows": [], "topic_counts": {},
    }
    if not db_path.exists():
        return empty
    with _connect(db_path) as conn:
        row = conn.execute("""
            SELECT
              COUNT(*) as total,
              SUM(CASE WHEN hidden=0 THEN 1 ELSE 0 END) as visible,
              SUM(CASE WHEN hidden=1 THEN 1 ELSE 0 END) as hidden,
              COUNT(DISTINCT CASE WHEN hidden=0 THEN city END) as cities,
              COUNT(DISTINCT CASE WHEN hidden=0 THEN topic END) as topics,
              SUM(CASE WHEN hidden=0
                   AND json_extract(data,'$.website') IS NOT NULL
                   AND json_extract(data,'$.website') != '' THEN 1 ELSE 0 END) as has_website,
              SUM(CASE WHEN hidden=0
                   AND json_extract(data,'$.contact') IS NOT NULL
                   AND json_extract(data,'$.contact') != '' THEN 1 ELSE 0 END) as has_contact,
              SUM(CASE WHEN hidden=0
                   AND length(COALESCE(json_extract(data,'$.description'),'')) > 50
                   THEN 1 ELSE 0 END) as has_description,
              SUM(CASE WHEN hidden=0 AND (
                   (json_extract(data,'$.website') IS NOT NULL AND json_extract(data,'$.website') != '')
                   OR
                   (json_extract(data,'$.contact') IS NOT NULL AND json_extract(data,'$.contact') != '')
                 ) THEN 1 ELSE 0 END) as has_any
            FROM communities
        """).fetchone()
        city_rows = conn.execute("""
            SELECT city, COUNT(*) as cnt,
              SUM(CASE WHEN json_extract(data,'$.website') IS NOT NULL
                       AND json_extract(data,'$.website') != '' THEN 1 ELSE 0 END) as w,
              SUM(CASE WHEN json_extract(data,'$.contact') IS NOT NULL
                       AND json_extract(data,'$.contact') != '' THEN 1 ELSE 0 END) as c
            FROM communities
            WHERE hidden=0
            GROUP BY city
            ORDER BY cnt DESC
            LIMIT 20
        """).fetchall()
    topic_counts = get_topic_counts(db_path)
    return {
        "total": row[0] or 0,
        "visible": row[1] or 0,
        "hidden": row[2] or 0,
        "cities": row[3] or 0,
        "topics": row[4] or 0,
        "has_website": row[5] or 0,
        "has_contact": row[6] or 0,
        "has_description": row[7] or 0,
        "has_any": row[8] or 0,
        "city_rows": [{"city": r[0], "cnt": r[1], "w": r[2] or 0, "c": r[3] or 0} for r in city_rows],
        "topic_counts": topic_counts,
    }


def get_activity_timeline(db_path: Path, period: str) -> list[dict]:
    """Return per-bucket activity counts for the given period.

    period: "24h" (hourly, last 24 h), "7d" (daily, last 7 d), "12m" (monthly, last 12 m)
    """
    from datetime import timedelta

    if not db_path.exists():
        return []

    now = datetime.now(timezone.utc)

    if period == "24h":
        fmt = "%Y-%m-%dT%H"
        since = now - timedelta(hours=24)
        since_sql = "datetime('now', '-24 hours')"
        buckets = [(now - timedelta(hours=i)).strftime(fmt) for i in range(23, -1, -1)]
        display = {b: b[-2:] + ":00" for b in buckets}
    elif period == "7d":
        fmt = "%Y-%m-%d"
        since = now - timedelta(days=7)
        since_sql = "datetime('now', '-7 days')"
        buckets = [(now - timedelta(days=i)).strftime(fmt) for i in range(6, -1, -1)]
        display = {b: datetime.strptime(b, "%Y-%m-%d").strftime("%b %d") for b in buckets}
    else:  # "12m"
        fmt = "%Y-%m"
        since_sql = "datetime('now', '-12 months')"
        buckets = []
        display = {}
        for i in range(11, -1, -1):
            m = now.month - i
            y = now.year
            while m <= 0:
                m += 12
                y -= 1
            key = f"{y:04d}-{m:02d}"
            buckets.append(key)
            display[key] = datetime(y, m, 1).strftime("%b %Y")

    rows: dict[str, dict] = {b: {
        "bucket": b, "label": display[b],
        "scrapes": 0, "extractions": 0,
        "enrich_scrapes": 0, "enrich_ai": 0,
        "new_communities": 0, "community_changes": 0,
        "new_venues": 0, "new_persons": 0,
    } for b in buckets}

    def _run(conn: sqlite3.Connection, sql: str, key: str) -> None:
        for bkt, cnt in conn.execute(sql).fetchall():
            if bkt and bkt in rows:
                rows[bkt][key] = cnt

    strftime_col = f"strftime('{fmt}', {{col}})"

    with _connect(db_path) as conn:
        _run(conn, f"""
            SELECT {strftime_col.format(col='scraped_at')}, COUNT(*)
            FROM cache_pages WHERE scraped_at >= {since_sql}
            GROUP BY 1
        """, "scrapes")
        _run(conn, f"""
            SELECT {strftime_col.format(col='extracted_at')}, COUNT(*)
            FROM cache_pages WHERE extracted_at >= {since_sql}
            GROUP BY 1
        """, "extractions")
        _run(conn, f"""
            SELECT {strftime_col.format(col="json_extract(data,'$.enrich_scraped_at')")}, COUNT(*)
            FROM cache_pages
            WHERE json_extract(data,'$.enrich_scraped_at') >= {since_sql}
            GROUP BY 1
        """, "enrich_scrapes")
        _run(conn, f"""
            SELECT {strftime_col.format(col="json_extract(data,'$.enrich_extracted_at')")}, COUNT(*)
            FROM cache_pages
            WHERE json_extract(data,'$.enrich_extracted_at') >= {since_sql}
            GROUP BY 1
        """, "enrich_ai")
        _run(conn, f"""
            SELECT {strftime_col.format(col='changed_at')}, COUNT(*)
            FROM community_history
            WHERE field='__created__' AND changed_at >= {since_sql}
            GROUP BY 1
        """, "new_communities")
        _run(conn, f"""
            SELECT {strftime_col.format(col='changed_at')}, COUNT(*)
            FROM community_history
            WHERE field!='__created__' AND changed_at >= {since_sql}
            GROUP BY 1
        """, "community_changes")
        _run(conn, f"""
            SELECT {strftime_col.format(col='first_seen')}, COUNT(*)
            FROM (
                SELECT venue_id, MIN(changed_at) AS first_seen
                FROM venue_history
                WHERE field='__created__'
                GROUP BY venue_id
            )
            WHERE first_seen >= {since_sql}
            GROUP BY 1
        """, "new_venues")
        # Use MIN(changed_at) per person_id so delete+reinsert cycles only count once.
        _run(conn, f"""
            SELECT {strftime_col.format(col='first_seen')}, COUNT(*)
            FROM (
                SELECT person_id, MIN(changed_at) AS first_seen
                FROM person_history
                WHERE field='__created__'
                GROUP BY person_id
            )
            WHERE first_seen >= {since_sql}
            GROUP BY 1
        """, "new_persons")

    return [rows[b] for b in buckets]


# ── Outclick tracking ─────────────────────────────────────────────────────────

def log_outclick(db_path: Path, community_id: str, url: str, link_type: str) -> None:
    try:
        with _connect(db_path) as conn:
            conn.execute(
                "INSERT INTO outclick_events (community_id, url, link_type) VALUES (?, ?, ?)",
                (community_id, url, link_type),
            )
            conn.commit()
    except Exception:
        pass


def get_outclick_stats(db_path: Path) -> dict:
    empty: dict = {"total": 0, "total_30d": 0, "top_communities": [], "by_type": []}
    if not db_path.exists():
        return empty
    with _connect(db_path) as conn:
        tbl = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='outclick_events'"
        ).fetchone()
        if not tbl:
            return empty
        total = conn.execute("SELECT COUNT(*) FROM outclick_events").fetchone()[0]
        total_30d = conn.execute(
            "SELECT COUNT(*) FROM outclick_events WHERE clicked_at >= datetime('now','-30 days')"
        ).fetchone()[0]
        top = conn.execute("""
            SELECT o.community_id,
                   json_extract(c.data,'$.name') AS name,
                   json_extract(c.data,'$.city') AS city,
                   COUNT(*) AS clicks
            FROM outclick_events o
            LEFT JOIN communities c ON c.community_id = o.community_id
            WHERE o.clicked_at >= datetime('now','-30 days')
            GROUP BY o.community_id
            ORDER BY clicks DESC
            LIMIT 15
        """).fetchall()
        by_type = conn.execute("""
            SELECT link_type, COUNT(*) AS cnt
            FROM outclick_events
            WHERE clicked_at >= datetime('now','-30 days')
            GROUP BY link_type
            ORDER BY cnt DESC
        """).fetchall()
    return {
        "total": total,
        "total_30d": total_30d,
        "top_communities": [
            {"community_id": r[0], "name": r[1] or r[0], "city": r[2] or "", "clicks": r[3]}
            for r in top
        ],
        "by_type": [{"type": r[0], "cnt": r[1]} for r in by_type],
    }
