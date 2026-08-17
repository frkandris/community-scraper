import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import structlog

from .identity import (
    community_record_key as _community_record_key,
    normalized_match_key,
    person_record_key as _person_record_key,
    venue_record_key as _venue_record_key,
)

_UNICODE_RECORD_KEYS_MIGRATION = "unicode_record_keys_v2"
log = structlog.get_logger()


#: WAL is enabled once per process, not per connection — the mode is a property
#: of the database file and the PRAGMA is a write, so doing it on every connect
#: adds a lock acquisition to every query.
_wal_enabled: set[str] = set()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    key = str(db_path)
    if key not in _wal_enabled:
        # Write-Ahead Logging lets readers proceed while a writer holds the
        # database. Without it the pipeline's writes block every HTTP request:
        # 2026-08-16 produced "database is locked" and 30-second page loads once
        # the Hungarian import pushed the write volume up. Readers and one
        # writer can now run concurrently.
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            # NORMAL is the standard companion to WAL: durable across process
            # crashes, only at risk in an OS-level crash, and far fewer fsyncs.
            conn.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.Error as exc:  # e.g. a read-only mount
            log.warning("wal_enable_failed", error=str(exc))
        _wal_enabled.add(key)
    return conn


def _migrate_unicode_record_keys(conn: sqlite3.Connection) -> None:
    """Rewrite legacy ASCII-only keys and every persisted reference once."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name       TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
    """)
    if conn.execute(
        "SELECT 1 FROM schema_migrations WHERE name=?",
        (_UNICODE_RECORD_KEYS_MIGRATION,),
    ).fetchone():
        return

    mappings: dict[str, dict[str, str]] = {
        "community": {},
        "venue": {},
        "person": {},
    }
    for old_key, data_json in conn.execute("SELECT record_key, data FROM communities"):
        data = json.loads(data_json)
        mappings["community"][old_key] = _community_record_key(
            data.get("name", ""), data.get("city", ""), data.get("topic", "")
        )
    for old_key, data_json in conn.execute("SELECT record_key, data FROM venues"):
        data = json.loads(data_json)
        mappings["venue"][old_key] = _venue_record_key(
            data.get("name", ""), data.get("city", "")
        )
    for old_key, data_json in conn.execute("SELECT record_key, data FROM persons"):
        data = json.loads(data_json)
        mappings["person"][old_key] = _person_record_key(
            data.get("name", ""),
            data.get("city", ""),
            data.get("role", "leader"),
            data.get("community_name", ""),
        )

    table_for_type = {
        "community": "communities",
        "venue": "venues",
        "person": "persons",
    }
    for entity_type, key_map in mappings.items():
        table = table_for_type[entity_type]
        for old_key, new_key in key_map.items():
            if old_key == new_key:
                continue
            conn.execute(
                f"UPDATE {table} SET record_key=? WHERE record_key=?",
                (new_key, old_key),
            )
            conn.execute(
                "UPDATE duplicate_candidates SET winner_key=?"
                " WHERE entity_type=? AND winner_key=?",
                (new_key, entity_type, old_key),
            )
            conn.execute(
                "UPDATE duplicate_candidates SET loser_key=?"
                " WHERE entity_type=? AND loser_key=?",
                (new_key, entity_type, old_key),
            )
            conn.execute(
                "UPDATE edit_requests SET record_key=?"
                " WHERE entity_type=? AND record_key=?",
                (new_key, entity_type, old_key),
            )

    conn.execute(
        "INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)",
        (_UNICODE_RECORD_KEYS_MIGRATION, datetime.now(timezone.utc).isoformat()),
    )


#: Databases already initialised in this process. init_db is documented as safe
#: to call on every request, and a dozen routes do — but it issues CREATE TABLE
#: and ALTER TABLE, i.e. it takes a *write* lock. Under a writing pipeline that
#: turned every admin page load into a lock wait. The schema cannot change
#: mid-process, so once is enough.
_initialised: set[str] = set()


def init_db(db_path: Path, force: bool = False) -> None:
    """Create/migrate every table. Idempotent, and now also *cheap to call*.

    Routes are documented as free to call this per request, and a dozen do —
    but the body is CREATE TABLE / ALTER TABLE, which takes a write lock. With
    the pipeline writing concurrently that produced "database is locked" and
    multi-second page loads (2026-08-16). The schema cannot change within a
    process, so the work runs once per path; pass force=True in tests that
    rebuild a database at the same location.
    """
    key = str(db_path)
    if not force and key in _initialised:
        return
    with _connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at  TEXT NOT NULL,
                finished_at TEXT,
                run_mode    TEXT NOT NULL DEFAULT 'full',
                success     INTEGER NOT NULL DEFAULT 1,
                search_log  TEXT,
                error       TEXT
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
        try:
            conn.execute("ALTER TABLE runs ADD COLUMN error TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            # 'ok' | 'warning' | 'aborted'. NULL on rows written before
            # 2026-07-31; readers fall back to the boolean via _OUTCOME_SQL.
            conn.execute("ALTER TABLE runs ADD COLUMN outcome TEXT")
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
            conn.execute("ALTER TABLE communities ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0")
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
        try:
            # Quality score (0-100) of the model that produced the cached
            # extraction. NULL = pre-router row, treated as the lowest tier so
            # an upgrade sweep can reconsider it. Read by the router's
            # re-extraction policy; never part of any cache key.
            conn.execute("ALTER TABLE cache_pages ADD COLUMN extract_quality INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE cache_pages ADD COLUMN extract_model TEXT")
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
                collected_at TEXT,
                PRIMARY KEY (city, topic)
            )
        """)
        try:
            conn.execute("ALTER TABLE search_cache ADD COLUMN collected_at TEXT")
        except sqlite3.OperationalError:
            pass
        else:
            # Legacy rows were produced by runs that already attempted their fetch
            # batch. Treat them as terminally collected so permanently unreadable
            # URLs do not force a full historical retry after this migration.
            conn.execute(
                "UPDATE search_cache SET collected_at=cached_at WHERE collected_at IS NULL"
            )

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
            CREATE TABLE IF NOT EXISTS wrong_city_candidates (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                record_key     TEXT NOT NULL,
                community_id   TEXT NOT NULL DEFAULT '',
                mentioned_city TEXT NOT NULL,
                field          TEXT NOT NULL,
                snippet        TEXT NOT NULL DEFAULT '',
                matched_text   TEXT NOT NULL DEFAULT '',
                detected_at    TEXT NOT NULL,
                resolved_at    TEXT,
                resolution     TEXT
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_wrong_city_pair
            ON wrong_city_candidates(record_key, mentioned_city)
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS traffic_daily (
                day       TEXT NOT NULL,
                site      TEXT NOT NULL,
                pageviews INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (day, site)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS traffic_visitors (
                day          TEXT NOT NULL,
                site         TEXT NOT NULL,
                visitor_hash TEXT NOT NULL,
                PRIMARY KEY (day, site, visitor_hash)
            )
        """)

        # Per-provider, per-UTC-day quota ledger for the free-tier model router.
        # Persisted rather than in-memory because free daily allowances are
        # calendar-day budgets: a container restart must not hand a provider a
        # fresh 14,400 requests it does not actually have.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS provider_usage (
                day             TEXT NOT NULL,
                provider        TEXT NOT NULL,
                calls           INTEGER NOT NULL DEFAULT 0,
                failures        INTEGER NOT NULL DEFAULT 0,
                rate_limits     INTEGER NOT NULL DEFAULT 0,
                -- Requests served on the day we first hit a 429. The published
                -- limits are stale or unpublished for most providers, so the
                -- observed ceiling is what actually governs routing.
                observed_limit  INTEGER,
                blocked_until   REAL NOT NULL DEFAULT 0,
                last_error      TEXT,
                PRIMARY KEY (day, provider)
            )
        """)

        _migrate_unicode_record_keys(conn)
        conn.commit()
    _initialised.add(key)


# ── Runs ──────────────────────────────────────────────────────────────────────

#: Reads the three-state outcome, falling back to the legacy boolean for rows
#: written before the column existed (2026-07-31). Those rows only ever
#: distinguished clean from not-clean, so a failed one maps to 'aborted'.
_OUTCOME_SQL = (
    "COALESCE(outcome, CASE WHEN success=1 THEN 'ok' ELSE 'aborted' END)"
)


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
    error: str | None = None,
    outcome: str | None = None,
) -> None:
    """outcome: 'ok' | 'warning' | 'aborted' (see pipeline.classify_run_outcome).

    `success` stays for every existing reader and means "the run completed" —
    a warning run is successful. Callers that pass `outcome` should derive
    `success` from it the same way; callers that don't get the legacy mapping.
    """
    if outcome is None:
        outcome = "ok" if success else "aborted"
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE runs SET finished_at=?, success=?, search_log=?, new_records=?,"
            " error=?, outcome=? WHERE id=?",
            (finished_at.isoformat(), int(success), search_log, new_records,
             error, outcome, run_id),
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
                f"SELECT id, run_mode, finished_at, success, {_OUTCOME_SQL}"
                " FROM runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                return {"id": row[0], "run_mode": row[1], "finished_at": row[2],
                        "success": bool(row[3]), "outcome": row[4]}
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
                "SELECT id, started_at, finished_at, run_mode, success, COALESCE(new_records, 0), "
                f"{_OUTCOME_SQL} FROM runs ORDER BY id DESC LIMIT ?",
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
                "outcome": r[6],
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
                "SELECT id, started_at, finished_at, run_mode, success, search_log, "
                f"{_OUTCOME_SQL} FROM runs WHERE id = ?",
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
                "outcome": row[6],
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
    "name", "description", "short_description", "long_description", "history",
    "website", "tags", "social_links",
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


# SEO-enrichment fields written by scraper/enrich.py, never by extraction. They
# must survive a re-extraction that rebuilds the record from cache, so they are
# carried forward from the existing row whenever the incoming record lacks them.
_PRESERVED_ENRICHMENT_FIELDS = ("short_description", "long_description")


def _merge_source_urls(old_data: dict | None, record: dict) -> dict:
    if not old_data:
        return record
    prev_urls: list[str] = old_data.get("source_urls") or []
    if old_data.get("source_url") and old_data["source_url"] not in prev_urls:
        prev_urls = [old_data["source_url"]] + prev_urls
    new_urls: list[str] = record.get("source_urls") or []
    if record.get("source_url") and record["source_url"] not in new_urls:
        new_urls = [record["source_url"]] + new_urls
    merged = {**record, "source_urls": list(dict.fromkeys(new_urls + prev_urls))}
    for field in _PRESERVED_ENRICHMENT_FIELDS:
        if not merged.get(field) and old_data.get(field):
            merged[field] = old_data[field]
    return merged


# Fields that change on every extraction without any user-visible content change;
# excluded from the change-detection fingerprint so `updated_at`/`<lastmod>` stay stable.
# `enrich_attempted_at` is a retry marker (dropped on re-extraction) — must be volatile
# or its removal reads as a change and falsely bumps <lastmod>.
_VOLATILE_COMMUNITY_FIELDS = {"extracted_at", "enrich_attempted_at"}


def _community_content_fingerprint(data: dict) -> str:
    # Drop volatile fields AND null/empty values, so newly-added optional fields
    # (e.g. short_description/long_description serialized as None on the first save
    # of a pre-existing record) don't register as a content change and churn every
    # page's <lastmod>.
    return json.dumps(
        {k: v for k, v in data.items()
         if k not in _VOLATILE_COMMUNITY_FIELDS and v not in (None, "", [], {})},
        ensure_ascii=False, sort_keys=True,
    )


def _bulk_upsert_communities(
    conn: sqlite3.Connection,
    records: list[dict],
    previous: dict[str, dict] | None = None,
    prev_updated: dict[str, str] | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    for record in records:
        key = _community_record_key(record["name"], record["city"], record["topic"])

        # Prefer pre-delete snapshot; fall back to live row (used by bulk_upsert_communities)
        old_data = (previous or {}).get(key)
        old_updated = (prev_updated or {}).get(key)
        if old_data is None:
            existing_row = conn.execute(
                "SELECT data, updated_at FROM communities WHERE record_key=?", (key,)
            ).fetchone()
            if existing_row:
                old_data = json.loads(existing_row[0])
                old_updated = existing_row[1]

        record = _merge_source_urls(old_data, record)
        data_str = json.dumps(record, ensure_ascii=False)
        # Only advance updated_at on a real content change, so it is a trustworthy
        # sitemap <lastmod> and a re-extraction that reproduces identical data does
        # not churn the whole corpus's freshness dates. (replace_communities_for_topic
        # DELETEs first, so the ON CONFLICT branch never fires there — the preserved
        # timestamp must be computed here and passed as the inserted value.)
        row_updated = now
        if old_data is not None and old_updated and \
                _community_content_fingerprint(record) == _community_content_fingerprint(old_data):
            row_updated = old_updated

        conn.execute("""
            INSERT INTO communities (record_key, community_id, city, topic, data, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_key) DO UPDATE SET
                data=excluded.data,
                community_id=excluded.community_id,
                updated_at=excluded.updated_at
        """, (key, record.get("community_id", ""), record["city"], record["topic"],
              data_str, row_updated))

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
        # Snapshot existing records before delete so history can diff against them.
        # hidden is moderation state that must survive the DELETE+reinsert —
        # otherwise a merged/reported (hidden) community resurfaces publicly on
        # the next scrape.
        rows = conn.execute(
            "SELECT data, hidden, updated_at FROM communities WHERE city=? AND topic=?",
            (city, topic)
        ).fetchall()
        previous: dict[str, dict] = {}
        prev_updated: dict[str, str] = {}
        hidden_keys: set[str] = set()
        for data_str, hidden, updated_at in rows:
            d = json.loads(data_str)
            key = _community_record_key(d["name"], d["city"], d["topic"])
            previous[key] = d
            prev_updated[key] = updated_at
            if hidden:
                hidden_keys.add(key)

        conn.execute("DELETE FROM communities WHERE city=? AND topic=?", (city, topic))
        _bulk_upsert_communities(conn, records, previous, prev_updated)
        for key in hidden_keys:
            conn.execute(
                "UPDATE communities SET hidden=1 WHERE record_key=?", (key,)
            )
        conn.commit()


def get_enrichment_candidates(
    db_path: Path, city_names: set[str], limit: int, retry_after_days: int = 7,
) -> list[dict]:
    """Communities in `city_names` still missing a `long_description` (the durable
    enrichment marker — set fields survive re-extraction via `_merge_source_urls`).
    Candidates attempted-but-not-enriched within the last `retry_after_days` are
    skipped (see `mark_enrichment_attempted`) so blocked/dead sources or repeated
    invalid output don't starve later communities while still allowing eventual
    retry. Returns dicts with record_key/name/city/topic/locale/description + all
    source_urls and any cached raw_text. raw_text is looked up per candidate
    (bounded to ~limit lookups), never the whole cache at once (OOM)."""
    import hashlib
    from datetime import timedelta

    def _uh(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    if not db_path.exists() or not city_names or limit <= 0:
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retry_after_days)).isoformat()
    out: list[dict] = []
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT record_key, data FROM communities WHERE hidden=0 ORDER BY id"
        ).fetchall()
        for record_key, data_str in rows:
            if len(out) >= limit:
                break
            try:
                d = json.loads(data_str)
            except (TypeError, json.JSONDecodeError):
                continue
            if d.get("city") not in city_names or (d.get("long_description") or "").strip():
                continue  # already enriched (durable marker) or out of scope
            if (d.get("enrich_attempted_at") or "") > cutoff:
                continue  # attempted recently and failed — retry later, not now
            urls = d.get("source_urls") or ([d["source_url"]] if d.get("source_url") else [])
            if not urls:
                continue
            raw_text = None
            for url in urls:
                row = conn.execute(
                    "SELECT json_extract(data, '$.raw_text') FROM cache_pages WHERE url_hash=?",
                    (_uh(url),),
                ).fetchone()
                if row and row[0] and len(row[0]) >= 300:
                    raw_text = row[0]
                    break
            out.append({
                "record_key": record_key, "name": d.get("name", ""),
                "city": d.get("city", ""), "topic": d.get("topic", ""),
                "locale": d.get("locale", "hu"),
                "description": d.get("description") or "",
                "source_urls": urls, "raw_text": raw_text,
            })
    return out


def update_community_enrichment(
    db_path: Path, record_key: str, short_description: str, long_description: str,
) -> bool:
    """Set a community's enrichment fields (`short_description` + `long_description`)
    and bump `updated_at` (genuine content change → correct <lastmod>). Returns
    False if the row is gone. No cache write needed: `_merge_source_urls` carries
    these fields forward across every re-extraction, so they are durable."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (record_key,)
        ).fetchone()
        if not row:
            return False
        old = json.loads(row[0])
        now = datetime.now(timezone.utc).isoformat()
        new = {**old, "short_description": short_description,
               "long_description": long_description}
        conn.execute(
            "UPDATE communities SET data=?, updated_at=? WHERE record_key=?",
            (json.dumps(new, ensure_ascii=False), now, record_key),
        )
        _log_community_changes(conn, new.get("community_id", ""), old, new)
        conn.commit()
    return True


def mark_enrichment_attempted(db_path: Path, record_key: str) -> None:
    """Stamp `enrich_attempted_at` so a candidate that failed/produced junk is not
    re-selected every batch (see `get_enrichment_candidates`). Does NOT bump
    `updated_at` (no user-visible change) or log history. Not a CommunityRecord
    field, so a genuine re-extraction clears it and allows a fresh retry."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (record_key,)
        ).fetchone()
        if not row:
            return
        d = json.loads(row[0])
        d["enrich_attempted_at"] = datetime.now(timezone.utc).isoformat()
        conn.execute("UPDATE communities SET data=? WHERE record_key=?",
                     (json.dumps(d, ensure_ascii=False), record_key))
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


def get_community_lastmods(db_path: Path) -> dict[tuple[str, str], str]:
    """(city, name_slug) → updated_at date (YYYY-MM-DD) for visible communities.

    Keyed by public slug and ordered by (topic, id) with first-wins, so the date
    is the one for the exact record the public URL resolves to (see
    `_find_community_by_slug` / `get_communities_for_city`) — even when a name
    exists under multiple topics or two names share a slug. `updated_at` only
    advances on real content changes (see `_bulk_upsert_communities`), so it is a
    stable <lastmod>. One query for the whole sitemap.
    """
    if not db_path.exists():
        return {}
    from .identity import public_slug
    out: dict[tuple[str, str], str] = {}
    with _connect(db_path) as conn:
        for city, name, updated_at in conn.execute(
            "SELECT city, json_extract(data,'$.name'), updated_at "
            "FROM communities WHERE hidden=0 ORDER BY topic, id"
        ):
            if name and updated_at:
                out.setdefault((city, public_slug(name)), updated_at[:10])
    return out


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


def get_collected_pairs(db_path: Path, max_pages: int) -> set[tuple[str, str]]:
    """Pairs whose selected search results have all had a fetch attempt."""
    del max_pages  # retained in the public signature for compatibility
    if not db_path.exists():
        return set()
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT city, topic FROM search_cache WHERE collected_at IS NOT NULL"
        ).fetchall()
    return {(row[0], row[1]) for row in rows}


def get_fully_processed_pairs(
    db_path: Path,
    current_fp: str,
    venue_fp: str | None = None,
    person_fp: str | None = None,
    *,
    run_communities: bool = True,
    run_venues: bool = False,
    run_persons: bool = False,
    max_pages: int | None = None,
) -> set[tuple[str, str]]:
    """Return (city, topic) pairs that need no pipeline work this run.

    cache_pages is keyed by url_hash and city/topic columns are overwritten on
    every save (last-write-wins), so city/topic-based joins are unreliable.
    Instead we check by url_hash, exactly as cache.py does at fetch time.

    A pair is done if it has a search_cache entry and every successfully
    scraped page is current for every extraction family enabled for this run.
    Venue/person extraction is only expected when the cached community result
    is non-empty, matching the pipeline's cost gates.
    """
    import hashlib

    def _url_hash(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    def _json_object_keys(raw: str | None) -> set[str]:
        if not raw:
            return set()
        try:
            value = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return set()
        return set(value) if isinstance(value, dict) else set()

    if not db_path.exists():
        return set()
    with _connect(db_path) as conn:
        pages_by_hash: dict[str, dict] = {
            row[0]: {
                "extract_fingerprint": row[1],
                "venue_fingerprint": row[2],
                "person_fingerprint": row[3],
                "records_present": bool(row[4]),
                "has_communities": bool(row[5]),
                "person_keys": _json_object_keys(row[6]),
            }
            for row in conn.execute(
                "SELECT url_hash, extract_fingerprint, venue_fingerprint,"
                " person_fingerprint,"
                " json_type(data, '$.records') = 'array',"
                " COALESCE(json_array_length(json_extract(data, '$.records')), 0) > 0,"
                " json_extract(data, '$.persons_data')"
                " FROM cache_pages"
                " WHERE scraped_at IS NOT NULL"
            )
        }
        search_rows = conn.execute("SELECT city, topic, urls FROM search_cache").fetchall()

    result: set[tuple[str, str]] = set()
    for city, topic, urls_json in search_rows:
        try:
            urls: list[str] = json.loads(urls_json) if urls_json else []
        except (TypeError, json.JSONDecodeError):
            log.warning("invalid_search_cache_urls", city=city, topic=topic)
            continue
        if max_pages is not None:
            urls = urls[:max_pages]
        if not urls:
            result.add((city, topic))
            continue

        processable = [
            pages_by_hash[_url_hash(url)]
            for url in urls
            if _url_hash(url) in pages_by_hash
        ]
        if not processable:
            continue

        pair_person_key = f"{city}/{topic}"
        all_current = True
        for page in processable:
            community_current = (
                page["extract_fingerprint"] == current_fp
                and page["records_present"]
            )
            if (run_communities or run_persons) and not community_current:
                all_current = False
                break

            has_communities = community_current and page["has_communities"]
            venue_expected = run_venues and (has_communities or not run_communities)
            if venue_expected and (
                not venue_fp or page["venue_fingerprint"] != venue_fp
            ):
                all_current = False
                break

            person_expected = run_persons and has_communities
            if person_expected and (
                not person_fp
                or page["person_fingerprint"] != person_fp
                or pair_person_key not in page["person_keys"]
            ):
                all_current = False
                break

        if all_current:
            result.add((city, topic))
    return result


def get_recently_added_communities(db_path: Path, limit: int = 30) -> list[dict]:
    """Latest first-seen visible communities (first __created__ history row per id)."""
    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT c.data, h.first_seen FROM communities c
            JOIN (SELECT community_id, MIN(changed_at) AS first_seen
                  FROM community_history WHERE field='__created__'
                  GROUP BY community_id) h ON h.community_id = c.community_id
            WHERE c.hidden = 0
              AND c.id = (SELECT MIN(c2.id) FROM communities c2
                          WHERE c2.community_id = c.community_id AND c2.hidden = 0)
            ORDER BY h.first_seen DESC LIMIT ?
        """, (limit,)).fetchall()
    out = []
    for data_str, first_seen in rows:
        try:
            d = json.loads(data_str)
        except Exception:
            continue
        d["first_seen"] = first_seen
        out.append(d)
    return out


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

def _write_cache_page(conn: sqlite3.Connection, entry: dict) -> None:
    # extract_quality/extract_model mirror the blob into real columns so the
    # router's upgrade sweep can filter and order in SQL rather than by decoding
    # every cached page. Neither is part of any cache key.
    quality = entry.get("extract_quality")
    conn.execute("""
        INSERT INTO cache_pages
            (url_hash, url, city, topic, domain, scraped_at, extracted_at,
             extract_fingerprint, venue_fingerprint, person_fingerprint,
             extract_quality, extract_model, data)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url_hash) DO UPDATE SET
            city=excluded.city,
            topic=excluded.topic,
            domain=excluded.domain,
            scraped_at=excluded.scraped_at,
            extracted_at=excluded.extracted_at,
            extract_fingerprint=excluded.extract_fingerprint,
            venue_fingerprint=excluded.venue_fingerprint,
            person_fingerprint=excluded.person_fingerprint,
            extract_quality=excluded.extract_quality,
            extract_model=excluded.extract_model,
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
        int(quality) if isinstance(quality, (int, float)) else None,
        entry.get("extract_model"),
        json.dumps(entry, ensure_ascii=False),
    ))


def update_cache_page(db_path: Path, url_hash: str, updates: dict | None = None,
                      *, create: dict | None = None, drop: list[str] | None = None,
                      mutate=None) -> dict | None:
    """Atomic read-merge-write of ONE cache page inside a single transaction.

    The old load→mutate→save pattern used two separate connections, so two
    concurrent writers to the same URL (pipeline vs. admin queue ops) could
    have the later full-blob write erase the other's fields. BEGIN IMMEDIATE
    holds the write lock across the read.

    create: entry skeleton used when the row does not exist (None = no-op on
    missing rows). drop: keys removed from the entry. mutate: callable applied
    to the entry inside the transaction for nested structures."""
    with _connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT data FROM cache_pages WHERE url_hash=?", (url_hash,)
        ).fetchone()
        if row:
            entry = json.loads(row[0])
        elif create is not None:
            entry = {**create, "url_hash": url_hash}
        else:
            conn.commit()
            return None
        if updates:
            entry.update(updates)
        for key in drop or ():
            entry.pop(key, None)
        if mutate is not None:
            entry = mutate(entry) or entry
        _write_cache_page(conn, entry)
        conn.commit()
    return entry


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


def invalidate_extraction_cache(
    db_path: Path,
    city: str | None = None,
    topic: str | None = None,
    urls: list[str] | None = None,
) -> int:
    """Remove community extraction results while preserving scraped page text.

    With no city/topic the invalidation is global. For a specific pair, URL
    hashes come from search_cache as well as cache_pages' denormalized pair
    columns; explicit URLs cover manually submitted or otherwise uncatalogued
    source pages.
    """
    import hashlib

    if not db_path.exists():
        return 0
    if (city is None) != (topic is None):
        raise ValueError("city and topic must be provided together")

    json_paths = (
        "'$.records', '$.extracted_at', '$.extract_duration_s',"
        " '$.extract_fingerprint', '$.extract_model',"
        " '$.enrich_scraped_at', '$.enrich_scrape_duration_s',"
        " '$.enrich_extracted_at', '$.enrich_extract_duration_s',"
        " '$.enrich_count', '$.enrich_model', '$.enrich_log'"
    )
    stale_predicate = (
        "(extracted_at IS NOT NULL OR extract_fingerprint IS NOT NULL"
        " OR json_extract(data, '$.records') IS NOT NULL)"
    )

    with _connect(db_path) as conn:
        if city is None:
            cur = conn.execute(
                f"UPDATE cache_pages SET extracted_at=NULL, extract_fingerprint=NULL, "
                f"data=json_remove(data, {json_paths}) WHERE {stale_predicate}"
            )
            conn.commit()
            return cur.rowcount

        target_hashes = {
            row[0]
            for row in conn.execute(
                "SELECT url_hash FROM cache_pages WHERE city=? AND topic=?",
                (city, topic),
            )
        }
        search_row = conn.execute(
            "SELECT urls FROM search_cache WHERE city=? AND topic=?",
            (city, topic),
        ).fetchone()
        if search_row and search_row[0]:
            target_hashes.update(
                hashlib.sha256(url.encode()).hexdigest()[:16]
                for url in json.loads(search_row[0])
            )
        target_hashes.update(
            hashlib.sha256(url.encode()).hexdigest()[:16]
            for url in (urls or [])
            if url
        )

        updated = 0
        hashes = sorted(target_hashes)
        for start in range(0, len(hashes), 500):
            chunk = hashes[start:start + 500]
            placeholders = ",".join("?" for _ in chunk)
            cur = conn.execute(
                f"UPDATE cache_pages SET extracted_at=NULL, extract_fingerprint=NULL, "
                f"data=json_remove(data, {json_paths}) "
                f"WHERE url_hash IN ({placeholders}) AND {stale_predicate}",
                chunk,
            )
            updated += cur.rowcount
        conn.commit()
        return updated


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
        conn.execute("UPDATE cache_pages SET person_fingerprint=NULL")
        conn.commit()
        return cur.rowcount


def get_cache_index(db_path: Path) -> list[dict]:
    """Cache listing without loading page texts: json_extract pulls only the
    small metadata keys (the old SELECT data + json.loads deserialized every
    full page text just to render a table)."""
    if not db_path.exists():
        return []
    small_keys = [
        "url_hash", "url", "domain", "city", "topic",
        "scraped_at", "scrape_duration_s", "extracted_at", "extract_duration_s",
        "enrich_scraped_at", "enrich_scrape_duration_s",
        "enrich_extracted_at", "enrich_extract_duration_s", "enrich_count",
        "extract_fingerprint", "extract_model", "enrich_model",
    ]
    select = ", ".join(f"json_extract(data, '$.{k}')" for k in small_keys)
    with _connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT {select},"
            " json_array_length(COALESCE(json_extract(data, '$.records'), '[]')),"
            " json_extract(data, '$.raw_text') IS NOT NULL"
            " FROM cache_pages ORDER BY url_hash"
        ).fetchall()
    entries = []
    for row in rows:
        entry = dict(zip(small_keys, row[:len(small_keys)]))
        entry["record_count"] = row[len(small_keys)] or 0
        entry["has_text"] = bool(row[len(small_keys) + 1])
        for k in ("url_hash", "url", "domain", "city", "topic"):
            entry[k] = entry[k] or ""
        entries.append(entry)
    return entries


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


def get_scraped_cache_by_search_pair(db_path: Path) -> list[tuple[str, str, str, str]]:
    """Return scraped pages attributed by authoritative search-cache URL lists.

    A URL may belong to several pairs, so it may occur more than once. Cached
    pages absent from search_cache (for example manual submissions) fall back
    to their denormalized city/topic metadata.
    """
    import hashlib

    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        page_rows = conn.execute(
            "SELECT url_hash, data FROM cache_pages WHERE scraped_at IS NOT NULL"
        ).fetchall()
        search_rows = conn.execute("SELECT city, topic, urls FROM search_cache").fetchall()

    pages: dict[str, tuple[str, str, str, str]] = {}
    for url_hash, data_json in page_rows:
        try:
            entry = json.loads(data_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(entry, dict) and entry.get("raw_text") and entry.get("url"):
            pages[url_hash] = (
                entry["url"],
                entry["raw_text"],
                entry.get("city", ""),
                entry.get("topic", ""),
            )

    result: list[tuple[str, str, str, str]] = []
    linked_hashes: set[str] = set()
    linked_pairs: set[tuple[str, str, str]] = set()
    for city, topic, urls_json in search_rows:
        try:
            urls = json.loads(urls_json) if urls_json else []
        except (TypeError, json.JSONDecodeError):
            continue
        for url in urls:
            url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
            page = pages.get(url_hash)
            pair_key = (url_hash, city, topic)
            if not page or pair_key in linked_pairs:
                continue
            result.append((page[0], page[1], city, topic))
            linked_hashes.add(url_hash)
            linked_pairs.add(pair_key)

    result.extend(page for url_hash, page in pages.items() if url_hash not in linked_hashes)
    return result


def get_scraped_cache_for_search_pair(
    db_path: Path, city: str, topic: str
) -> list[tuple[str, str]]:
    """Return only one pair's ``(url, raw_text)`` rows.

    The old bulk helper materializes every raw page in memory before ai_only can
    process its first pair. At production scale that can trigger an OOM restart.
    Search-cache URLs remain authoritative; denormalized cache metadata is used
    only for pairs without a search row (for example manual legacy pages).
    """
    import hashlib

    if not db_path.exists():
        return []
    with _connect(db_path) as conn:
        search_row = conn.execute(
            "SELECT urls FROM search_cache WHERE city=? AND topic=?",
            (city, topic),
        ).fetchone()
        if search_row is None:
            rows = conn.execute(
                "SELECT data FROM cache_pages"
                " WHERE city=? AND topic=? AND scraped_at IS NOT NULL",
                (city, topic),
            ).fetchall()
            ordered_hashes = None
        else:
            try:
                urls = json.loads(search_row[0]) if search_row[0] else []
            except (TypeError, json.JSONDecodeError):
                log.warning("invalid_search_cache_urls", city=city, topic=topic)
                return []
            ordered_hashes = [hashlib.sha256(url.encode()).hexdigest()[:16] for url in urls]
            if not ordered_hashes:
                return []
            placeholders = ",".join("?" for _ in ordered_hashes)
            rows = conn.execute(
                f"SELECT url_hash, data FROM cache_pages"
                f" WHERE url_hash IN ({placeholders}) AND scraped_at IS NOT NULL",
                ordered_hashes,
            ).fetchall()

    if ordered_hashes is None:
        entries = []
        for (data_json,) in rows:
            try:
                entry = json.loads(data_json)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(entry, dict) and entry.get("url") and entry.get("raw_text"):
                entries.append((entry["url"], entry["raw_text"]))
        return entries

    by_hash: dict[str, tuple[str, str]] = {}
    for url_hash, data_json in rows:
        try:
            entry = json.loads(data_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(entry, dict) and entry.get("url") and entry.get("raw_text"):
            by_hash[url_hash] = (entry["url"], entry["raw_text"])
    return [by_hash[url_hash] for url_hash in ordered_hashes if url_hash in by_hash]


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
                urls=excluded.urls, queries=excluded.queries,
                cached_at=excluded.cached_at, collected_at=NULL
        """, (city, topic, json.dumps(urls), json.dumps(queries), now))
        conn.commit()


def mark_search_collection_complete(db_path: Path, city: str, topic: str) -> None:
    """Mark a searched pair complete after every selected URL was attempted.

    Individual fetch failures remain visible in the run log, but do not keep the
    whole pair runnable forever. If the process dies before this call, the NULL
    marker makes the next collector resume the pair.
    """
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE search_cache SET collected_at=? WHERE city=? AND topic=?",
            (now, city, topic),
        )
        conn.commit()


def get_search_cache(db_path: Path, city: str, topic: str,
                     ttl_days: int) -> list[str] | None:
    if not db_path.exists():
        return None
    import json
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT urls FROM search_cache WHERE city=? AND topic=? AND cached_at>=?",
            (city, topic, cutoff)
        ).fetchone()
    return json.loads(row[0]) if row else None




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

def delete_leader_persons_for_community(db_path: Path, community_name: str, city: str,
                                         only_synthesized: bool = False) -> int:
    """Delete role='leader' persons for a community before re-inserting clean parsed ones.

    only_synthesized=True restricts the delete to rows synthesized from the
    community's leader field (marked origin='leader_field' in data) — used for
    stale-leader cleanup so independently AI-extracted leader persons survive.

    Known limitation: rows synthesized before 2026-07-24 carry no origin marker
    and are indistinguishable from AI-extracted ones, so only_synthesized skips
    them (a safe migration is impossible — marking every role='leader' row
    would re-introduce the data-loss this marker prevents). They are still
    replaced whenever their community yields leaders again."""
    if not db_path.exists():
        return 0
    sql = ("DELETE FROM persons WHERE city=? AND role='leader' "
           "AND json_extract(data,'$.community_name')=?")
    if only_synthesized:
        sql += " AND json_extract(data,'$.origin')='leader_field'"
    with _connect(db_path) as conn:
        cur = conn.execute(sql, (city, community_name))
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
    """Persons of one community: fetch the city's persons and match community_name
    by normalized form (the LLM's name variants differ in case/punctuation; the
    old record_key LIKE anchored on the wrong key segments and never matched)."""
    if not db_path.exists():
        return []
    target = normalized_match_key(community_name)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT data FROM persons WHERE city=? ORDER BY role, id", (city,)
        ).fetchall()
    persons = [json.loads(r[0]) for r in rows]
    return [
        p for p in persons
        if normalized_match_key(p.get("community_name", "")) == target
    ]


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

    escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    pattern = f"%{escaped}%"
    with _connect(db_path) as conn:
        community_rows = conn.execute(
            "SELECT data FROM communities WHERE hidden=0 AND ("
            "  json_extract(data,'$.name') LIKE ? ESCAPE '\\' OR"
            "  json_extract(data,'$.description') LIKE ? ESCAPE '\\'"
            ") ORDER BY city, id LIMIT ?",
            (pattern, pattern, limit),
        ).fetchall()
        venue_rows = conn.execute(
            "SELECT data FROM venues WHERE ("
            "  json_extract(data,'$.name') LIKE ? ESCAPE '\\' OR"
            "  json_extract(data,'$.description') LIKE ? ESCAPE '\\'"
            ") ORDER BY city, id LIMIT ?",
            (pattern, pattern, limit),
        ).fetchall()
        person_rows = conn.execute(
            "SELECT data FROM persons WHERE"
            "  json_extract(data,'$.name') LIKE ? ESCAPE '\\' ORDER BY city, id LIMIT ?",
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


def count_pending_interactions(db_path: Path) -> dict:
    """Pending user-submitted items for the admin Inbox badge.

    Not-community reports have no status column — every stored row is pending
    (handling deletes the row)."""
    counts = {"edit_requests": 0, "reports": 0, "submissions": 0, "total": 0}
    if not db_path or not db_path.exists():
        return counts
    with _connect(db_path) as conn:
        for key, sql in (
            ("edit_requests", "SELECT COUNT(*) FROM edit_requests WHERE status='pending'"),
            ("reports", "SELECT COUNT(*) FROM not_community_reports"),
            ("submissions", "SELECT COUNT(*) FROM community_submissions WHERE status='pending'"),
        ):
            try:
                counts[key] = conn.execute(sql).fetchone()[0]
            except sqlite3.OperationalError:
                pass  # table not created yet on an old DB
    counts["total"] = counts["edit_requests"] + counts["reports"] + counts["submissions"]
    return counts


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
        # Pair idempotency checks BOTH orders: winner/loser now carry meaning
        # (winner = the record a merge keeps), so re-scans may compute the
        # reverse order for an already-known pair and must not duplicate it.
        existing = conn.execute(
            "SELECT id, winner_key, resolution, signal FROM duplicate_candidates"
            " WHERE entity_type=? AND ((winner_key=? AND loser_key=?)"
            "                       OR (winner_key=? AND loser_key=?))",
            (entity_type, winner_key, loser_key, loser_key, winner_key),
        ).fetchone()
        if existing:
            # Pending rows get their orientation corrected in place so a later
            # merge keeps the right record. Precedence: an admin's manual flag
            # always wins (it also stamps signal='manual' so later auto scans
            # cannot flip it back); auto re-scans may only reorient other auto
            # rows.
            may_update = signal == "manual" or existing[3] != "manual"
            needs_change = (existing[1] != winner_key
                            or (signal == "manual" and existing[3] != "manual"))
            if existing[2] is None and may_update and needs_change:
                # Reorienting can collide with *another* pending row holding the
                # reverse orientation of the same pair: idx_dup_pair is partial
                # (resolution IS NULL), and rows predating the both-orders
                # lookup above could be stored both ways round. They describe
                # one pair, so the redundant one goes rather than the write
                # failing — 2026-08-17 the post-run scan died on
                # "UNIQUE constraint failed: duplicate_candidates.entity_type,
                # winner_key, loser_key" and took the whole pass with it.
                conn.execute(
                    "DELETE FROM duplicate_candidates"
                    " WHERE entity_type=? AND winner_key=? AND loser_key=?"
                    "   AND resolution IS NULL AND id<>?",
                    (entity_type, winner_key, loser_key, existing[0]),
                )
                conn.execute(
                    "UPDATE duplicate_candidates"
                    " SET winner_id=?, loser_id=?, winner_key=?, loser_key=?, signal=?"
                    " WHERE id=?",
                    (winner_id, loser_id, winner_key, loser_key,
                     signal if signal == "manual" else existing[3], existing[0]),
                )
                conn.commit()
            return False
        # ON CONFLICT, not a bare INSERT: the SELECT above and this write are
        # separate statements, so a row can appear between them — the
        # 2026-08-17 post-run scan died on
        # "UNIQUE constraint failed: duplicate_candidates.entity_type,
        # winner_key, loser_key" and took the whole duplicate pass with it.
        # "Record this pair unless it is already known" is exactly what the
        # conflict clause says, and it says it atomically. The WHERE mirrors
        # idx_dup_pair's own predicate — without it SQLite cannot tell which
        # index the clause targets and rejects the statement.
        cursor = conn.execute("""
            INSERT INTO duplicate_candidates
              (entity_type, winner_id, loser_id, winner_key, loser_key, similarity, signal, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (entity_type, winner_key, loser_key)
              WHERE resolution IS NULL DO NOTHING
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


# ── Wrong-city candidates ──────────────────────────────────────────────────────

def insert_wrong_city_candidate(
    db_path: Path,
    record_key: str,
    community_id: str,
    mentioned_city: str,
    field: str,
    snippet: str,
    matched_text: str,
) -> bool:
    """Insert a wrong-city candidate. Returns False if the (record, city) pair
    was already flagged — dismissed pairs stay dismissed and are not re-raised."""
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        try:
            conn.execute(
                "INSERT INTO wrong_city_candidates"
                " (record_key, community_id, mentioned_city, field, snippet,"
                "  matched_text, detected_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record_key, community_id, mentioned_city, field, snippet,
                 matched_text, now),
            )
        except sqlite3.IntegrityError:
            return False
        conn.commit()
    return True


def get_wrong_city_candidates(db_path: Path, resolved: bool | None = False) -> list[dict]:
    if not db_path.exists():
        return []
    where = ""
    if resolved is True:
        where = "WHERE resolution IS NOT NULL"
    elif resolved is False:
        where = "WHERE resolution IS NULL"
    with _connect(db_path) as conn:
        cursor = conn.execute(
            f"SELECT * FROM wrong_city_candidates {where} ORDER BY detected_at DESC, id DESC"
        )
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def resolve_wrong_city_candidate(db_path: Path, candidate_id: int, resolution: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE wrong_city_candidates SET resolution=?, resolved_at=? WHERE id=?",
            (resolution, now, candidate_id),
        )
        conn.commit()


def get_entity_by_record_key(db_path: Path, entity_type: str,
                             record_key: str) -> dict | None:
    table = {"venue": "venues", "person": "persons"}.get(entity_type)
    if table is None or not db_path.exists():
        return None
    with _connect(db_path) as conn:
        row = conn.execute(
            f"SELECT data FROM {table} WHERE record_key=?", (record_key,)).fetchone()
    return json.loads(row[0]) if row else None


def merge_entity_into(db_path: Path, entity_type: str,
                      winner_key: str, loser_key: str) -> bool:
    """Merge a venue/person duplicate: fill the winner's empty fields from the
    loser, union source_urls, delete the loser row. Returns False when either
    record is missing (candidate is stale)."""
    table = {"venue": "venues", "person": "persons"}.get(entity_type)
    if table is None:
        return False
    with _connect(db_path) as conn:
        winner_row = conn.execute(
            f"SELECT data FROM {table} WHERE record_key=?", (winner_key,)).fetchone()
        loser_row = conn.execute(
            f"SELECT data FROM {table} WHERE record_key=?", (loser_key,)).fetchone()
        if not winner_row or not loser_row:
            return False
        winner_data = json.loads(winner_row[0])
        loser_data = json.loads(loser_row[0])
        for field, value in loser_data.items():
            if not value:
                continue
            current = winner_data.get(field)
            if isinstance(value, list) or isinstance(current, list):
                # Relationship/list fields (community_ids, welcomed_topics,
                # social_links, …) are unioned — keeping only the winner's list
                # would silently drop the loser's associations.
                merged = list(current or []) + [v for v in value
                                                if v not in (current or [])]
                winner_data[field] = merged
            elif not current:
                winner_data[field] = value
        w_urls = list(winner_data.get("source_urls") or [])
        if winner_data.get("source_url") and winner_data["source_url"] not in w_urls:
            w_urls = [winner_data["source_url"]] + w_urls
        l_urls = list(loser_data.get("source_urls") or [])
        if loser_data.get("source_url") and loser_data["source_url"] not in l_urls:
            l_urls = [loser_data["source_url"]] + l_urls
        winner_data["source_urls"] = list(dict.fromkeys(w_urls + l_urls))
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            f"UPDATE {table} SET data=?, updated_at=? WHERE record_key=?",
            (json.dumps(winner_data, ensure_ascii=False), now, winner_key),
        )
        conn.execute(f"DELETE FROM {table} WHERE record_key=?", (loser_key,))
        conn.commit()
    return True


def apply_venue_edit(db_path: Path, record_key: str, change_type: str,
                     new_value: str | None) -> bool:
    """Apply an approved venue edit. 'closed' deletes the venue (venues have no
    hidden flag); 'name_correction' renames and recomputes the record key.
    'wrong_info' carries free-text notes only and cannot be auto-applied."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM venues WHERE record_key=?", (record_key,)).fetchone()
        if not row:
            return False
        if change_type == "closed":
            conn.execute("DELETE FROM venues WHERE record_key=?", (record_key,))
            conn.commit()
            return True
        if change_type == "name_correction" and new_value:
            data = json.loads(row[0])
            data["name"] = new_value
            new_key = _venue_record_key(new_value, data.get("city", ""))
            try:
                conn.execute(
                    "UPDATE venues SET record_key=?, data=?, updated_at=? WHERE record_key=?",
                    (new_key, json.dumps(data, ensure_ascii=False),
                     datetime.now(timezone.utc).isoformat(), record_key),
                )
            except sqlite3.IntegrityError:
                return False  # target key exists — surface as failed edit
            conn.commit()
            return True
    return False


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
) -> str:
    """Apply an approved edit to a community record.

    Returns a status string: "ok" (applied), "merged" (target identity already
    existed — this row was merged into it), "not_found", or "unsupported".
    """
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT data FROM communities WHERE record_key=?", (record_key,)
        ).fetchone()
        if not row:
            return "not_found"
        data = json.loads(row[0])
        now = datetime.now(timezone.utc).isoformat()
        if change_type in ("archive", "delete"):
            conn.execute(
                "UPDATE communities SET hidden=1, updated_at=? WHERE record_key=?",
                (now, record_key),
            )
        elif change_type in ("wrong_city", "wrong_topic", "name_correction"):
            if change_type == "wrong_city":
                data["city"] = new_value
            elif change_type == "wrong_topic":
                data["topic"] = new_value
            else:
                data["name"] = new_value
            # record_key derives from (name, city, topic) — leaving it stale made
            # the next scrape insert a duplicate row and broke follow-up edits.
            new_key = _community_record_key(data["name"], data["city"], data["topic"])
            try:
                conn.execute(
                    "UPDATE communities SET record_key=?, city=?, topic=?, data=?, updated_at=?"
                    " WHERE record_key=?",
                    (new_key, data["city"], data["topic"],
                     json.dumps(data, ensure_ascii=False), now, record_key),
                )
            except sqlite3.IntegrityError:
                # The corrected identity already exists (typically the scraper
                # also found this community under its real city). Merge this
                # row into the existing target: union source_urls, make sure
                # the target is visible, hide this row. Done on the same
                # connection — a second connection would deadlock here.
                target = conn.execute(
                    "SELECT data FROM communities WHERE record_key=?", (new_key,)
                ).fetchone()
                if not target:
                    return "not_found"
                target_data = json.loads(target[0])
                urls: list[str] = []
                for d in (target_data, data):
                    if d.get("source_url") and d["source_url"] not in urls:
                        urls.append(d["source_url"])
                    for u in d.get("source_urls") or []:
                        if u not in urls:
                            urls.append(u)
                target_data["source_urls"] = urls
                conn.execute(
                    "UPDATE communities SET data=?, hidden=0, updated_at=? WHERE record_key=?",
                    (json.dumps(target_data, ensure_ascii=False), now, new_key),
                )
                conn.execute(
                    "UPDATE communities SET hidden=1, updated_at=? WHERE record_key=?",
                    (now, record_key),
                )
                conn.commit()
                return "merged"
        else:
            return "unsupported"
        conn.commit()
    return "ok"


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
        now - timedelta(hours=24)
        since_sql = "datetime('now', '-24 hours')"
        buckets = [(now - timedelta(hours=i)).strftime(fmt) for i in range(23, -1, -1)]
        display = {b: b[-2:] + ":00" for b in buckets}
    elif period == "7d":
        fmt = "%Y-%m-%d"
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
        # MIN(changed_at) per community_id — delete+reinsert cycles (topic
        # replace, dedup churn) re-log __created__ and would double-count.
        _run(conn, f"""
            SELECT {strftime_col.format(col='first_seen')}, COUNT(*)
            FROM (
                SELECT community_id, MIN(changed_at) AS first_seen
                FROM community_history
                WHERE field='__created__'
                GROUP BY community_id
            )
            WHERE first_seen >= {since_sql}
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


# ── Daily traffic + report ────────────────────────────────────────────────────

def record_pageview(db_path: Path, day: str, site: str, visitor_hash: str) -> None:
    """One public page hit. Lightweight server-side counter (bot-filtered by the
    caller); uniques approximated by a per-day visitor hash."""
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO traffic_daily(day, site, pageviews) VALUES(?,?,1)"
            " ON CONFLICT(day, site) DO UPDATE SET pageviews = pageviews + 1",
            (day, site))
        conn.execute(
            "INSERT OR IGNORE INTO traffic_visitors(day, site, visitor_hash) VALUES(?,?,?)",
            (day, site, visitor_hash))
        conn.commit()


def get_traffic_for_day(db_path: Path, day: str) -> dict:
    """{site: {"pageviews": n, "visitors": m}} for one UTC day."""
    if not db_path.exists():
        return {}
    out: dict = {}
    with _connect(db_path) as conn:
        for site, pv in conn.execute(
                "SELECT site, pageviews FROM traffic_daily WHERE day=?", (day,)).fetchall():
            out[site] = {"pageviews": pv, "visitors": 0}
        for site, uniq in conn.execute(
                "SELECT site, COUNT(*) FROM traffic_visitors WHERE day=? GROUP BY site",
                (day,)).fetchall():
            out.setdefault(site, {"pageviews": 0, "visitors": 0})["visitors"] = uniq
    return out


def get_daily_summary(db_path: Path, start_iso: str, end_iso: str,
                      hu_cities: set) -> dict:
    """Changes between two ISO timestamps, split into 'hu' / 'intl' scopes."""
    empty = {"new_communities": 0, "changed_communities": 0, "change_rows": 0,
             "new_venues": 0, "new_persons": 0, "pages_scraped": 0,
             "pages_extracted": 0, "searches": 0}
    stock_empty = {"communities": 0, "venues": 0, "persons": 0,
                   "pages_cached": 0, "pages_extracted": 0, "covered_pairs": 0}
    result = {"hu": dict(empty), "intl": dict(empty), "runs": [],
              "totals": {"hu": 0, "intl": 0, "covered_pairs_hu": 0, "covered_pairs_intl": 0},
              "stock": {"hu": dict(stock_empty), "intl": dict(stock_empty)}}
    if not db_path.exists():
        return result

    def scope(city: str) -> str:
        return "hu" if city in hu_cities else "intl"

    with _connect(db_path) as conn:
        for city, cnt in conn.execute("""
            SELECT c.city, COUNT(DISTINCT c.community_id) FROM communities c
            JOIN (SELECT community_id, MIN(changed_at) AS fs FROM community_history
                  WHERE field='__created__' GROUP BY community_id) h
              ON h.community_id = c.community_id
            WHERE c.hidden = 0 AND h.fs >= ? AND h.fs < ?
            GROUP BY c.city
        """, (start_iso, end_iso)).fetchall():
            result[scope(city)]["new_communities"] += cnt

        for city, ids, rows in conn.execute("""
            SELECT c.city, COUNT(DISTINCT ch.community_id), COUNT(DISTINCT ch.id)
            FROM community_history ch
            JOIN communities c ON c.community_id = ch.community_id
            WHERE ch.field != '__created__' AND ch.changed_at >= ? AND ch.changed_at < ?
            GROUP BY c.city
        """, (start_iso, end_iso)).fetchall():
            result[scope(city)]["changed_communities"] += ids
            result[scope(city)]["change_rows"] += rows

        for table, id_col, target, key in (
            ("venue_history", "venue_id", "venues", "new_venues"),
            ("person_history", "person_id", "persons", "new_persons"),
        ):
            for city, cnt in conn.execute(f"""
                SELECT t.city, COUNT(DISTINCT t.{id_col}) FROM {target} t
                JOIN (SELECT {id_col} AS eid, MIN(changed_at) AS fs FROM {table}
                      WHERE field='__created__' GROUP BY {id_col}) h
                  ON h.eid = t.{id_col}
                WHERE h.fs >= ? AND h.fs < ?
                GROUP BY t.city
            """, (start_iso, end_iso)).fetchall():
                result[scope(city)][key] += cnt

        for col, key in (("scraped_at", "pages_scraped"), ("extracted_at", "pages_extracted")):
            for city, cnt in conn.execute(
                    f"SELECT city, COUNT(*) FROM cache_pages WHERE {col} >= ? AND {col} < ? GROUP BY city",
                    (start_iso, end_iso)).fetchall():
                result[scope(city or "")][key] += cnt

        for city, cnt in conn.execute(
                "SELECT city, COUNT(*) FROM search_cache WHERE cached_at >= ? AND cached_at < ? GROUP BY city",
                (start_iso, end_iso)).fetchall():
            result[scope(city)]["searches"] += cnt

        for row in conn.execute(
            "SELECT id, run_mode, started_at, finished_at, success, search_log, error, "
                f"{_OUTCOME_SQL}"
                " FROM runs WHERE started_at >= ? AND started_at < ? ORDER BY started_at",
                (start_iso, end_iso)).fetchall():
            interrupted_error = (
                "run unfinished (still running, container restart, or OOM)"
                if not row[3] and not row[6] else ""
            )
            run = {"id": row[0], "mode": row[1], "started_at": row[2],
                   "finished_at": row[3], "success": bool(row[4]),
                   # An unfinished row is an abort however its columns read.
                   "outcome": "aborted" if interrupted_error else row[7],
                   "pairs": 0, "records": 0, "search_failed": 0, "extract_failed": 0,
                   "search_error": "", "extract_error": "",
                   "error": row[6] or interrupted_error}
            if row[5]:
                try:
                    logs = json.loads(row[5])
                    run["pairs"] = len(logs)
                    run["records"] = sum(p.get("records_extracted", 0) for p in logs)
                    run["search_failed"] = sum(1 for p in logs if p.get("search_failed"))
                    run["extract_failed"] = sum(p.get("extract_failed", 0) for p in logs)
                    run["search_error"] = next(
                        (p["search_error"] for p in logs if p.get("search_error")), "")
                    run["extract_error"] = next(
                        (p["extract_error"] for p in logs if p.get("extract_error")), "")
                except Exception:
                    pass
            result["runs"].append(run)

        for city, cnt in conn.execute(
                "SELECT city, COUNT(*) FROM communities WHERE hidden=0 GROUP BY city").fetchall():
            result["totals"][scope(city)] += cnt
            result["stock"][scope(city)]["communities"] += cnt
        for city, cnt in conn.execute(
                "SELECT city, COUNT(*) FROM search_cache GROUP BY city").fetchall():
            result["totals"]["covered_pairs_" + scope(city)] += cnt
            result["stock"][scope(city)]["covered_pairs"] += cnt

        for table, key in (("venues", "venues"), ("persons", "persons")):
            for city, cnt in conn.execute(
                    f"SELECT city, COUNT(*) FROM {table} GROUP BY city").fetchall():
                result["stock"][scope(city or "")][key] += cnt
        for city, cnt in conn.execute(
                "SELECT city, COUNT(*) FROM cache_pages GROUP BY city").fetchall():
            result["stock"][scope(city or "")]["pages_cached"] += cnt
        for city, cnt in conn.execute(
                "SELECT city, COUNT(*) FROM cache_pages WHERE extracted_at IS NOT NULL"
                " GROUP BY city").fetchall():
            result["stock"][scope(city or "")]["pages_extracted"] += cnt
    return result


# ── Provider quota ledger (free-tier model router) ────────────────────────────

def get_provider_usage(db_path: Path, day: str) -> dict[str, dict]:
    """{provider: {calls, failures, rate_limits, observed_limit, blocked_until,
    last_error}} for one UTC day. Missing providers simply have no row."""
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM provider_usage WHERE day=?", (day,)).fetchall()
    return {r["provider"]: dict(r) for r in rows}


def record_provider_call(
    db_path: Path,
    day: str,
    provider: str,
    *,
    ok: bool = True,
    rate_limited: bool = False,
    blocked_until: float | None = None,
    error: str | None = None,
    observed_limit: int | None = None,
) -> None:
    """Count one provider call against its daily budget.

    Every attempt increments `calls`, including failures: a 429 or a 400 still
    consumed a request slot at most providers, and undercounting is what makes a
    router walk straight into a hard block.

    `observed_limit` is only ever lowered, never raised — once a provider has
    proven it stops at N requests, a later run must not optimistically restore a
    higher published number.
    """
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO provider_usage (day, provider) VALUES (?, ?)",
            (day, provider),
        )
        conn.execute(
            """
            UPDATE provider_usage
               SET calls          = calls + 1,
                   failures       = failures + ?,
                   rate_limits    = rate_limits + ?,
                   blocked_until  = MAX(blocked_until, COALESCE(?, 0)),
                   last_error     = COALESCE(?, last_error),
                   observed_limit = CASE
                       WHEN ? IS NULL THEN observed_limit
                       WHEN observed_limit IS NULL THEN ?
                       ELSE MIN(observed_limit, ?)
                   END
             WHERE day=? AND provider=?
            """,
            (0 if ok else 1, 1 if rate_limited else 0, blocked_until, error,
             observed_limit, observed_limit, observed_limit, day, provider),
        )
        conn.commit()


def get_upgradable_pages(
    db_path: Path, min_quality: int, limit: int, fingerprint: str,
    cities: list[str] | None = None,
) -> list[dict]:
    """Cached pages whose extraction came from a model scoring below
    `min_quality` — candidates for re-extraction with a better free model.

    **NULL `extract_quality` is excluded, not treated as zero.** Every page
    extracted before the router existed (~74K of them) carries NULL, and those
    came from the paid incumbent, which scores *above* every free model.
    Ranking them worst-first would have the sweep overwrite good DeepSeek output
    with weaker free-model output — a downgrade wearing an upgrade's name. A row
    only becomes a candidate once a router run has recorded what produced it.

    Ordered worst-first so a bounded sweep spends its budget where the gain is
    largest. Restricted to the current fingerprint: a page at a stale
    fingerprint is already scheduled for ordinary re-extraction.

    `cities` restricts the query to the caller's city set. It must be applied
    **in SQL**, before LIMIT: the caller runs one country group at a time, so
    filtering afterwards can return an empty result while thousands of eligible
    pages sit further down a globally-ordered list.
    """
    where = ["extracted_at IS NOT NULL", "extract_fingerprint = ?",
             "extract_quality IS NOT NULL", "extract_quality < ?"]
    params: list = [fingerprint, min_quality]
    if cities is not None:
        if not cities:
            return []
        where.append(f"city IN ({','.join('?' * len(cities))})")
        params.extend(cities)
    params.append(limit)
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT url, url_hash, city, topic, extract_quality AS q
              FROM cache_pages
             WHERE {' AND '.join(where)}
             ORDER BY q ASC, extracted_at ASC
             LIMIT ?
            """,
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def set_page_extract_quality(
    db_path: Path, url_hash: str, quality: int, model: str,
) -> None:
    """Stamp which model produced a page's cached extraction, and how good it
    is. Kept out of every cache key on purpose — the fingerprint must stay
    stable across providers or the done-pair check falls apart."""
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE cache_pages SET extract_quality=?, extract_model=? WHERE url_hash=?",
            (quality, model, url_hash),
        )
        conn.commit()


def get_extraction_quality_mix(db_path: Path, limit: int = 15) -> list[dict]:
    """Cached pages grouped by the model that extracted them, largest first.

    The honest answer to "how good is the corpus actually" — and the input the
    router's upgrade sweep works from. A NULL model/quality means the page was
    extracted before the router existed.
    """
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT extract_model, extract_quality, COUNT(*) AS pages
              FROM cache_pages
             WHERE extracted_at IS NOT NULL
             GROUP BY extract_model, extract_quality
             ORDER BY pages DESC
             LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [{"model": m, "quality": q, "pages": n} for m, q, n in rows]


def get_sitemap_communities(db_path: Path) -> dict[tuple[str, str], list[dict]]:
    """{(city, topic): [{name, thin}]} for every visible community, in one query.

    The sitemap used to call `get_communities(city, topic)` inside a loop over
    every city×topic pair. With 3.8K cities that is thousands of separate
    connections and JSON decodes on the event loop — measured at >30s on
    2026-08-16, which blocked every other request behind it.

    Only the two fields the sitemap needs are decoded: the name (for the slug)
    and whether the page is thin (no description of either kind), which decides
    whether it belongs in the sitemap at all.
    """
    out: dict[tuple[str, str], list[dict]] = {}
    if not db_path.exists():
        return out
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT city, topic,
                   json_extract(data, '$.name'),
                   COALESCE(NULLIF(TRIM(COALESCE(json_extract(data, '$.description'), '')), ''),
                            NULLIF(TRIM(COALESCE(json_extract(data, '$.long_description'), '')), ''))
              FROM communities
             WHERE hidden=0
             ORDER BY city, topic, id
            """
        ).fetchall()
    for city, topic, name, described in rows:
        if not name:
            continue
        out.setdefault((city or "", topic or ""), []).append(
            {"name": name, "thin": not described})
    return out
