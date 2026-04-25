import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


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
        conn.commit()


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
