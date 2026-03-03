import os
import re
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent.parent / "contactsync.db"
DATABASE_URL = os.getenv("DATABASE_URL", "")

# Type aliases for main.py to use instead of sqlite3.Cursor / sqlite3.Row
Cursor = Any
Row = Any

_pg_pool = None

_INSERT_OR_IGNORE_RE = re.compile(r"INSERT\s+OR\s+IGNORE\s+INTO", re.IGNORECASE)


def _is_postgres() -> bool:
    return DATABASE_URL.startswith("postgresql://")


def _init_pg_pool():
    global _pg_pool
    if _pg_pool is None:
        from psycopg2 import pool

        _pg_pool = pool.SimpleConnectionPool(2, 20, DATABASE_URL)


def _translate_sql(sql: str) -> str:
    """Convert SQLite SQL dialect to PostgreSQL.

    - ``?`` placeholders become ``%s``
    - ``INSERT OR IGNORE INTO`` becomes ``INSERT INTO … ON CONFLICT DO NOTHING``
    """
    needs_on_conflict = bool(_INSERT_OR_IGNORE_RE.search(sql))
    if needs_on_conflict:
        sql = _INSERT_OR_IGNORE_RE.sub("INSERT INTO", sql)

    sql = sql.replace("?", "%s")

    if needs_on_conflict:
        sql = sql.rstrip().rstrip(";")
        sql += " ON CONFLICT DO NOTHING"

    return sql


class _PgCursorWrapper:
    """Wraps a psycopg2 RealDictCursor, translating ``?`` → ``%s``."""

    def __init__(self, real_cursor):
        self._cur = real_cursor

    def execute(self, sql: str, params=None):
        translated = _translate_sql(sql)
        self._cur.execute(translated, params)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()


class _PgConnectionWrapper:
    """Wraps a psycopg2 connection to mimic the sqlite3.Connection interface.

    * ``cursor()`` returns a ``_PgCursorWrapper`` backed by ``RealDictCursor``
      so that ``row["col"]`` works identically to sqlite3.Row.
    * ``commit()`` re-raises ``psycopg2.IntegrityError`` as
      ``sqlite3.IntegrityError`` so callers in main.py need no changes.
    * ``close()`` returns the connection to the pool instead of destroying it.
    """

    def __init__(self, raw_conn):
        self._conn = raw_conn

    def cursor(self):
        from psycopg2.extras import RealDictCursor

        return _PgCursorWrapper(self._conn.cursor(cursor_factory=RealDictCursor))

    def commit(self):
        try:
            self._conn.commit()
        except Exception as e:
            import psycopg2

            if isinstance(e, psycopg2.IntegrityError):
                self._conn.rollback()
                raise sqlite3.IntegrityError(str(e)) from e
            raise

    def close(self):
        global _pg_pool
        if _pg_pool and self._conn and not self._conn.closed:
            try:
                self._conn.rollback()
            except Exception:
                pass
            _pg_pool.putconn(self._conn)


def get_conn():
    """Return a database connection (SQLite or PostgreSQL depending on env)."""
    if _is_postgres():
        _init_pg_pool()
        raw = _pg_pool.getconn()
        return _PgConnectionWrapper(raw)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                email TEXT UNIQUE,
                phone TEXT UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contacts (
                contact_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                display_name TEXT,
                given_name TEXT,
                family_name TEXT,
                phone_numbers TEXT,
                email_addresses TEXT,
                postal_addresses TEXT,
                organization TEXT,
                job_title TEXT,
                notes TEXT,
                photo_uri TEXT,
                source_device_id TEXT,
                local_id TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                sync_status TEXT NOT NULL DEFAULT 'synced',
                hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                deleted_at TEXT,
                UNIQUE(user_id, local_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_ack_log (
                ack_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                acked_until TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS conflict_log (
                conflict_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                local_id TEXT NOT NULL,
                contact_id TEXT,
                conflict_type TEXT NOT NULL,
                local_payload TEXT NOT NULL,
                server_payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolved_payload TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS contact_history (
                history_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                contact_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                snapshot TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dedupe_ignore (
                ignore_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                pair_key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, pair_key)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                token_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                expires_at TEXT NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                audit_id TEXT PRIMARY KEY,
                user_id TEXT,
                action TEXT NOT NULL,
                target_type TEXT,
                target_id TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_user_updated ON contacts(user_id, updated_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_user_deleted ON contacts(user_id, deleted_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_ack_user_device ON sync_ack_log(user_id, device_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_conflict_user_status ON conflict_log(user_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_history_contact_version ON contact_history(contact_id, version)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_ignore_user_pair ON dedupe_ignore(user_id, pair_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_refresh_user_revoked ON refresh_tokens(user_id, revoked)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_user_time ON audit_log(user_id, created_at)")
        conn.commit()
    finally:
        conn.close()
