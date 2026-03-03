import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "contactsync.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_user_updated ON contacts(user_id, updated_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_contacts_user_deleted ON contacts(user_id, deleted_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_ack_user_device ON sync_ack_log(user_id, device_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_conflict_user_status ON conflict_log(user_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_history_contact_version ON contact_history(contact_id, version)")
        conn.commit()
    finally:
        conn.close()
