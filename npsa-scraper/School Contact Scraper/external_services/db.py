"""
Database abstraction layer — SQLite or Postgres, chosen by environment.

If DATABASE_URL is set → Postgres (psycopg2).
Otherwise falls back to SQLITE_PATH → SQLite (existing behavior).

Usage:
    from external_services.db import get_connection, is_postgres, placeholder

    conn = get_connection()
    conn.execute(f"SELECT * FROM foo WHERE id = {placeholder()}", (some_id,))
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

_lock = threading.Lock()

# Cache the Postgres connection pool (created once)
_pg_pool: Any = None
_pg_pool_lock = threading.Lock()


def database_url() -> Optional[str]:
    return os.getenv("DATABASE_URL", "").strip() or None


def sqlite_path() -> Optional[str]:
    return os.getenv("SQLITE_PATH", "").strip() or None


def is_postgres() -> bool:
    return database_url() is not None


def is_enabled() -> bool:
    return is_postgres() or bool(sqlite_path())


def placeholder() -> str:
    """Return the parameter placeholder for the active backend."""
    return "%s" if is_postgres() else "?"


def adapt_sql(sql: str) -> str:
    """Convert SQLite-flavored SQL to Postgres when needed.

    Handles:
    - ? → %s placeholders
    - INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    - INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY
    - PRAGMA statements (stripped for Postgres)
    - BEGIN IMMEDIATE → BEGIN
    """
    if not is_postgres():
        return sql

    s = sql
    # Parameter placeholders
    s = s.replace("?", "%s")
    # INSERT OR IGNORE
    if "INSERT OR IGNORE" in s.upper():
        s = s.replace("INSERT OR IGNORE", "INSERT")
        s = s.replace("insert or ignore", "INSERT")
        # Add ON CONFLICT DO NOTHING before VALUES if not present
        if "ON CONFLICT" not in s.upper():
            s = s.replace("VALUES", "VALUES", 1)  # keep as-is, add at end
            # Find the closing paren of VALUES(...)
            # Simpler: just append ON CONFLICT DO NOTHING
            s = s.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    # AUTOINCREMENT → SERIAL
    s = s.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    # PRAGMA (no-op in Postgres)
    if s.strip().upper().startswith("PRAGMA"):
        return "SELECT 1"  # no-op
    # BEGIN IMMEDIATE → BEGIN
    s = s.replace("BEGIN IMMEDIATE", "BEGIN")
    return s


def _connect_sqlite() -> sqlite3.Connection:
    path = sqlite_path()
    if not path:
        raise RuntimeError("SQLITE_PATH not set")
    parent = Path(path).parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    conn = sqlite3.connect(path, check_same_thread=False, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _get_pg_pool():
    global _pg_pool
    if _pg_pool is not None:
        return _pg_pool
    with _pg_pool_lock:
        if _pg_pool is not None:
            return _pg_pool
        import psycopg2
        import psycopg2.pool
        import psycopg2.extras
        url = database_url()
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=url,
        )
        return _pg_pool


class _PgConnectionWrapper:
    """Wraps a psycopg2 connection to provide a sqlite3-like interface.

    Specifically: conn.execute(sql, params) returns a cursor with
    .fetchone(), .fetchall(), .lastrowid, .rowcount, and rows
    accessible as dicts.
    """

    def __init__(self, raw_conn):
        import psycopg2.extras
        self._conn = raw_conn
        self._conn.autocommit = False

    def execute(self, sql: str, params=None):
        import psycopg2.extras
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        adapted = adapt_sql(sql)
        # Handle RETURNING for INSERT to get lastrowid
        cur.execute(adapted, params)
        return _PgCursorWrapper(cur)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        """Return connection to pool instead of closing."""
        try:
            self._conn.rollback()  # ensure clean state
        except Exception:
            pass
        pool = _get_pg_pool()
        pool.putconn(self._conn)

    @property
    def raw(self):
        return self._conn


class _PgCursorWrapper:
    """Wraps psycopg2 cursor to behave like sqlite3 cursor."""

    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return _DictRow(row)

    def fetchall(self):
        rows = self._cur.fetchall()
        return [_DictRow(r) for r in rows]

    @property
    def lastrowid(self):
        return self._cur.fetchone()[0] if self._cur.description else None

    @property
    def rowcount(self):
        return self._cur.rowcount


class _DictRow(dict):
    """Dict subclass that also supports item access by key (like sqlite3.Row)."""

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


def get_connection():
    """Get a database connection (SQLite or Postgres)."""
    if is_postgres():
        pool = _get_pg_pool()
        raw = pool.getconn()
        return _PgConnectionWrapper(raw)
    else:
        return _connect_sqlite()


@contextmanager
def connection():
    """Context manager for database connections. Auto-closes on exit."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
