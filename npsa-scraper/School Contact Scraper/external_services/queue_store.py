"""
SQLite-backed job queue for deferred pipeline runs.
Requires SQLITE_PATH (e.g. /data/npsa_queue.sqlite3 on Railway).
"""

from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

_lock = threading.Lock()


def db_path() -> Optional[str]:
    p = os.getenv("SQLITE_PATH", "").strip()
    return p or None


def is_enabled() -> bool:
    return bool(db_path())


def _connect() -> sqlite3.Connection:
    path = db_path()
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


def init_db() -> bool:
    if not is_enabled():
        print("[QUEUE] SQLITE_PATH not set — queue disabled")
        return False
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scraper_type TEXT NOT NULL,
                    state TEXT NOT NULL,
                    display_name TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    run_id TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_queue_pending
                ON queue_jobs (scraper_type, id)
                WHERE status = 'queued'
                """
            )
            conn.commit()
        finally:
            conn.close()
    print(f"[QUEUE] SQLite queue ready at {db_path()}")
    return True


def enqueue(state: str, scraper_type: str, display_name: str) -> dict[str, Any]:
    """Insert a queued job. Returns {job_id, position}."""
    state = state.lower().strip()
    now = datetime.now().isoformat()
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(
                """
                INSERT INTO queue_jobs (scraper_type, state, display_name, status, created_at)
                VALUES (?, ?, ?, 'queued', ?)
                """,
                (scraper_type, state, display_name, now),
            )
            job_id = cur.lastrowid
            pos = conn.execute(
                """
                SELECT COUNT(*) FROM queue_jobs
                WHERE scraper_type = ? AND status = 'queued' AND id <= ?
                """,
                (scraper_type, job_id),
            ).fetchone()[0]
            conn.commit()
        finally:
            conn.close()
    return {"job_id": job_id, "position": int(pos)}


def list_jobs(scraper_type: str, limit: int = 100) -> list[dict[str, Any]]:
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(
                """
                SELECT id, state, display_name, status, run_id, error, created_at, started_at, finished_at
                FROM queue_jobs
                WHERE scraper_type = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (scraper_type, limit),
            )
            rows = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    return rows


def cancel_queued_job(job_id: int, scraper_type: str) -> bool:
    """Cancel if still queued. Returns True if a row was updated."""
    with _lock:
        conn = _connect()
        try:
            now = datetime.now().isoformat()
            cur = conn.execute(
                """
                UPDATE queue_jobs
                SET status = 'cancelled', finished_at = ?
                WHERE id = ? AND scraper_type = ? AND status = 'queued'
                """,
                (now, job_id, scraper_type),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def peek_next_queued(scraper_type: str) -> Optional[tuple[int, str]]:
    """Return (job_id, state) for oldest queued job, or None."""
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(
                """
                SELECT id, state FROM queue_jobs
                WHERE scraper_type = ? AND status = 'queued'
                ORDER BY id ASC
                LIMIT 1
                """,
                (scraper_type,),
            )
            row = cur.fetchone()
            return (row["id"], row["state"]) if row else None
        finally:
            conn.close()


def mark_job_running(job_id: int, run_id: str, scraper_type: str) -> bool:
    """Move job from queued to running with run_id. Returns False if not queued."""
    now = datetime.now().isoformat()
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(
                """
                UPDATE queue_jobs
                SET status = 'running', run_id = ?, started_at = ?
                WHERE id = ? AND scraper_type = ? AND status = 'queued'
                """,
                (run_id, now, job_id, scraper_type),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def finalize_job_for_run_id(run_id: str, scraper_type: str, pipeline_status: str, error_message: Optional[str]) -> None:
    """Update queue row matching run_id when pipeline thread ends."""
    if pipeline_status in ("completed", "finalizing"):
        final_status = "done"
    elif pipeline_status == "cancelled":
        final_status = "cancelled"
    elif pipeline_status == "error":
        final_status = "failed"
    else:
        final_status = "failed"
        error_message = error_message or f"Pipeline ended with status={pipeline_status}"

    now = datetime.now().isoformat()
    err = error_message if final_status == "failed" else None
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                UPDATE queue_jobs
                SET status = ?, finished_at = ?, error = COALESCE(?, error)
                WHERE run_id = ? AND scraper_type = ? AND status = 'running'
                """,
                (final_status, now, err, run_id, scraper_type),
            )
            conn.commit()
        finally:
            conn.close()


def reconcile_stale_running(scraper_type: str) -> None:
    """Mark running jobs with no matching in-memory run as failed (e.g. after crash)."""
    # Optional v1: skip — worker single-process
    pass
