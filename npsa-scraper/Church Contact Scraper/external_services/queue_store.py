"""
SQLite-backed job queue for deferred pipeline runs + multi-replica county dispatch.

Requires SQLITE_PATH (e.g. /data/npsa_queue.sqlite3 on Railway).

Multi-replica church runs (same DB on a shared volume):
  - SQLITE_PATH on the shared volume (WAL-friendly filesystem).
  - EPHEMERAL_DATA_DIR (or ensure RUNS_DIR/checkpoints/metadata are on that same volume)
    so every replica sees the same county CSVs and metadata.
  - WORKERS_PER_REPLICA — processes per container claiming counties (default follows MAX_WORKERS).
  - CHURCH_REPLICA_COUNT — replica count for ETA only (default 1).
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timedelta
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


def init_db(*, log_ready: bool = True) -> bool:
    if not is_enabled():
        if log_ready:
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
            # Multi-replica county dispatch (church pipeline): shared SQLite + shared RUNS_DIR required
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS county_dispatch (
                    run_id TEXT PRIMARY KEY,
                    scraper_type TEXT NOT NULL DEFAULT 'church',
                    state TEXT NOT NULL,
                    total_counties INTEGER NOT NULL,
                    cancelled INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    meta_json TEXT,
                    aggregation_owner TEXT,
                    aggregation_started_at TEXT,
                    aggregation_done INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_county_dispatch_active
                ON county_dispatch (scraper_type, cancelled, aggregation_done)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS county_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    scraper_type TEXT NOT NULL DEFAULT 'church',
                    state TEXT NOT NULL,
                    county TEXT NOT NULL,
                    county_index INTEGER NOT NULL,
                    total_counties INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    claimed_by TEXT,
                    claimed_at TEXT,
                    completed_at TEXT,
                    result_json TEXT,
                    error TEXT,
                    UNIQUE (run_id, county)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_county_tasks_claim
                ON county_tasks (scraper_type, status, run_id, county_index)
                """
            )
            conn.commit()
        finally:
            conn.close()
    if log_ready:
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


# --- County task queue (multi-replica church pipeline) ---


def dispatch_exists(run_id: str) -> bool:
    if not is_enabled():
        return False
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM county_dispatch WHERE run_id = ? LIMIT 1",
                (run_id,),
            ).fetchone()
            return row is not None
        finally:
            conn.close()


def has_active_church_county_pipeline() -> bool:
    """True if any church dispatch is not finished (cancelled runs still block until cleaned — caller may refine)."""
    if not is_enabled():
        return False
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                """
                SELECT 1 FROM county_dispatch
                WHERE scraper_type = 'church' AND cancelled = 0 AND aggregation_done = 0
                LIMIT 1
                """
            ).fetchone()
            return row is not None
        finally:
            conn.close()


def active_dispatch_run_id_for_state(state: str, scraper_type: str = "church") -> Optional[str]:
    """Run ID for this state until aggregation is done (counties draining or finalizing)."""
    if not is_enabled():
        return None
    s = state.lower().strip()
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                """
                SELECT d.run_id FROM county_dispatch d
                WHERE d.scraper_type = ?
                  AND lower(d.state) = ?
                  AND d.cancelled = 0
                  AND d.aggregation_done = 0
                ORDER BY d.created_at DESC
                LIMIT 1
                """,
                (scraper_type, s),
            ).fetchone()
            return str(row["run_id"]) if row else None
        finally:
            conn.close()


def register_dispatch_run(
    run_id: str,
    state: str,
    total_counties: int,
    scraper_type: str,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    now = datetime.now().isoformat()
    meta_json = json.dumps(meta or {})
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO county_dispatch (
                    run_id, scraper_type, state, total_counties, cancelled, created_at, meta_json,
                    aggregation_owner, aggregation_started_at, aggregation_done
                )
                VALUES (?, ?, ?, ?, 0, ?, ?, NULL, NULL, 0)
                """,
                (run_id, scraper_type, state.lower(), int(total_counties), now, meta_json),
            )
            conn.commit()
        finally:
            conn.close()


def update_dispatch_meta(run_id: str, patch: dict[str, Any]) -> None:
    """Merge patch into meta_json (read-modify-write)."""
    if not is_enabled():
        return
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT meta_json FROM county_dispatch WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if not row:
                return
            cur = json.loads(row["meta_json"] or "{}")
            cur.update(patch)
            conn.execute(
                "UPDATE county_dispatch SET meta_json = ? WHERE run_id = ?",
                (json.dumps(cur), run_id),
            )
            conn.commit()
        finally:
            conn.close()


def cancel_dispatch_run(run_id: str) -> bool:
    if not is_enabled():
        return False
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(
                "UPDATE county_dispatch SET cancelled = 1 WHERE run_id = ? AND cancelled = 0",
                (run_id,),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def is_dispatch_cancelled(run_id: str) -> bool:
    if not is_enabled():
        return False
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT cancelled FROM county_dispatch WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return bool(row and row["cancelled"])
        finally:
            conn.close()


def seed_county_tasks(
    run_id: str,
    state: str,
    scraper_type: str,
    counties: list[tuple[int, str]],
    total_counties: int,
) -> None:
    """Insert pending tasks for (county_index, county_name). Idempotent per (run_id, county)."""
    if not counties:
        return
    st = state.lower()
    with _lock:
        conn = _connect()
        try:
            for idx, county in counties:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO county_tasks (
                        run_id, scraper_type, state, county, county_index, total_counties, status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'pending')
                    """,
                    (run_id, scraper_type, st, county, int(idx), int(total_counties)),
                )
            conn.commit()
        finally:
            conn.close()


def claim_next_county_task(worker_id: str, scraper_type: str) -> Optional[dict[str, Any]]:
    """Atomically claim one pending task across all active dispatches. Returns task row dict or None."""
    if not is_enabled():
        return None
    now = datetime.now().isoformat()
    with _lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT ct.id FROM county_tasks ct
                INNER JOIN county_dispatch cd ON cd.run_id = ct.run_id
                WHERE ct.scraper_type = ?
                  AND ct.status = 'pending'
                  AND cd.scraper_type = ?
                  AND cd.cancelled = 0
                  AND cd.aggregation_done = 0
                ORDER BY cd.created_at ASC, ct.county_index ASC
                LIMIT 1
                """,
                (scraper_type, scraper_type),
            ).fetchone()
            if not row:
                conn.commit()
                return None
            tid = int(row["id"])
            cur = conn.execute(
                """
                UPDATE county_tasks
                SET status = 'processing', claimed_by = ?, claimed_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (worker_id, now, tid),
            )
            if cur.rowcount == 0:
                conn.commit()
                return None
            out = conn.execute(
                "SELECT id, run_id, state, county, county_index, total_counties FROM county_tasks WHERE id = ?",
                (tid,),
            ).fetchone()
            conn.commit()
            return dict(out) if out else None
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def mark_county_done(run_id: str, county: str, result: dict[str, Any]) -> None:
    now = datetime.now().isoformat()
    payload = json.dumps(result, default=str)
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                UPDATE county_tasks
                SET status = 'done', completed_at = ?, result_json = ?, error = NULL
                WHERE run_id = ? AND county = ?
                """,
                (now, payload, run_id, county),
            )
            conn.commit()
        finally:
            conn.close()


def mark_county_failed(run_id: str, county: str, error: str) -> None:
    now = datetime.now().isoformat()
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                UPDATE county_tasks
                SET status = 'failed', completed_at = ?, error = ?, result_json = NULL
                WHERE run_id = ? AND county = ?
                """,
                (now, error[:4000], run_id, county),
            )
            conn.commit()
        finally:
            conn.close()


def reclaim_stale_county_tasks(stale_seconds: int = 7200) -> int:
    """Reset stuck processing tasks to pending. Returns rows updated."""
    cutoff = (datetime.now() - timedelta(seconds=stale_seconds)).isoformat()
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(
                """
                UPDATE county_tasks
                SET status = 'pending', claimed_by = NULL, claimed_at = NULL
                WHERE status = 'processing'
                  AND (claimed_at IS NULL OR claimed_at < ?)
                """,
                (cutoff,),
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


def get_run_progress(run_id: str) -> dict[str, Any]:
    """Aggregate task stats for a run."""
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS n FROM county_tasks WHERE run_id = ? GROUP BY status
                """,
                (run_id,),
            ).fetchall()
            counts: dict[str, int] = {r["status"]: int(r["n"]) for r in rows}
            total = sum(counts.values())
            done = counts.get("done", 0)
            failed = counts.get("failed", 0)
            pending = counts.get("pending", 0)
            processing = counts.get("processing", 0)
            terminal = done + failed
            # Roll up numeric fields from done tasks
            cur = conn.execute(
                "SELECT result_json FROM county_tasks WHERE run_id = ? AND status = 'done'",
                (run_id,),
            )
            churches = contacts = places = oc = opt = oct = octt = 0
            for r in cur.fetchall():
                if not r["result_json"]:
                    continue
                try:
                    data = json.loads(r["result_json"])
                except json.JSONDecodeError:
                    continue
                churches += int(data.get("churches") or 0)
                contacts += int(data.get("contacts") or 0)
                places += int(data.get("places_api_calls") or 0)
                oc += int(data.get("openai_calls") or 0)
                opt += int(data.get("openai_prompt_tokens") or 0)
                octt += int(data.get("openai_completion_tokens") or 0)
            return {
                "total_tasks": total,
                "done": done,
                "failed": failed,
                "pending": pending,
                "processing": processing,
                "terminal": terminal,
                "churches_found": churches,
                "contacts_found": contacts,
                "places_api_calls_total": places,
                "openai_calls_total": oc,
                "openai_prompt_tokens_total": opt,
                "openai_completion_tokens_total": octt,
            }
        finally:
            conn.close()


def terminal_county_names_ordered(run_id: str) -> list[str]:
    """Counties in done or failed status, ordered by county_index (for checkpoints)."""
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                """
                SELECT county FROM county_tasks
                WHERE run_id = ? AND status IN ('done', 'failed')
                ORDER BY county_index ASC
                """,
                (run_id,),
            ).fetchall()
            return [str(r["county"]) for r in rows]
        finally:
            conn.close()


def all_county_tasks_terminal(run_id: str) -> bool:
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM county_tasks
                WHERE run_id = ? AND status NOT IN ('done', 'failed')
                """,
                (run_id,),
            ).fetchone()
            return int(row["n"]) == 0 if row else True
        finally:
            conn.close()


def get_dispatch_row(run_id: str) -> Optional[dict[str, Any]]:
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT * FROM county_dispatch WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def try_claim_aggregation(run_id: str, claimer_id: str, stale_seconds: int = 7200) -> bool:
    """Single winner for aggregate_final_results across replicas."""
    now = datetime.now().isoformat()
    with _lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT aggregation_owner, aggregation_started_at, aggregation_done, cancelled
                FROM county_dispatch WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if not row or row["aggregation_done"] or row["cancelled"]:
                conn.commit()
                return False
            owner = row["aggregation_owner"]
            started = row["aggregation_started_at"]
            if owner and started:
                try:
                    # ISO from SQLite
                    t0 = datetime.fromisoformat(started.replace("Z", "+00:00"))
                    if t0.tzinfo:
                        t0 = t0.replace(tzinfo=None)
                    age = (datetime.now() - t0).total_seconds()
                    if age < stale_seconds:
                        conn.commit()
                        return False
                except Exception:
                    pass
            cur = conn.execute(
                """
                UPDATE county_dispatch
                SET aggregation_owner = ?, aggregation_started_at = ?
                WHERE run_id = ? AND aggregation_done = 0 AND cancelled = 0
                """,
                (claimer_id, now, run_id),
            )
            ok = cur.rowcount > 0
            conn.commit()
            return ok
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def mark_aggregation_done(run_id: str) -> None:
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "UPDATE county_dispatch SET aggregation_done = 1 WHERE run_id = ?",
                (run_id,),
            )
            conn.commit()
        finally:
            conn.close()


def clear_aggregation_claim(run_id: str) -> None:
    """Allow another replica to retry aggregation after a failed attempt."""
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                UPDATE county_dispatch
                SET aggregation_owner = NULL, aggregation_started_at = NULL
                WHERE run_id = ?
                """,
                (run_id,),
            )
            conn.commit()
        finally:
            conn.close()


def _count_tasks(run_id: str, conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM county_tasks WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    return int(row["n"]) if row else 0


def list_dispatch_pending_aggregation(scraper_type: str = "church") -> list[str]:
    """Run IDs where all tasks terminal but aggregation not done and not cancelled."""
    if not is_enabled():
        return []
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                """
                SELECT d.run_id FROM county_dispatch d
                WHERE d.scraper_type = ?
                  AND d.cancelled = 0
                  AND d.aggregation_done = 0
                """,
                (scraper_type,),
            ).fetchall()
            out: list[str] = []
            for r in rows:
                rid = str(r["run_id"])
                if _count_tasks(rid, conn) == 0:
                    continue
                row2 = conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM county_tasks
                    WHERE run_id = ? AND status NOT IN ('done', 'failed')
                    """,
                    (rid,),
                ).fetchone()
                if row2 and int(row2["n"]) == 0:
                    out.append(rid)
            return out
        finally:
            conn.close()
