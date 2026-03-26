"""
Postgres-backed state management for pipeline runs.
Replaces in-memory pipeline_runs dict, filesystem checkpoints, and filesystem metadata.
Enables multi-replica deployment by storing all mutable state in shared Postgres.
"""

import os
import json
import time
import threading
from datetime import datetime
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, Json

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable must be set")

# Connection pool (simple thread-local approach)
_local = threading.local()


def _get_conn() -> psycopg2.extensions.connection:
    """Get a thread-local Postgres connection, reconnecting if needed."""
    conn = getattr(_local, "conn", None)
    if conn is None or conn.closed:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        _local.conn = conn
    return conn


def _execute(query: str, params: tuple = (), fetch: str = "none") -> Any:
    """Execute a query with automatic reconnect on failure."""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch == "one":
                result = cur.fetchone()
            elif fetch == "all":
                result = cur.fetchall()
            else:
                result = cur.rowcount
            conn.commit()
            return result
    except Exception:
        conn.rollback()
        # Try once more with a fresh connection
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None
        conn = _get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch == "one":
                result = cur.fetchone()
            elif fetch == "all":
                result = cur.fetchall()
            else:
                result = cur.rowcount
            conn.commit()
            return result


# ─── Schema initialization ───────────────────────────────────────────────────

def init_tables():
    """Create tables if they don't exist. Safe to call multiple times."""
    try:
        conn = _get_conn()
    except Exception as e:
        print(f"[DB] ERROR: Failed to connect to Postgres: {e}")
        raise
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                scraper_type TEXT NOT NULL DEFAULT 'church',
                state TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                display_name TEXT,
                progress INTEGER DEFAULT 0,
                current_step INTEGER DEFAULT 1,
                total_steps INTEGER DEFAULT 7,
                status_message TEXT,
                total_contacts INTEGER DEFAULT 0,
                total_contacts_with_emails INTEGER DEFAULT 0,
                total_contacts_without_emails INTEGER DEFAULT 0,
                churches_found INTEGER DEFAULT 0,
                churches_processed INTEGER DEFAULT 0,
                csv_filename TEXT,
                final_csv_path TEXT,
                error TEXT,
                counties_processed INTEGER DEFAULT 0,
                total_counties INTEGER DEFAULT 0,
                current_county TEXT,
                current_county_index INTEGER DEFAULT 0,
                county_times JSONB DEFAULT '[]'::jsonb,
                county_contacts JSONB DEFAULT '[]'::jsonb,
                county_churches JSONB DEFAULT '[]'::jsonb,
                start_time DOUBLE PRECISION,
                initial_estimated_time_remaining INTEGER,
                finalizing_at DOUBLE PRECISION,
                completed_at DOUBLE PRECISION,
                queue_job_id INTEGER,
                notify_sent BOOLEAN DEFAULT FALSE,
                archived BOOLEAN DEFAULT FALSE,
                deleted BOOLEAN DEFAULT FALSE,
                deleted_at TEXT,
                cancelled_at TEXT,
                created_at TEXT,
                updated_at TEXT,
                -- Replica coordination
                owner_replica TEXT,
                heartbeat_at DOUBLE PRECISION,
                -- Extra data as JSON for forward-compat
                extra JSONB DEFAULT '{}'::jsonb
            );

            CREATE INDEX IF NOT EXISTS idx_pr_status ON pipeline_runs (status);
            CREATE INDEX IF NOT EXISTS idx_pr_state ON pipeline_runs (state, status);
            CREATE INDEX IF NOT EXISTS idx_pr_scraper ON pipeline_runs (scraper_type, status);

            CREATE TABLE IF NOT EXISTS checkpoints (
                run_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                completed_counties JSONB DEFAULT '[]'::jsonb,
                next_county_index INTEGER DEFAULT 0,
                total_counties INTEGER DEFAULT 0,
                timestamp DOUBLE PRECISION,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS queue_jobs (
                id SERIAL PRIMARY KEY,
                scraper_type TEXT NOT NULL,
                state TEXT NOT NULL,
                display_name TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                run_id TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_qj_pending
                ON queue_jobs (scraper_type, id)
                WHERE status = 'queued';
        """)
        conn.commit()
        print("[DB] Postgres tables initialized")
    except Exception as e:
        print(f"[DB] ERROR: Failed to create tables: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise


# ─── Pipeline runs ───────────────────────────────────────────────────────────

def create_run(run_id: str, state: str, scraper_type: str = "church",
               display_name: str = "", queue_job_id: Optional[int] = None) -> dict:
    """Create a new pipeline run entry. Returns the row as dict."""
    now = datetime.now().isoformat()
    _execute("""
        INSERT INTO pipeline_runs (
            run_id, scraper_type, state, status, display_name,
            start_time, queue_job_id, created_at, updated_at,
            status_message
        ) VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status = 'running',
            start_time = EXCLUDED.start_time,
            updated_at = EXCLUDED.updated_at,
            status_message = EXCLUDED.status_message
    """, (run_id, scraper_type, state.lower(), display_name,
          time.time(), queue_job_id, now, now,
          f"Starting pipeline for {state}..."))
    return get_run(run_id)


def get_run(run_id: str) -> Optional[dict]:
    """Get a single run by ID. Returns None if not found or deleted."""
    row = _execute(
        "SELECT * FROM pipeline_runs WHERE run_id = %s AND deleted = FALSE",
        (run_id,), fetch="one"
    )
    return dict(row) if row else None


def get_run_including_deleted(run_id: str) -> Optional[dict]:
    """Get a single run by ID, including deleted."""
    row = _execute(
        "SELECT * FROM pipeline_runs WHERE run_id = %s",
        (run_id,), fetch="one"
    )
    return dict(row) if row else None


def update_run(run_id: str, **fields) -> None:
    """Update specific fields on a run. Only updates provided fields."""
    if not fields:
        return
    fields["updated_at"] = datetime.now().isoformat()
    set_clauses = ", ".join(f"{k} = %s" for k in fields)
    values = list(fields.values()) + [run_id]
    _execute(
        f"UPDATE pipeline_runs SET {set_clauses} WHERE run_id = %s",
        tuple(values)
    )


def update_run_json_append(run_id: str, field: str, value: Any) -> None:
    """Append a value to a JSONB array field (county_times, county_contacts, etc.)."""
    _execute(
        f"UPDATE pipeline_runs SET {field} = {field} || %s::jsonb, updated_at = %s WHERE run_id = %s",
        (json.dumps([value]), datetime.now().isoformat(), run_id)
    )


def increment_run(run_id: str, **fields) -> None:
    """Increment integer fields atomically (churches_found, churches_processed, etc.)."""
    if not fields:
        return
    set_clauses = ", ".join(f"{k} = COALESCE({k}, 0) + %s" for k in fields)
    values = list(fields.values()) + [datetime.now().isoformat(), run_id]
    _execute(
        f"UPDATE pipeline_runs SET {set_clauses}, updated_at = %s WHERE run_id = %s",
        tuple(values)
    )


def delete_run(run_id: str) -> None:
    """Soft-delete a run."""
    update_run(run_id, deleted=True, deleted_at=datetime.now().isoformat())


def hard_delete_run(run_id: str) -> None:
    """Permanently delete a run and its checkpoint."""
    _execute("DELETE FROM pipeline_runs WHERE run_id = %s", (run_id,))
    _execute("DELETE FROM checkpoints WHERE run_id = %s", (run_id,))


def list_runs(scraper_type: str = "church", include_archived: bool = False) -> list:
    """List all non-deleted runs, optionally including archived."""
    if include_archived:
        rows = _execute(
            "SELECT * FROM pipeline_runs WHERE scraper_type = %s AND deleted = FALSE ORDER BY created_at DESC",
            (scraper_type,), fetch="all"
        )
    else:
        rows = _execute(
            "SELECT * FROM pipeline_runs WHERE scraper_type = %s AND deleted = FALSE AND archived = FALSE ORDER BY created_at DESC",
            (scraper_type,), fetch="all"
        )
    return [dict(r) for r in rows]


def get_active_states(scraper_type: str = "church") -> set:
    """Return set of state slugs with status='running' and recent heartbeat."""
    rows = _execute("""
        SELECT DISTINCT state FROM pipeline_runs
        WHERE scraper_type = %s AND status = 'running' AND deleted = FALSE
    """, (scraper_type,), fetch="all")
    return {r["state"] for r in rows}


def get_running_runs(scraper_type: str = "church") -> list:
    """Get all runs with status 'running'."""
    rows = _execute("""
        SELECT * FROM pipeline_runs
        WHERE scraper_type = %s AND status = 'running' AND deleted = FALSE
    """, (scraper_type,), fetch="all")
    return [dict(r) for r in rows]


def get_finalizing_runs(scraper_type: str = "church") -> list:
    """Get all runs with status 'finalizing'."""
    rows = _execute("""
        SELECT * FROM pipeline_runs
        WHERE scraper_type = %s AND status = 'finalizing' AND deleted = FALSE
    """, (scraper_type,), fetch="all")
    return [dict(r) for r in rows]


def is_state_running(state: str, scraper_type: str = "church", exclude_run_id: str = None) -> bool:
    """Check if a state has an active running run."""
    if exclude_run_id:
        row = _execute("""
            SELECT 1 FROM pipeline_runs
            WHERE scraper_type = %s AND LOWER(state) = %s AND status = 'running'
              AND deleted = FALSE AND run_id != %s
            LIMIT 1
        """, (scraper_type, state.lower(), exclude_run_id), fetch="one")
    else:
        row = _execute("""
            SELECT 1 FROM pipeline_runs
            WHERE scraper_type = %s AND LOWER(state) = %s AND status = 'running' AND deleted = FALSE
            LIMIT 1
        """, (scraper_type, state.lower()), fetch="one")
    return row is not None


def is_state_finalizing(state: str, scraper_type: str = "church") -> bool:
    """Check if a state has a finalizing run within the grace period."""
    row = _execute("""
        SELECT 1 FROM pipeline_runs
        WHERE scraper_type = %s AND LOWER(state) = %s AND status = 'finalizing'
          AND deleted = FALSE AND finalizing_at IS NOT NULL
          AND (EXTRACT(EPOCH FROM NOW()) - finalizing_at) < 120
        LIMIT 1
    """, (scraper_type, state.lower()), fetch="one")
    return row is not None


def cleanup_stale_runs(scraper_type: str = "church", stale_seconds: float = 600) -> set:
    """
    Mark runs as cancelled if heartbeat is stale (no update for stale_seconds).
    Transition old finalizing runs to completed.
    Returns set of currently active state slugs.
    """
    now = time.time()

    # Expire stale 'running' runs (heartbeat too old)
    _execute("""
        UPDATE pipeline_runs
        SET status = 'cancelled', status_message = 'Cancelled: heartbeat stale', updated_at = %s
        WHERE scraper_type = %s AND status = 'running' AND deleted = FALSE
          AND heartbeat_at IS NOT NULL AND (%s - heartbeat_at) > %s
    """, (datetime.now().isoformat(), scraper_type, now, stale_seconds))

    # Transition old 'finalizing' runs to 'completed'
    _execute("""
        UPDATE pipeline_runs
        SET status = 'completed', completed_at = %s, updated_at = %s,
            status_message = 'Completed (auto-transitioned from finalizing)'
        WHERE scraper_type = %s AND status = 'finalizing' AND deleted = FALSE
          AND finalizing_at IS NOT NULL AND (%s - finalizing_at) >= 120
    """, (now, datetime.now().isoformat(), scraper_type, now))

    return get_active_states(scraper_type)


def heartbeat(run_id: str) -> None:
    """Update heartbeat timestamp to signal this replica is still processing."""
    _execute(
        "UPDATE pipeline_runs SET heartbeat_at = %s WHERE run_id = %s",
        (time.time(), run_id)
    )


# ─── Checkpoints ─────────────────────────────────────────────────────────────

def save_checkpoint(run_id: str, state: str, completed_counties: list,
                    next_county_index: int, total_counties: int) -> bool:
    """Save or update checkpoint in Postgres."""
    try:
        _execute("""
            INSERT INTO checkpoints (run_id, state, completed_counties, next_county_index, total_counties, timestamp, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                completed_counties = EXCLUDED.completed_counties,
                next_county_index = EXCLUDED.next_county_index,
                total_counties = EXCLUDED.total_counties,
                timestamp = EXCLUDED.timestamp,
                updated_at = EXCLUDED.updated_at
        """, (run_id, state, json.dumps(completed_counties), next_county_index,
              total_counties, time.time(), datetime.now().isoformat()))
        print(f"[{run_id}] Checkpoint saved: {len(completed_counties)}/{total_counties} counties completed")
        return True
    except Exception as e:
        print(f"[{run_id}] Error saving checkpoint: {e}")
        return False


def load_checkpoint(run_id: str) -> Optional[dict]:
    """Load checkpoint from Postgres."""
    row = _execute(
        "SELECT * FROM checkpoints WHERE run_id = %s",
        (run_id,), fetch="one"
    )
    if not row:
        return None
    result = dict(row)
    # Ensure completed_counties is a list
    if isinstance(result.get("completed_counties"), str):
        result["completed_counties"] = json.loads(result["completed_counties"])
    print(f"[{run_id}] Checkpoint loaded: {len(result.get('completed_counties', []))}/{result.get('total_counties', 0)} counties completed")
    return result


def delete_checkpoint(run_id: str) -> None:
    """Delete checkpoint for a run."""
    _execute("DELETE FROM checkpoints WHERE run_id = %s", (run_id,))


# ─── Queue jobs (replaces SQLite queue_store) ────────────────────────────────

def queue_enqueue(state: str, scraper_type: str, display_name: str) -> dict:
    """Insert a queued job. Returns {job_id, position}."""
    state = state.lower().strip()
    now = datetime.now().isoformat()
    row = _execute("""
        INSERT INTO queue_jobs (scraper_type, state, display_name, status, created_at)
        VALUES (%s, %s, %s, 'queued', %s)
        RETURNING id
    """, (scraper_type, state, display_name, now), fetch="one")
    job_id = row["id"]
    pos_row = _execute("""
        SELECT COUNT(*) as cnt FROM queue_jobs
        WHERE scraper_type = %s AND status = 'queued' AND id <= %s
    """, (scraper_type, job_id), fetch="one")
    return {"job_id": job_id, "position": int(pos_row["cnt"])}


def queue_list_jobs(scraper_type: str, limit: int = 100) -> list:
    """List queue jobs ordered by newest first."""
    rows = _execute("""
        SELECT id, state, display_name, status, run_id, error, created_at, started_at, finished_at
        FROM queue_jobs
        WHERE scraper_type = %s
        ORDER BY id DESC
        LIMIT %s
    """, (scraper_type, limit), fetch="all")
    return [dict(r) for r in rows]


def queue_cancel_job(job_id: int, scraper_type: str) -> bool:
    """Cancel if still queued. Returns True if a row was updated."""
    now = datetime.now().isoformat()
    count = _execute("""
        UPDATE queue_jobs
        SET status = 'cancelled', finished_at = %s
        WHERE id = %s AND scraper_type = %s AND status = 'queued'
    """, (now, job_id, scraper_type))
    return count > 0


def queue_peek_next(scraper_type: str) -> Optional[tuple]:
    """Return (job_id, state) for oldest queued job, or None."""
    row = _execute("""
        SELECT id, state FROM queue_jobs
        WHERE scraper_type = %s AND status = 'queued'
        ORDER BY id ASC
        LIMIT 1
    """, (scraper_type,), fetch="one")
    return (row["id"], row["state"]) if row else None


def queue_mark_running(job_id: int, run_id: str, scraper_type: str) -> bool:
    """Move job from queued to running. Returns False if not queued."""
    now = datetime.now().isoformat()
    count = _execute("""
        UPDATE queue_jobs
        SET status = 'running', run_id = %s, started_at = %s
        WHERE id = %s AND scraper_type = %s AND status = 'queued'
    """, (run_id, now, job_id, scraper_type))
    return count > 0


def queue_finalize(run_id: str, scraper_type: str, pipeline_status: str,
                   error_message: Optional[str] = None) -> None:
    """Update queue row when pipeline thread ends."""
    if pipeline_status in ("completed", "finalizing"):
        final_status = "done"
    elif pipeline_status == "cancelled":
        final_status = "cancelled"
    else:
        final_status = "failed"
        error_message = error_message or f"Pipeline ended with status={pipeline_status}"

    now = datetime.now().isoformat()
    err = error_message if final_status == "failed" else None
    _execute("""
        UPDATE queue_jobs
        SET status = %s, finished_at = %s, error = COALESCE(%s, error)
        WHERE run_id = %s AND scraper_type = %s AND status = 'running'
    """, (final_status, now, err, run_id, scraper_type))


# ─── Leader election via advisory lock ────────────────────────────────────────

QUEUE_WORKER_LOCK_ID = 839201  # Arbitrary constant for pg_advisory_lock


def try_acquire_queue_leader() -> bool:
    """
    Try to acquire the queue worker advisory lock (non-blocking).
    Returns True if this replica is the leader.
    Advisory locks are session-scoped — held until connection closes.
    """
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (QUEUE_WORKER_LOCK_ID,))
        result = cur.fetchone()[0]
        conn.commit()
    return result


def release_queue_leader() -> None:
    """Release the advisory lock."""
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (QUEUE_WORKER_LOCK_ID,))
            conn.commit()
    except Exception:
        pass


# ─── Distributed county dispatch ─────────────────────────────────────────────

REPLICA_ID = os.getenv("RAILWAY_REPLICA_ID") or os.getenv("HOSTNAME") or f"replica-{os.getpid()}"


def dispatch_counties(run_id: str, state: str, counties: list, scraper_type: str = "church") -> None:
    """
    Publish all counties for a run into county_tasks as 'pending'.
    Also create a county_dispatch row to track the overall dispatch.
    Called by the leader replica that starts the run.
    """
    now = datetime.now().isoformat()
    total = len(counties)
    conn = _get_conn()
    with conn.cursor() as cur:
        # Create dispatch record
        cur.execute("""
            INSERT INTO county_dispatch (run_id, scraper_type, state, total_counties, cancelled, created_at, aggregation_done)
            VALUES (%s, %s, %s, %s, 0, %s, 0)
            ON CONFLICT (run_id) DO UPDATE SET total_counties = %s, cancelled = 0, aggregation_done = 0
        """, (run_id, scraper_type, state, total, now, total))
        # Insert all county tasks
        for idx, county in enumerate(counties):
            cur.execute("""
                INSERT INTO county_tasks (run_id, scraper_type, state, county, county_index, total_counties, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending')
            """, (run_id, scraper_type, state, county, idx, total))
        conn.commit()
    print(f"[DISPATCH] Published {total} counties for {state} (run {run_id[:8]})")


def claim_county_task(scraper_type: str = "church") -> Optional[dict]:
    """
    Claim the next pending county task using FOR UPDATE SKIP LOCKED.
    Returns the task row dict or None if nothing available.
    """
    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, run_id, state, county, county_index, total_counties
            FROM county_tasks
            WHERE scraper_type = %s AND status = 'pending'
            ORDER BY run_id, county_index
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """, (scraper_type,))
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        # Mark as claimed
        cur.execute("""
            UPDATE county_tasks SET status = 'running', claimed_by = %s, claimed_at = %s
            WHERE id = %s
        """, (REPLICA_ID, datetime.now().isoformat(), row["id"]))
        conn.commit()
    return dict(row)


def complete_county_task(task_id: int, result: dict, csv_data: Optional[str] = None,
                         row_count: int = 0) -> dict:
    """
    Mark a county task as completed and store results.
    Returns summary dict with completion status.
    """
    now = datetime.now().isoformat()
    result_json = json.dumps(result)
    conn = _get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            UPDATE county_tasks SET status = 'completed', completed_at = %s, result_json = %s
            WHERE id = %s
            RETURNING run_id, county
        """, (now, result_json, task_id))
        updated = cur.fetchone()
        # Store CSV data in county_results if available
        if csv_data and updated:
            cur.execute("""
                INSERT INTO county_results (run_id, county, csv_rows, row_count, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (updated["run_id"], updated["county"], csv_data, row_count, now))
        conn.commit()
    return dict(updated) if updated else {}


def fail_county_task(task_id: int, error: str) -> None:
    """Mark a county task as failed."""
    _execute("""
        UPDATE county_tasks SET status = 'failed', completed_at = %s, error = %s
        WHERE id = %s
    """, (datetime.now().isoformat(), error, task_id))


def get_dispatch_progress(run_id: str) -> dict:
    """Get progress for a dispatched run: how many counties pending/running/completed/failed."""
    rows = _execute("""
        SELECT status, COUNT(*) as cnt FROM county_tasks
        WHERE run_id = %s GROUP BY status
    """, (run_id,), fetch="all")
    counts = {r["status"]: r["cnt"] for r in (rows or [])}
    total = sum(counts.values())
    completed = counts.get("completed", 0) + counts.get("failed", 0)
    return {
        "total": total,
        "pending": counts.get("pending", 0),
        "running": counts.get("running", 0),
        "completed": counts.get("completed", 0),
        "failed": counts.get("failed", 0),
        "done": completed >= total and total > 0,
    }


def try_claim_aggregation(run_id: str) -> bool:
    """
    Atomically claim aggregation for a run. Returns True if this replica got it.
    Only one replica should aggregate results.
    """
    now = datetime.now().isoformat()
    count = _execute("""
        UPDATE county_dispatch
        SET aggregation_owner = %s, aggregation_started_at = %s
        WHERE run_id = %s AND aggregation_owner IS NULL AND aggregation_done = 0
    """, (REPLICA_ID, now, run_id))
    return count > 0


def mark_aggregation_done(run_id: str) -> None:
    """Mark aggregation as complete for a run."""
    _execute("""
        UPDATE county_dispatch SET aggregation_done = 1
        WHERE run_id = %s
    """, (run_id,))


def get_county_results(run_id: str) -> list:
    """Get all county CSV results for a run."""
    rows = _execute("""
        SELECT county, csv_rows, row_count FROM county_results
        WHERE run_id = %s ORDER BY county
    """, (run_id,), fetch="all")
    return [dict(r) for r in (rows or [])]


def get_completed_county_tasks(run_id: str) -> list:
    """Get all completed county tasks with their results."""
    rows = _execute("""
        SELECT county, county_index, result_json, completed_at
        FROM county_tasks
        WHERE run_id = %s AND status IN ('completed', 'failed')
        ORDER BY county_index
    """, (run_id,), fetch="all")
    return [dict(r) for r in (rows or [])]


def cancel_dispatch(run_id: str) -> None:
    """Cancel all pending county tasks for a run."""
    _execute("""
        UPDATE county_tasks SET status = 'cancelled'
        WHERE run_id = %s AND status IN ('pending', 'running')
    """, (run_id,))
    _execute("""
        UPDATE county_dispatch SET cancelled = 1
        WHERE run_id = %s
    """, (run_id,))


def cleanup_dispatch(run_id: str) -> None:
    """Clean up dispatch tables for a run after aggregation."""
    _execute("DELETE FROM county_tasks WHERE run_id = %s", (run_id,))
    _execute("DELETE FROM county_results WHERE run_id = %s", (run_id,))
    _execute("DELETE FROM county_dispatch WHERE run_id = %s", (run_id,))


def is_dispatch_active(scraper_type: str = "church") -> Optional[str]:
    """Check if there's an active dispatch (non-aggregated). Returns run_id or None."""
    row = _execute("""
        SELECT run_id FROM county_dispatch
        WHERE scraper_type = %s AND aggregation_done = 0 AND cancelled = 0
        ORDER BY created_at DESC LIMIT 1
    """, (scraper_type,), fetch="one")
    return row["run_id"] if row else None


# ─── Helper: convert DB row to legacy pipeline_runs format ──────────────────

def row_to_pipeline_format(row: dict) -> dict:
    """Convert a Postgres row to the dict format that the frontend expects
    (matching the old in-memory pipeline_runs structure)."""
    if not row:
        return {}
    return {
        "status": row.get("status", "unknown"),
        "state": row.get("state", ""),
        "progress": row.get("progress", 0),
        "currentStep": row.get("current_step", 1),
        "totalSteps": row.get("total_steps", 7),
        "statusMessage": row.get("status_message", ""),
        "steps": [],
        "totalContacts": row.get("total_contacts", 0),
        "totalContactsWithEmails": row.get("total_contacts_with_emails", 0),
        "totalContactsNoEmails": row.get("total_contacts_without_emails", 0),
        "totalContactsWithoutEmails": row.get("total_contacts_without_emails", 0),
        "churchesFound": row.get("churches_found", 0),
        "churchesProcessed": row.get("churches_processed", 0),
        "csvData": None,  # Don't send CSV data over API
        "csvFilename": row.get("csv_filename"),
        "csvNoEmailsData": None,
        "csvNoEmailsFilename": None,
        "error": row.get("error"),
        "countiesProcessed": row.get("counties_processed", 0),
        "totalCounties": row.get("total_counties", 0),
        "currentCounty": row.get("current_county"),
        "currentCountyIndex": row.get("current_county_index", 0),
        "countyTimes": row.get("county_times", []),
        "countyContacts": row.get("county_contacts", []),
        "countyChurches": row.get("county_churches", []),
        "startTime": row.get("start_time"),
        "initialEstimatedTimeRemaining": row.get("initial_estimated_time_remaining"),
        "finalizingAt": row.get("finalizing_at"),
        "completedAt": row.get("completed_at"),
        "queue_job_id": row.get("queue_job_id"),
        "notify_sent": row.get("notify_sent", False),
    }


def row_to_list_format(row: dict) -> dict:
    """Convert a Postgres row to the format expected by the /runs endpoint."""
    if not row:
        return {}
    return {
        "run_id": row.get("run_id"),
        "scraper_type": row.get("scraper_type", "church"),
        "state": row.get("state", ""),
        "status": row.get("status", "unknown"),
        "display_name": row.get("display_name", ""),
        "created_at": row.get("created_at", ""),
        "updated_at": row.get("updated_at", ""),
        "start_time": row.get("start_time"),
        "completed_at": row.get("completed_at"),
        "archived": row.get("archived", False),
        "total_contacts": row.get("total_contacts", 0),
        "total_contacts_with_emails": row.get("total_contacts_with_emails", 0),
        "total_counties": row.get("total_counties", 0),
        "counties_processed": row.get("counties_processed", 0),
        "completed_counties": [],  # Loaded from checkpoint if needed
        "progress": row.get("progress", 0),
        "churches_found": row.get("churches_found", 0),
        "final_csv_path": row.get("final_csv_path"),
        "csv_filename": row.get("csv_filename"),
        "county_churches": row.get("county_churches", []),
    }
