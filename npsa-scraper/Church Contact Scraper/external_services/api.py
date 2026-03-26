"""
Backend API for Church Scraper Pipeline
Deploy this to Railway to handle POST requests from Vercel frontend
Processes states county-by-county to avoid timeout issues
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import subprocess
import os
import json
from datetime import datetime
import pandas as pd
import threading
import uuid
import time
from pathlib import Path
import requests
import sys
import gc  # For explicit garbage collection
from multiprocessing import Pool
import resource  # For memory monitoring
import logging  # For logging
import platform  # For OS detection
import multiprocessing  # For subprocess isolation
import re  # For regex validation
import shutil
from typing import Optional

# Try to import psutil for process tree killing (optional)
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# CRITICAL: Ensure dumb-init is PID 1 for proper process reaping
# If Railway or another platform overrides the Dockerfile ENTRYPOINT,
# this check ensures dumb-init still runs as PID 1
# PID 1 check removed - no longer needed as dumb-init is properly configured

# Add parent directory to path to import pipeline
sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline import StreamingPipeline
from assets.shared.models import Contact

# Import authentication module
from external_services.auth import require_auth, verify_password, generate_token
from external_services.notify import send_run_complete_email, send_test_notification_email
from external_services import db_state

SCRAPER_TYPE = "church"

# Handle hyphens in filenames using importlib.util
import importlib.util
_parent_dir = Path(__file__).parent.parent
_steps_dir = _parent_dir / "steps"

def load_module_with_hyphen(filename, module_name):
    """Load a Python module from a file with hyphens in the filename"""
    spec = importlib.util.spec_from_file_location(module_name, _steps_dir / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

step11_contact_splitter = load_module_with_hyphen('step11-contact_splitter.py', 'step11_contact_splitter')
step12_hunter_io = load_module_with_hyphen('step12-enrichment.py', 'step12_enrichment')
step13_final_compiler = load_module_with_hyphen('step13-compiler.py', 'step13_compiler')

app = Flask(__name__)

# Verify dumb-init is PID 1 on startup (will only log once when app initializes)
# Only log success, not warnings (warnings are handled in __main__ block)
if HAS_PSUTIL:
    try:
        pid1 = psutil.Process(1)
        pid1_name = pid1.name().lower()
        if 'dumb-init' in pid1_name or 'init' in pid1_name:
            print(f"[INIT] Verified: dumb-init is PID 1 - process reaping enabled")
    except:
        pass  # Can't verify, but continue

# CORS configuration - restrict to allowed origin (REQUIRED in production)
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN")
if ALLOWED_ORIGIN:
    # Normalize origin - remove trailing slashes to prevent CORS mismatch
    ALLOWED_ORIGIN = ALLOWED_ORIGIN.rstrip('/')
    print(f"CORS: Using ALLOWED_ORIGIN = {ALLOWED_ORIGIN}")
else:
    # Allow wildcard only if explicitly set to "*" for development
    # In production, ALLOWED_ORIGIN must be set to your frontend URL
    import sys
    print("WARNING: ALLOWED_ORIGIN not set. Defaulting to '*' for development.")
    print("SECURITY: Set ALLOWED_ORIGIN environment variable in production to your frontend URL.")
    ALLOWED_ORIGIN = "*"

# Configure CORS to automatically handle OPTIONS preflight requests
CORS(app, 
     resources={r"/*": {
         "origins": ALLOWED_ORIGIN if ALLOWED_ORIGIN != "*" else "*",
         "methods": ["GET", "POST", "DELETE", "OPTIONS", "PUT"],
         "allow_headers": ["Content-Type", "Authorization"],
         "expose_headers": ["Content-Type"],
         "supports_credentials": False,
         "max_age": 86400  # 24 hours
     }},
     automatic_options=True  # Automatically handle OPTIONS requests
)

# Track running threads and cancellation flags (local to this replica only)
# Format: {run_id: {'thread': Thread, 'cancelled': bool}}
running_threads = {}


def _unique_running_states_after_stale_cleanup() -> set:
    """Expire stale runs in Postgres; return set of state slugs still running."""
    return db_state.cleanup_stale_runs(SCRAPER_TYPE)


def _same_state_running(state: str) -> bool:
    return db_state.is_state_running(state, SCRAPER_TYPE)


def _state_finalizing(state: str) -> bool:
    return db_state.is_state_finalizing(state, SCRAPER_TYPE)


def _church_queue_worker_loop():
    """Queue worker loop. Uses Postgres advisory lock so only one replica processes the queue."""
    while True:
        time.sleep(2.5)
        try:
            # Try to become the queue leader (non-blocking)
            if not db_state.try_acquire_queue_leader():
                continue
            active = _unique_running_states_after_stale_cleanup()
            if len(active) >= 2:
                continue
            nxt = db_state.queue_peek_next(SCRAPER_TYPE)
            if not nxt:
                continue
            job_id, st = nxt
            if _same_state_running(st):
                continue
            if _state_finalizing(st):
                continue
            if len(active) >= 2:
                continue
            run_id = str(uuid.uuid4())
            if not db_state.queue_mark_running(job_id, run_id, SCRAPER_TYPE):
                continue
            run_streaming_pipeline(st, run_id, False, job_id)
        except Exception as e:
            print(f"[QUEUE] worker error: {e}")
            import traceback
            traceback.print_exc()

def _distributed_county_worker_loop():
    """
    County worker loop that runs on EVERY replica.
    Claims pending county tasks from Postgres and processes them locally.
    This is how distributed processing works — all replicas pull from the same queue.
    """
    while True:
        time.sleep(1.0)  # Poll interval
        try:
            task = db_state.claim_county_task(SCRAPER_TYPE)
            if not task:
                continue

            task_id = task["id"]
            run_id = task["run_id"]
            state = task["state"]
            county = task["county"]
            county_index = task["county_index"]
            total_counties = task["total_counties"]

            print(f"[WORKER] Claimed {county} County (task {task_id}) for {state} run {run_id[:8]}")

            # Check if run has been cancelled before processing
            run_row = db_state.get_run(run_id)
            if not run_row or run_row.get("status") == "cancelled":
                print(f"[WORKER] Run {run_id[:8]} is cancelled, skipping {county}")
                db_state.fail_county_task(task_id, "Run cancelled")
                continue

            # Process in an isolated subprocess via Pool(1, maxtasksperchild=1)
            # This ensures Chrome processes die when the worker dies
            args = (county_index, county, state, run_id, total_counties)

            try:
                with Pool(processes=1, maxtasksperchild=1) as pool:
                    results_iter = pool.map(process_county_worker_multiprocessing, [args])
                    idx, county_name, result, processing_time = results_iter[0]
            except Exception as e:
                print(f"[WORKER] {county} County crashed: {e}")
                db_state.fail_county_task(task_id, str(e))
                gc.collect()
                continue

            if not result.get("success", False):
                error_msg = result.get("error", "Unknown error")
                print(f"[WORKER] {county} County failed: {error_msg}")
                db_state.fail_county_task(task_id, error_msg)
            else:
                # Read the CSV data from the county output file to store in Postgres
                csv_data = None
                row_count = 0
                run_dir = RUNS_DIR / run_id
                county_csv = run_dir / county.replace(' ', '_') / "final_contacts.csv"
                if county_csv.exists():
                    try:
                        csv_data = county_csv.read_text()
                        row_count = max(0, csv_data.count('\n') - 1)  # subtract header
                    except Exception:
                        pass

                db_state.complete_county_task(task_id, result, csv_data, row_count)
                print(f"[WORKER] Completed {county} County: {result.get('contacts', 0)} contacts in {processing_time:.0f}s")

            # Update pipeline_runs progress from county_tasks counts
            progress = db_state.get_dispatch_progress(run_id)
            completed_count = progress["completed"] + progress["failed"]
            total = progress["total"]
            progress_pct = int((completed_count / total) * 100) if total > 0 else 0

            db_state.update_run(run_id,
                progress=progress_pct,
                counties_processed=completed_count,
                status_message=f"Processing {completed_count}/{total} counties...",
                current_county=f"Processing {completed_count}/{total} counties"
                    if completed_count < total
                    else f"All {total} counties completed",
            )
            if result.get("success", False):
                db_state.increment_run(run_id,
                    churches_found=result.get("churches", 0),
                    churches_processed=result.get("churches", 0),
                )
                db_state.update_run_json_append(run_id, "county_times", processing_time)
                db_state.update_run_json_append(run_id, "county_contacts", result.get("contacts", 0))
                db_state.update_run_json_append(run_id, "county_churches", result.get("churches", 0))
            db_state.heartbeat(run_id)

            # Check if all counties are done — if so, try to claim aggregation
            if progress["done"]:
                print(f"[WORKER] All counties done for {run_id[:8]}, attempting aggregation claim...")
                if db_state.try_claim_aggregation(run_id):
                    print(f"[WORKER] Won aggregation for {run_id[:8]}, starting...")
                    try:
                        _distributed_aggregate(run_id, state)
                    except Exception as e:
                        print(f"[WORKER] Aggregation failed: {e}")
                        import traceback
                        traceback.print_exc()
                        db_state.update_run(run_id, status="error", error=str(e))
                    finally:
                        db_state.mark_aggregation_done(run_id)
                        # Finalize queue job
                        row = db_state.get_run(run_id)
                        if row and row.get("queue_job_id") is not None:
                            try:
                                db_state.queue_finalize(run_id, SCRAPER_TYPE,
                                    str(row.get("status", "error")),
                                    row.get("error"))
                            except Exception as qe:
                                print(f"[WORKER] queue finalize error: {qe}")
                else:
                    print(f"[WORKER] Another replica is aggregating {run_id[:8]}")

            # Explicit GC after each county
            gc.collect()

        except Exception as e:
            print(f"[WORKER] Error in county worker loop: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)  # Back off on error


def _distributed_aggregate(run_id: str, state: str):
    """
    Aggregation function for distributed processing.
    Reads county CSV data from county_results table instead of local filesystem,
    then runs the standard aggregation steps (dedup, enrichment, final CSV).
    """
    from models import Contact
    import pandas as pd
    from io import StringIO

    print(f"[AGG] Starting aggregation for {run_id[:8]}")
    db_state.update_run(run_id, status_message="Aggregating results from all counties...")

    # Get all county results from Postgres
    county_results = db_state.get_county_results(run_id)
    completed_tasks = db_state.get_completed_county_tasks(run_id)

    # Also read any local CSV files (from counties processed by this replica)
    run_dir = RUNS_DIR / run_id
    counties = load_counties_from_state(state)
    total_counties = len(counties)

    all_contacts = []
    for cr in county_results:
        if not cr.get("csv_rows"):
            continue
        try:
            df = pd.read_csv(StringIO(cr["csv_rows"]))
            if len(df) > 0:
                for _, row in df.iterrows():
                    def sval(val):
                        return str(val).strip() if val is not None and not (isinstance(val, float) and pd.isna(val)) else ""

                    email_raw = row.get('email', '') or row.get('Email', '')
                    email_val = sval(email_raw)
                    if not email_val:
                        email_val = None

                    contact = Contact(
                        first_name=sval(row.get('first_name', '') or row.get('First Name', '')),
                        last_name=sval(row.get('last_name', '') or row.get('Last Name', '')),
                        title=sval(row.get('title', '') or row.get('Title', '')),
                        email=email_val,
                        phone=sval(row.get('phone', '') or row.get('Phone', '')),
                        church_name=sval(row.get('church_name', '') or row.get('Church Name', '')),
                        source_url=sval(row.get('source_url', '') or row.get('Source URL', '')),
                    )
                    all_contacts.append(contact)
        except Exception as e:
            print(f"[AGG] Error reading CSV for {cr.get('county', '?')}: {e}")

    print(f"[AGG] Collected {len(all_contacts)} total contacts from {len(county_results)} counties")

    if not all_contacts:
        print(f"[AGG] No contacts found, completing run")
        db_state.update_run(run_id,
            status="finalizing",
            finalizing_at=time.time(),
            progress=100,
            total_contacts=0,
            status_message=f"Pipeline completed: {total_counties}/{total_counties} counties processed. 0 contacts found.",
        )
        _finalize_completion(run_id, state, total_counties)
        return

    # Dedup contacts
    seen = set()
    unique_contacts = []
    for c in all_contacts:
        key = (c.first_name, c.last_name, c.church_name)
        if key not in seen:
            seen.add(key)
            unique_contacts.append(c)

    contacts_with_emails = [c for c in unique_contacts if c.has_email()]
    contacts_without_emails = [c for c in unique_contacts if not c.has_email()]

    print(f"[AGG] After dedup: {len(unique_contacts)} unique ({len(contacts_with_emails)} with email, {len(contacts_without_emails)} without)")

    # Generate final CSV
    db_state.update_run(run_id, status_message="Generating final CSV...")
    csv_filename = f"{state.lower().replace(' ', '_')}_church_contacts.csv"

    run_dir.mkdir(parents=True, exist_ok=True)
    final_csv_path = str(run_dir / csv_filename)

    # Write CSV
    if unique_contacts:
        df_data = [c.to_dict() for c in unique_contacts]
        df = pd.DataFrame(df_data)
        df.to_csv(final_csv_path, index=False)
        print(f"[AGG] Wrote {len(df)} contacts to {csv_filename}")

    db_state.update_run(run_id,
        csv_filename=csv_filename,
        final_csv_path=final_csv_path,
        total_contacts=len(unique_contacts),
        total_contacts_with_emails=len(contacts_with_emails),
        total_contacts_without_emails=len(contacts_without_emails),
        status="finalizing",
        finalizing_at=time.time(),
        progress=100,
        status_message=f"Pipeline completed: {total_counties} counties, {len(unique_contacts)} contacts. Finalizing...",
    )

    _finalize_completion(run_id, state, total_counties)

    # Cleanup dispatch tables
    db_state.cleanup_dispatch(run_id)
    print(f"[AGG] Aggregation complete for {run_id[:8]}")


def _finalize_completion(run_id: str, state: str, total_counties: int):
    """Background thread to transition from finalizing to completed after grace period."""
    def finalize():
        time.sleep(120)
        row = db_state.get_run(run_id)
        if row and row.get("status") == "finalizing":
            completed_now = time.time()
            db_state.update_run(run_id,
                status="completed",
                completed_at=completed_now,
                status_message=f"Pipeline completed: {total_counties} counties processed",
            )
            if not row.get("notify_sent"):
                duration = completed_now - (row.get("start_time") or completed_now)
                send_run_complete_email(
                    run_id, state, total_counties, total_counties,
                    row.get("total_contacts", 0),
                    row.get("total_contacts_with_emails", 0),
                    duration,
                )
                db_state.update_run(run_id, notify_sent=True)

    t = threading.Thread(target=finalize, daemon=True)
    t.start()


# Track 404 requests for non-existent run IDs to prevent spam from stale browser tabs
# Format: {run_id: (first_404_time, count)}
not_found_runs = {}

# Container restart logic removed - container stays running after completion (matches backup branch)

# Storage configuration
# Ephemeral: runs, checkpoints, metadata during processing (cleaned up at run end)
EPHEMERAL_DATA_DIR = Path(os.getenv("EPHEMERAL_DATA_DIR", "/tmp/npsa_data"))
EPHEMERAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

RUNS_DIR = EPHEMERAL_DATA_DIR / "runs"
CHECKPOINTS_DIR = EPHEMERAL_DATA_DIR / "checkpoints"
METADATA_DIR = EPHEMERAL_DATA_DIR / "metadata"

RUNS_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# Volume: only the final state CSV is persisted here
VOLUME_DIR = Path(os.getenv("PERSISTENT_DATA_DIR", "/data"))
VOLUME_DIR.mkdir(parents=True, exist_ok=True)

# Automatic cleanup configuration
# Delete runs older than this many days (default: 7 days)
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "7"))

def cleanup_old_runs():
    """
    Clean up old completed runs to prevent storage exhaustion.
    Deletes runs older than CLEANUP_DAYS that are completed or cancelled.
    """
    try:
        import shutil
        from datetime import timedelta

        cutoff_date = datetime.now() - timedelta(days=CLEANUP_DAYS)
        deleted_count = 0
        freed_space = 0

        # Get all metadata files
        for metadata_file in METADATA_DIR.glob("*.json"):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)

                # Skip if already deleted or still running
                if metadata.get("deleted", False):
                    continue

                status = metadata.get("status")
                if status not in ["completed", "cancelled", "error"]:
                    continue

                # Check creation date
                created_at_str = metadata.get("created_at", "")
                if not created_at_str:
                    continue

                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    if created_at.tzinfo:
                        created_at = created_at.replace(tzinfo=None)
                except Exception:
                    try:
                        created_at = datetime.strptime(created_at_str.split('.')[0], '%Y-%m-%dT%H:%M:%S')
                    except Exception:
                        continue

                if created_at < cutoff_date:
                    run_id = metadata_file.stem
                    run_dir = RUNS_DIR / run_id
                    checkpoint_file = CHECKPOINTS_DIR / f"{run_id}.json"

                    # Delete ephemeral files
                    try:
                        if run_dir.exists():
                            total_size = sum(f.stat().st_size for f in run_dir.rglob('*') if f.is_file())
                            shutil.rmtree(run_dir)
                            freed_space += total_size

                        if checkpoint_file.exists():
                            freed_space += checkpoint_file.stat().st_size
                            checkpoint_file.unlink()

                        # Delete final CSV from volume (only thing that was persisted)
                        final_csv = metadata.get("final_csv_path")
                        if final_csv and Path(final_csv).exists():
                            try:
                                Path(final_csv).unlink()
                                freed_space += Path(final_csv).stat().st_size
                            except Exception:
                                pass
                    except Exception as e:
                        print(f"[CLEANUP] Error deleting files for {run_id}: {e}")
                        continue

                    # Mark as deleted in metadata
                    metadata["deleted"] = True
                    metadata["deleted_at"] = datetime.now().isoformat()
                    metadata["auto_deleted"] = True
                    save_run_metadata(run_id, metadata)

                    deleted_count += 1
                    print(f"[CLEANUP] Auto-deleted old run {run_id} (created: {created_at_str})")
            except Exception as e:
                print(f"[CLEANUP] Error processing metadata file {metadata_file}: {e}")
                continue

        if deleted_count > 0:
            freed_mb = freed_space / (1024 * 1024)
            print(f"[CLEANUP] Cleaned up {deleted_count} old runs, freed ~{freed_mb:.2f} MB")
        else:
            print(f"[CLEANUP] No old runs to clean up (cutoff: {cutoff_date.isoformat()})")
    except Exception as e:
        print(f"[CLEANUP] Error during cleanup: {e}")
        import traceback
        traceback.print_exc()


def cleanup_ephemeral_run(run_id: str):
    """
    Delete all ephemeral run data after final CSV has been saved to volume.
    Removes run dir (county CSVs, chrome_tmp, etc.) and checkpoint.
    """
    try:
        run_dir = RUNS_DIR / run_id
        checkpoint_file = CHECKPOINTS_DIR / f"{run_id}.json"
        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)
            print(f"[{run_id}] Cleaned up ephemeral run data")
        if checkpoint_file.exists():
            try:
                checkpoint_file.unlink()
            except Exception:
                pass
    except Exception as e:
        print(f"[{run_id}] Error during ephemeral cleanup: {e}")


# Batch size for checkpointing (save checkpoint every N counties)
# Set to 1 to save after every county for better recovery
CHECKPOINT_BATCH_SIZE = int(os.getenv("CHECKPOINT_BATCH_SIZE", "1"))

# Number of parallel workers for processing counties
# Default to 4 for parallel processing
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

# Estimated seconds per "parallel slot" for ETA math: (counties / MAX_WORKERS) * this value.
# Calibrated from Alabama full-state run ≈ 42h wall-clock, 67 counties, MAX_WORKERS=4:
#   42 * 3600 / (67 / 4) ≈ 9024. Override with CHURCH_AVG_SECONDS_PER_COUNTY_SLOT if needed.
CHURCH_AVG_SECONDS_PER_COUNTY_SLOT = int(
    os.getenv("CHURCH_AVG_SECONDS_PER_COUNTY_SLOT", "9024")
)

# Thread locks for thread-safe operations
checkpoint_lock = threading.Lock()
progress_lock = threading.Lock()

# ANSI escape codes for bold text
BOLD = '\033[1m'
RESET = '\033[0m'

def bold(text: str) -> str:
    """Make text bold in terminal output"""
    return f"{BOLD}{text}{RESET}"


# Security: Validate run_id to prevent path traversal attacks
UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
# Also allow timestamp-style IDs (e.g. 20260315211349) from /data - alphanumeric, no path chars
SAFE_RUN_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{8,64}$')

def validate_run_id(run_id: str) -> bool:
    """
    Validate that run_id is safe (UUID or alphanumeric).
    Prevents path traversal attacks (e.g., '../../../etc/passwd').
    Accepts UUID format or timestamp-style IDs from /data.
    """
    if not run_id or not isinstance(run_id, str):
        return False
    s = run_id.strip()
    return bool(UUID_PATTERN.match(s) or SAFE_RUN_ID_PATTERN.match(s))


def list_chrome_processes():
    """
    Health check function that lists all Chrome and ChromeDriver processes.
    Does NOT kill processes - only reports what exists.
    
    Returns:
        dict with process information: {
            'chrome_count': int,
            'chromedriver_count': int,
            'total_count': int,
            'chrome_processes': list of {'pid': int, 'name': str},
            'chromedriver_processes': list of {'pid': int, 'name': str}
        }
    """
    if not HAS_PSUTIL:
        return {
            'chrome_count': 0,
            'chromedriver_count': 0,
            'total_count': 0,
            'chrome_processes': [],
            'chromedriver_processes': []
        }
    
    try:
        chrome_processes = []
        chromedriver_processes = []
        
        for proc in psutil.process_iter(['name', 'pid']):
            try:
                name = proc.info['name'].lower()
                pid = proc.info['pid']
                
                if 'chromedriver' in name:
                    chromedriver_processes.append({'pid': pid, 'name': proc.info['name']})
                elif 'chrome' in name and 'chromedriver' not in name:
                    chrome_processes.append({'pid': pid, 'name': proc.info['name']})
                elif 'chromium' in name:
                    chrome_processes.append({'pid': pid, 'name': proc.info['name']})
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        return {
            'chrome_count': len(chrome_processes),
            'chromedriver_count': len(chromedriver_processes),
            'total_count': len(chrome_processes) + len(chromedriver_processes),
            'chrome_processes': chrome_processes,
            'chromedriver_processes': chromedriver_processes
        }
    except Exception as e:
        # Error listing processes - silently continue
        pass
        return {
            'chrome_count': 0,
            'chromedriver_count': 0,
            'total_count': 0,
            'chrome_processes': [],
            'chromedriver_processes': []
        }


def get_chrome_process_counts():
    """Get current Chrome process counts: (zombies, orphaned_zombies, active)"""
    if not HAS_PSUTIL:
        return (0, 0, 0)
    
    zombies = 0
    orphaned_zombies = 0
    active = 0
    
    try:
        for proc in psutil.process_iter(['name', 'pid', 'ppid', 'status']):
            try:
                name = proc.info.get('name', '').lower()
                status = proc.info.get('status', '').lower()
                ppid = proc.info.get('ppid', -1)
                
                if ('chrome' in name or 'chromium' in name or 'chromedriver' in name):
                    if status == 'zombie':
                        if ppid == 1:
                            orphaned_zombies += 1
                        else:
                            zombies += 1
                    else:
                        active += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, KeyError):
                continue
    except:
        pass
    
    return (zombies, orphaned_zombies, active)


def kill_chrome_processes_bottom_up(current_pid: int = None):
    """
    Kill all Chrome/Chromium/ChromeDriver processes BOTTOM-UP (children first, then parents).
    Only targets processes in the current worker's process tree (not system-wide).
    Protects main container processes (waitress-serve, main Python process).
    
    This prevents zombie processes by ensuring children are killed before parents.
    
    Args:
        current_pid: PID of current process (defaults to os.getpid())
        
    Returns:
        int: Number of processes killed
    """
    if not HAS_PSUTIL:
        return 0
    
    if current_pid is None:
        current_pid = os.getpid()
    
    # Monitor processes before cleanup
    before_zombies, before_orphaned, before_active = get_chrome_process_counts()
    print(f"{bold('[CLEANUP]')} BEFORE kill_chrome_processes_bottom_up: Active: {before_active}, Zombies: {before_zombies}, Orphaned: {before_orphaned}")
    
    killed_count = 0
    try:
        current_process = psutil.Process(current_pid)
        
        # Identify main container processes to protect
        # These should never be killed, even if they match patterns
        protected_names = {'waitress-serve', 'dumb-init', 'python', 'python3', 'waitress'}
        protected_pids = set()
        
        # Get main container PID (parent or grandparent process)
        try:
            parent = current_process.parent()
            if parent:
                protected_pids.add(parent.pid)
                # Also protect parent's parent (grandparent, likely waitress or main Python)
                try:
                    grandparent = parent.parent()
                    if grandparent:
                        protected_pids.add(grandparent.pid)
                        # Also protect great-grandparent (likely dumb-init or system process)
                        try:
                            great_grandparent = grandparent.parent()
                            if great_grandparent:
                                protected_pids.add(great_grandparent.pid)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        
        # Collect all Chrome processes in worker's process tree ONLY (not system-wide)
        chrome_processes = []
        try:
            for child in current_process.children(recursive=True):
                try:
                    # child.name() returns the process name directly (no .info attribute)
                    name = child.name().lower()
                    # Check if it's a Chrome process
                    if ('chrome' in name or 'chromium' in name or 'chromedriver' in name):
                        # Skip protected processes
                        if child.pid not in protected_pids:
                            if name not in protected_names:
                                chrome_processes.append(child)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, AttributeError):
                    continue
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        
        if not chrome_processes:
            return 0
        
        # Sort processes by depth (deepest children first) - BOTTOM-UP approach
        # Build depth map: calculate depth of each process from current process
        depth_map = {}
        def get_depth(proc):
            if proc.pid in depth_map:
                return depth_map[proc.pid]
            try:
                if proc.pid == current_pid:
                    depth = 0
                else:
                    parent_depth = get_depth(proc.parent()) if proc.parent() else 0
                    depth = parent_depth + 1
                depth_map[proc.pid] = depth
                return depth
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                return 999  # Unknown depth, kill last
        
        # Calculate depths
        for proc in chrome_processes:
            get_depth(proc)
        
        # Sort by depth descending (deepest = children first, shallowest = parents last)
        chrome_processes.sort(key=lambda p: depth_map.get(p.pid, 999), reverse=True)
        
        # Kill processes BOTTOM-UP (deepest children first)
        for proc in chrome_processes:
            try:
                if proc.is_running():
                    # Try graceful termination first
                    proc.terminate()
                    killed_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        # Wait a moment for processes to die
        if killed_count > 0:
            time.sleep(0.5)
            
            # Force kill any remaining processes
            for proc in chrome_processes:
                try:
                    if proc.is_running():
                        proc.kill()  # Force kill if still running
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        
        if killed_count > 0:
            print(f"{bold('[CLEANUP]')} Killed {killed_count} Chrome processes (bottom-up)")
        
        # Monitor processes after cleanup
        after_zombies, after_orphaned, after_active = get_chrome_process_counts()
        print(f"{bold('[CLEANUP]')} AFTER kill_chrome_processes_bottom_up: Active: {after_active}, Zombies: {after_zombies}, Orphaned: {after_orphaned}, Killed: {killed_count}")
        
    except Exception as e:
        print(f"{bold('[CLEANUP]')} Error in bottom-up Chrome cleanup: {e}")
    
    return killed_count


def check_health():
    """
    Health check function that lists Chrome/ChromeDriver processes and kills orphaned ones.
    Orphaned processes (PPID=1) are killed BOTTOM-UP (children first, then parents).
    This prevents Chrome process accumulation in containers where orphaned processes escape cleanup.
    
    Also verifies that dumb-init is running as PID 1 (required for proper process reaping).
    """
    # Health check diagnostics removed - no longer needed
    # Kill orphaned processes (PPID=1) - these are processes reparented to PID 1 in containers
    # This prevents process accumulation when workers don't properly clean up
    if HAS_PSUTIL:
        try:
            orphaned_count = 0
            orphaned_processes = []
            
            # Identify main container processes to protect (never kill these)
            protected_names = {'waitress-serve', 'dumb-init', 'python', 'python3', 'waitress'}
            protected_pids = set()
            
            # Get current process and identify protected processes
            try:
                current_pid = os.getpid()
                current_process = psutil.Process(current_pid)
                try:
                    parent = current_process.parent()
                    if parent:
                        protected_pids.add(parent.pid)
                        try:
                            grandparent = parent.parent()
                            if grandparent:
                                protected_pids.add(grandparent.pid)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            
            # Find orphaned Chrome processes (PPID=1)
            for proc in psutil.process_iter(['name', 'pid', 'ppid']):
                try:
                    name = proc.info.get('name', '').lower()
                    ppid = proc.info.get('ppid', -1)
                    
                    # Check if it's an orphaned Chrome process (PPID=1) and not protected
                    if ppid == 1 and ('chrome' in name or 'chromium' in name or 'chromedriver' in name):
                        pid = proc.info['pid']
                        if pid not in protected_pids and name not in protected_names:
                            orphaned_processes.append(proc)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, KeyError):
                    continue
            
            if orphaned_processes:
                # Sort orphaned processes by depth (children first) - BOTTOM-UP approach
                # Build depth map: processes with more children are deeper
                depth_map = {}
                def get_orphan_depth(proc):
                    if proc.pid in depth_map:
                        return depth_map[proc.pid]
                    try:
                        # Count children depth (deeper if has more descendants)
                        children = proc.children(recursive=True)
                        depth = len(children)
                        depth_map[proc.pid] = depth
                        return depth
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        return 0
                
                # Calculate depths
                for proc in orphaned_processes:
                    get_orphan_depth(proc)
                
                # Sort by depth descending (processes with more children = deeper, kill first)
                orphaned_processes.sort(key=lambda p: depth_map.get(p.pid, 0), reverse=True)
                
                # Kill orphaned processes BOTTOM-UP (processes with more children first)
                for proc in orphaned_processes:
                    try:
                        if proc.is_running():
                            # First, try to kill children of this orphan if any (BOTTOM-UP)
                            try:
                                for child in proc.children(recursive=True):
                                    try:
                                        # child.name() returns the process name directly (no .info attribute)
                                        child_name = child.name().lower()
                                        if ('chrome' in child_name or 'chromium' in child_name or 'chromedriver' in child_name):
                                            if child.is_running():
                                                child.terminate()
                                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, AttributeError):
                                        continue
                                time.sleep(0.3)  # Wait for children to die
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                            
                            # Then kill the orphan process itself
                            proc.terminate()
                            orphaned_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        continue
                
                # Force kill any remaining
                if orphaned_count > 0:
                    time.sleep(0.5)
                    for proc in orphaned_processes:
                        try:
                            if proc.is_running():
                                proc.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            continue
                
                if orphaned_count > 0:
                    # Orphaned processes killed silently
                    pass
        except Exception as e:
            # Error killing orphaned processes - silently continue
            pass
    
    return True


def log_resource_usage():
    """Log resource usage for monitoring"""
    try:
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        memory_mb = rusage.ru_maxrss / 1024  # Convert KB to MB (on Linux)
        logging.info(f"Memory: {memory_mb:.1f}MB")
        print(f"{bold('[RESOURCE]')} Memory: {memory_mb:.1f}MB")
    except Exception as e:
        print(f"{bold('[RESOURCE]')} Error logging resource usage: {e}")




def save_checkpoint(run_id: str, state: str, completed_counties: list, next_county_index: int, total_counties: int):
    """Save checkpoint to Postgres"""
    if not validate_run_id(run_id):
        raise ValueError(f"Invalid run_id format: {run_id}")
    return db_state.save_checkpoint(run_id, state, completed_counties, next_county_index, total_counties)


def load_checkpoint(run_id: str) -> dict:
    """Load checkpoint from Postgres"""
    if not validate_run_id(run_id):
        return None
    return db_state.load_checkpoint(run_id)


def save_run_metadata(run_id: str, metadata: dict):
    """Save run metadata to Postgres (update fields on pipeline_runs row)"""
    if not validate_run_id(run_id):
        raise ValueError(f"Invalid run_id format: {run_id}")
    # Map metadata keys to DB column names
    field_map = {
        "status": "status", "state": "state", "display_name": "display_name",
        "total_counties": "total_counties", "start_time": "start_time",
        "final_csv_path": "final_csv_path", "csv_filename": "csv_filename",
        "total_contacts": "total_contacts",
        "total_contacts_with_emails": "total_contacts_with_emails",
        "total_contacts_without_emails": "total_contacts_without_emails",
        "archived": "archived", "deleted": "deleted", "deleted_at": "deleted_at",
        "cancelled_at": "cancelled_at", "completed_at": "completed_at",
        "progress": "progress", "scraper_type": "scraper_type",
        "churchesFound": "churches_found", "churchesProcessed": "churches_processed",
        "countyChurches": "county_churches",
    }
    db_fields = {}
    for k, v in metadata.items():
        if k in field_map:
            db_fields[field_map[k]] = v
    if db_fields:
        # Check if run exists; if not, create it
        existing = db_state.get_run_including_deleted(run_id)
        if existing:
            db_state.update_run(run_id, **db_fields)
        else:
            db_state.create_run(
                run_id,
                metadata.get("state", "unknown"),
                metadata.get("scraper_type", "church"),
                metadata.get("display_name", ""),
            )
            db_state.update_run(run_id, **db_fields)
    return True


def load_run_metadata(run_id: str) -> dict:
    """Load run metadata from Postgres"""
    if not validate_run_id(run_id):
        return None
    row = db_state.get_run_including_deleted(run_id)
    if not row:
        return None
    return db_state.row_to_list_format(row)


def _run_display_name(state: str, scraper_type: str) -> str:
    """Human-readable label for UI, e.g. 'Alabama Churches' / 'Delaware Schools'."""
    suffix = "Churches" if scraper_type == "church" else "Schools"
    if not state:
        return f"Unknown {suffix}"
    pretty = str(state).replace("_", " ").strip()
    if not pretty:
        return f"Unknown {suffix}"
    pretty = " ".join(w.capitalize() for w in pretty.split())
    return f"{pretty} {suffix}"


def _backfill_run_list_fields(metadata: dict, default_scraper_type: str) -> None:
    """Ensure scraper_type and display_name for /runs JSON (mutates dict)."""
    if "scraper_type" not in metadata:
        metadata["scraper_type"] = default_scraper_type
    st = metadata.get("scraper_type") or default_scraper_type
    if not metadata.get("display_name"):
        metadata["display_name"] = _run_display_name(metadata.get("state") or "", st)


def _parse_run_from_csv_filename(name: str) -> dict | None:
    """Parse run metadata from CSV filename: {state}_leads_{run_id}_{timestamp}.csv"""
    import re
    m = re.match(r"^(.+?)_leads_([a-f0-9\-]{36})_(\d+_\d+)\.csv$", name, re.I)
    if not m:
        return None
    state, run_id, ts = m.group(1), m.group(2), m.group(3)
    return {
        "run_id": run_id,
        "state": state,
        "status": "completed",
        "created_at": ts.replace("_", ""),
        "scraper_type": "church",
        "display_name": _run_display_name(state, "church"),
        "archived": False,
        "_from_volume": True,
    }


def list_all_runs(include_archived: bool = False) -> list:
    """List all runs from Postgres, excluding deleted runs"""
    try:
        rows = db_state.list_runs(SCRAPER_TYPE, include_archived)
        runs = [db_state.row_to_list_format(r) for r in rows]

        # Fallback: include runs from volume CSVs when Postgres entry is missing
        seen_ids = {r["run_id"] for r in runs}
        for f in VOLUME_DIR.glob("*_leads_*.csv"):
            parsed = _parse_run_from_csv_filename(f.name)
            if parsed and parsed["run_id"] not in seen_ids:
                _backfill_run_list_fields(parsed, "church")
                seen_ids.add(parsed["run_id"])
                runs.append(parsed)

        runs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return runs
    except Exception as e:
        print(f"Error listing runs: {e}")
        return []


def load_counties_from_state(state: str) -> list:
    """Load counties for a given state from assets/data/state_counties/{state}.txt
    For Texas, checks for texas_top50.txt first (for faster processing)"""
    state_normalized = state.lower().replace(' ', '_')
    repo_root = Path(__file__).parent.parent
    
    # For Texas, prefer top50 file if it exists
    if state_normalized == 'texas':
        top50_file = repo_root / 'assets' / 'data' / 'state_counties' / 'texas_top50.txt'
        if top50_file.exists():
            print(f"Using top 50 counties file for faster processing")
            state_file = top50_file
        else:
            state_file = repo_root / 'assets' / 'data' / 'state_counties' / f'{state_normalized}.txt'
    else:
        state_file = repo_root / 'assets' / 'data' / 'state_counties' / f'{state_normalized}.txt'
    
    if not state_file.exists():
        raise FileNotFoundError(f"State file not found: {state_file}")
    
    counties = []
    with open(state_file, 'r', encoding='utf-8') as f:
        for line in f:
            county = line.strip()
            if county and not county.startswith('#'):
                counties.append(county)
    
    return counties


def _county_worker(state: str, county: str, run_id: str, county_index: int, total_counties: int, result_file: str):
    """
    Worker function that runs in a subprocess to process a single county.
    When this subprocess dies, all its children (Chrome processes) die automatically.
    
    Args:
        state: State name
        county: County name
        run_id: Run ID
        county_index: Index of county
        total_counties: Total number of counties
        result_file: Path to JSON file to write results to
    """
    try:
        # Update progress for this county
        print(f"[{run_id}] {county} County ({county_index + 1}/{total_counties})")
        
        # Create persistent output directory for this run
        run_dir = RUNS_DIR / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        county_dir = run_dir / county.replace(' ', '_')
        county_dir.mkdir(parents=True, exist_ok=True)
        
        # Create persistent output file (single file for all contacts)
        output_csv = str(county_dir / "final_contacts.csv")
        
        # Chrome tmp dir on volume to avoid ephemeral storage exhaustion; deleted after county completes
        chrome_tmp_dir = county_dir / "chrome_tmp"
        chrome_tmp_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize pipeline for this county
        pipeline = StreamingPipeline(
            google_api_key=os.getenv("GOOGLE_PLACES_API_KEY", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            global_max_api_calls=None,  # No limit for full state runs
            max_pages_per_church=2,  # Reduced from 3 to 2 for faster processing
            state=state,
            chrome_tmp_dir=str(chrome_tmp_dir)
        )
        
        # Run pipeline for this single county - collects all contacts
        pipeline.run(
            counties=[county],  # Process only this county
            batch_size=0,  # Process all churches in county
            output_csv=output_csv
        )
        
        # Pipeline.run() already handles all compilation and writes to output_csv
        # Just verify the file was created
        all_contacts = pipeline.all_contacts
        print(f"[{run_id}] SUCCESS {county}: {len(all_contacts)} contacts")
        
        # Count contacts with and without emails
        contacts_with_emails = [c for c in all_contacts if c.has_email()]
        contacts_without_emails = [c for c in all_contacts if not c.has_email()]
        
        # Read results
        results = {
            'success': True,
            'churches': pipeline.stats.get('churches_processed', 0),
            'contacts': len(all_contacts),
            'contacts_with_emails': len(contacts_with_emails),
            'contacts_without_emails': len(contacts_without_emails),
            'csv_path': output_csv if os.path.exists(output_csv) else None
        }
        
        # Write results to file for main process to read
        with open(result_file, 'w') as f:
            json.dump(results, f)
        
        return results
            
    except Exception as e:
        import traceback
        error_msg = str(e)[:500]
        print(f"Error processing {county}: {error_msg}")
        traceback.print_exc()
        
        results = {'success': False, 'error': error_msg}
        # Write error results to file
        try:
            with open(result_file, 'w') as f:
                json.dump(results, f)
        except:
            pass
        return results
    finally:
        # CRITICAL: Cleanup Chrome processes BOTTOM-UP (children first, then parents)
        # This prevents zombie processes by ensuring children are killed before parents.
        # Only targets worker's process tree (not system-wide) to protect main container.
        
        # Monitor processes before cleanup
        before_zombies, before_orphaned, before_active = get_chrome_process_counts()
        print(f"[{run_id}] [{county}] CLEANUP BEFORE: Chrome processes - Active: {before_active}, Zombies: {before_zombies}, Orphaned: {before_orphaned}")
        
        try:
            # First, try to quit the driver properly
            if 'pipeline' in locals() and pipeline and hasattr(pipeline, 'content_collector') and pipeline.content_collector:
                if pipeline.content_collector.driver:
                    driver = pipeline.content_collector.driver
                    pipeline.content_collector.driver = None
                    try:
                        driver.quit()
                    except:
                        pass
        except Exception:
            pass
        
        # Kill Chrome processes BOTTOM-UP (only worker's process tree, protects main container)
        # This replaces system-wide process_iter() and pkill which could kill parents before children
        kill_chrome_processes_bottom_up()
        
        # Delete Chrome tmp dir (on volume) - no longer needed after county completes
        try:
            chrome_tmp_dir = RUNS_DIR / run_id / county.replace(' ', '_') / "chrome_tmp"
            if chrome_tmp_dir.exists():
                shutil.rmtree(chrome_tmp_dir, ignore_errors=True)
        except Exception:
            pass
        
        # Brief delay after cleanup to provide buffer
        time.sleep(1.0)
        
        # Monitor processes after cleanup
        after_zombies, after_orphaned, after_active = get_chrome_process_counts()
        print(f"[{run_id}] [{county}] CLEANUP AFTER: Chrome processes - Active: {after_active}, Zombies: {after_zombies}, Orphaned: {after_orphaned}")


def process_single_county(state: str, county: str, run_id: str, county_index: int, total_counties: int):
    """
    Process a single county in a subprocess.
    The subprocess will die naturally after completion, taking all Chrome processes with it.
    
    Returns:
        dict with results: {'churches': count, 'contacts': count, 'contacts_no_emails': count, 'success': bool, 'csv_path': str}
    """
    # Create result file for subprocess to write to
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    result_file = str(run_dir / f"{county.replace(' ', '_')}_result.json")
    
    # Update progress
    db_state.update_run(run_id,
        status_message=f"Processing {county} County ({county_index + 1}/{total_counties})...",
        current_county=county,
        current_county_index=county_index + 1,
        current_step=1,
    )
    
    # Start subprocess to run county
    # Use multiprocessing.Process to isolate the county processing
    # When the process dies, all its children (Chrome processes) die automatically
    process = multiprocessing.Process(
        target=_county_worker,
        args=(state, county, run_id, county_index, total_counties, result_file)
    )
    process.start()
    process.join()  # Wait for subprocess to complete
    
    # Read results from file
    try:
        if os.path.exists(result_file):
            with open(result_file, 'r') as f:
                results = json.load(f)
            # Clean up result file
            try:
                os.remove(result_file)
            except:
                pass
            return results
        else:
            return {'success': False, 'error': 'Subprocess did not write results file'}
    except Exception as e:
        return {'success': False, 'error': f'Error reading results: {str(e)}'}




def process_county_worker_multiprocessing(args):
    """
    Worker function for multiprocessing.Pool.
    Processes a single county and returns results.
    Does NOT update global state - that's handled in main process.
    
    This function runs in a Pool worker process, which provides the isolation.
    We call _county_worker directly (no nested subprocess needed).
    
    Args:
        args: tuple of (idx, county, state, run_id, total_counties)
    
    Returns:
        tuple: (idx, county, result_dict, processing_time_seconds)
    """
    idx, county, state, run_id, total_counties = args
    start_time = time.time()
        
    # Create result file for worker to write to
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    result_file = str(run_dir / f"{county.replace(' ', '_')}_result.json")
    
    try:
        # Call _county_worker directly - the Pool worker IS the isolation we need
        # No need for nested multiprocessing.Process since we're already in a worker
        _county_worker(state, county, run_id, idx, total_counties, result_file)
        
        # Read results from file
        if os.path.exists(result_file):
            with open(result_file, 'r') as f:
                result = json.load(f)
            # Clean up result file
            try:
                os.remove(result_file)
            except:
                pass
        else:
            result = {'success': False, 'error': 'Worker did not write results file'}
        
        processing_time = time.time() - start_time
        
        if not result.get('success'):
            print(f"[{run_id}] ERROR {county}: {result.get('error', 'Unknown error')}")
        
        return (idx, county, result, processing_time)
    except Exception as e:
        processing_time = time.time() - start_time
        print(f"[{run_id}] County {county} generated an exception: {e}")
        import traceback
        traceback.print_exc()
        return (idx, county, {'success': False, 'error': str(e)}, processing_time)


def aggregate_final_results(run_id: str, state: str, skip_wait: bool = False):
    """Aggregate all county results, run global Steps 11-13, and generate final CSV
    
    Args:
        run_id: Run ID
        state: State name
        skip_wait: If True, skip waiting for counties and proceed immediately with available data
    """
    try:
        counties = load_counties_from_state(state)
        run_dir = RUNS_DIR / run_id
        
        db_state.update_run(run_id, status_message="Waiting for all counties to complete...")
        
        # Wait for all counties to complete (check that all CSV files exist)
        # With multiprocessing pool, counties should complete more reliably, but add timeout as safety
        # Skip wait if manually triggered (skip_wait=True)
        if not skip_wait:
            max_wait_time = 7200  # 2 hours max wait (increased for large states)
            wait_interval = 5  # Check every 5 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                all_complete = True
                missing_counties = []
                
                for county in counties:
                    county_dir = run_dir / county.replace(' ', '_')
                    county_csv = county_dir / "final_contacts.csv"
                    if not county_csv.exists():
                        all_complete = False
                        missing_counties.append(county)
                    else:
                        # File exists, check if empty
                        file_size = county_csv.stat().st_size if county_csv.exists() else 0
                        # Skip empty files - they'll be handled during aggregation
                
                if all_complete:
                    print(f"[{run_id}] All {len(counties)} counties completed, proceeding with aggregation...")
                    break
                
                if elapsed_time % 30 == 0:  # Log every 30 seconds
                    print(f"[{run_id}] Waiting for {len(missing_counties)} counties to complete: {', '.join(missing_counties[:3])}...")
                
                time.sleep(wait_interval)
                elapsed_time += wait_interval
            
            if not all_complete:
                print(f"[{run_id}] WARNING: Some counties did not complete within timeout. Proceeding with available data...")
        else:
            # Skip wait - proceed immediately with available data
            print(f"[{run_id}] Skipping wait - proceeding with available county data...")
        
        db_state.update_run(run_id, status_message="Aggregating results from all counties...")
        
        # Read and combine all county CSVs into Contact objects
        all_contacts = []
        for county in counties:
            county_dir = run_dir / county.replace(' ', '_')
            county_csv = county_dir / "final_contacts.csv"
            
            if county_csv.exists():
                try:
                    df = pd.read_csv(county_csv)
                    if len(df) > 0:
                        # Convert DataFrame rows to Contact objects
                        for _, row in df.iterrows():
                            # Safe coercion helpers
                            def sval(val):
                                return str(val).strip() if val is not None and not (isinstance(val, float) and pd.isna(val)) else ""
                            
                            email_raw = row.get('email', '') or row.get('Email', '')
                            email_val = sval(email_raw)
                            if not email_val:
                                email_val = None
                            
                            contact = Contact(
                                first_name=sval(row.get('first_name', '') or row.get('First Name', '')),
                                last_name=sval(row.get('last_name', '') or row.get('Last Name', '')),
                                title=sval(row.get('title', '') or row.get('Title', '')),
                                email=email_val,
                                phone=sval(row.get('phone', '') or row.get('Phone', '')),
                                church_name=sval(row.get('church_name', '') or row.get('Church Name', '')),
                                source_url=sval(row.get('source_url', '') or row.get('Source URL', ''))
                            )
                            all_contacts.append(contact)
                        print(f"[{run_id}] Loaded {len(df)} contacts from {county} County")
                except Exception as e:
                    print(f"[{run_id}] Error reading {county_csv}: {e}")
                    import traceback
                    traceback.print_exc()
        
        if not all_contacts:
            print(f"[{run_id}] No contacts found in any county")
            now = time.time()
            db_state.update_run(run_id,
                status="finalizing",
                status_message="Pipeline completed but no contacts found. Finalizing...",
                finalizing_at=now,
                total_contacts=0,
                completed_at=now,
            )

            def finalize_completion():
                time.sleep(120)
                row = db_state.get_run(run_id)
                if row and row.get("status") == "finalizing":
                    db_state.update_run(run_id,
                        status="completed",
                        status_message="Pipeline completed but no contacts found.",
                        completed_at=time.time(),
                    )
                    cleanup_ephemeral_run(run_id)
                    if not row.get("notify_sent"):
                        duration = time.time() - (row.get("start_time") or time.time())
                        send_run_complete_email(run_id, state, len(counties), len(counties), 0, 0, duration)
                        db_state.update_run(run_id, notify_sent=True)

            finalize_thread = threading.Thread(target=finalize_completion, daemon=True)
            finalize_thread.start()
            return
        
        print(f"[{run_id}] Total contacts collected: {len(all_contacts)}")
        
        # STEP 11: Split contacts into with/without emails
        db_state.update_run(run_id, status_message="Step 11: Splitting contacts...")
        splitter = step11_contact_splitter.ContactSplitter()
        contacts_with_emails, contacts_without_emails = splitter.split_contacts(all_contacts)
        
        print(f"[{run_id}] Step 11 complete: {len(contacts_with_emails)} with emails, {len(contacts_without_emails)} without emails")
        
        # Deduplicate BEFORE enrichment (saves Hunter credits)
        compiler = step13_final_compiler.FinalCompiler()
        deduplicated = compiler.deduplicate_contacts_only(contacts_with_emails, contacts_without_emails)
        contacts_with_emails_deduped = [c for c in deduplicated if c.has_email()]
        contacts_without_emails_deduped = [c for c in deduplicated if not c.has_email()]
        print(f"[{run_id}] Deduplication: {len(contacts_with_emails) + len(contacts_without_emails)} -> {len(deduplicated)} (before Hunter)")
        
        # STEP 12: Email Enrichment with Hunter.io (optional, on deduplicated contacts only)
        contacts_enriched = []
        hunter_io_enabled = os.getenv('HUNTER_IO_API_KEY') is not None
        
        if hunter_io_enabled and contacts_without_emails_deduped:
            db_state.update_run(run_id, status_message=f"Step 12: Enriching {len(contacts_without_emails_deduped)} contacts with Hunter.io...")
            try:
                enricher = step12_hunter_io.HunterIOEnricher(
                    api_key=os.getenv('HUNTER_IO_API_KEY'),
                    verify_emails=False,
                    score_threshold=70
                )
                contacts_enriched = enricher.enrich_contact_objects(
                    contacts=contacts_without_emails_deduped,
                    batch_size=10,
                    delay_between_batches=1.0
                )
                print(f"[{run_id}] Step 12 complete: Enriched {len(contacts_enriched)} contacts with emails")
            except Exception as e:
                print(f"[{run_id}] WARNING Email enrichment failed: {e}")
                print(f"[{run_id}]    Continuing without enriched contacts...")
                import traceback
                traceback.print_exc()
        elif not hunter_io_enabled:
            print(f"[{run_id}] Step 12 skipped: HUNTER_IO_API_KEY not set")
        elif not contacts_without_emails_deduped:
            print(f"[{run_id}] Step 12 skipped: No contacts without emails to enrich")
        
        # STEP 13: Compile final CSV (already deduplicated)
        db_state.update_run(run_id, status_message="Step 13: Compiling final CSV...")
        final_output_csv = str(run_dir / f"{state.title()}_leads_final.csv")
        
        final_csv_path = compiler.compile_contacts_to_csv(
            contacts_with_emails=contacts_with_emails_deduped,
            contacts_enriched=contacts_enriched,
            output_csv=final_output_csv,
            state=state,
            already_deduplicated=True
        )
        
        print(f"[{run_id}] Step 13 complete: Final CSV saved to {final_csv_path}")
        
        # Read final CSV and copy to volume (only persistent storage)
        final_data_saved_to_volume = False
        if os.path.exists(final_csv_path):
            with open(final_csv_path, 'r') as f:
                csv_content = f.read()
            
            csv_filename = f"{state.title()}_leads_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            db_state.update_run(run_id, csv_filename=csv_filename)
        
            # Copy final CSV to volume (only thing persisted)
            volume_csv_path = VOLUME_DIR / csv_filename
            volume_saved = False
            try:
                shutil.copy2(final_csv_path, volume_csv_path)
                volume_saved = True
                final_data_saved_to_volume = True
                print(f"[{run_id}] Final CSV saved to volume: {volume_csv_path}")
            except Exception as e:
                print(f"[{run_id}] WARNING: Could not copy final CSV to volume: {e}")
        
            # Count final contacts
            final_df = pd.read_csv(final_csv_path)
            email_col = 'email' if 'email' in final_df.columns else 'Email'
            if email_col in final_df.columns:
                contacts_with_emails_final = final_df[final_df[email_col].notna() & (final_df[email_col] != '') & (final_df[email_col].str.strip() != '')]
                contacts_without_emails_final = final_df[final_df[email_col].isna() | (final_df[email_col] == '') | (final_df[email_col].str.strip() == '')]
            else:
                contacts_with_emails_final = pd.DataFrame()
                contacts_without_emails_final = final_df
            
            db_state.update_run(run_id,
                total_contacts=len(final_df),
                total_contacts_with_emails=len(contacts_with_emails_final),
                total_contacts_without_emails=len(contacts_without_emails_final),
                final_csv_path=str(volume_csv_path) if volume_saved else final_csv_path,
                csv_filename=csv_filename,
                status="finalizing",
            )
        else:
            print(f"[{run_id}] ERROR: Final CSV not created at {final_csv_path}")
            db_state.update_run(run_id,
                total_contacts=len(deduplicated),
                total_contacts_with_emails=len(contacts_with_emails_deduped) + len(contacts_enriched),
                total_contacts_without_emails=len(contacts_without_emails_deduped) - len(contacts_enriched),
            )
        
        # Update final stats - set to finalizing for 2-minute cooldown
        now = time.time()
        db_state.update_run(run_id,
            status="finalizing",
            status_message=f"Pipeline completed! Processed {len(counties)} counties. Enriched {len(contacts_enriched)} contacts. Finalizing...",
            current_step=13,
            progress=100,
            counties_processed=len(counties),
            finalizing_at=now,
        )

        # Clean up ephemeral run data only after final CSV successfully saved to volume
        if final_data_saved_to_volume:
            cleanup_ephemeral_run(run_id)

        # Background thread to transition to completed after 2 minutes
        def finalize_completion():
            time.sleep(120)
            row = db_state.get_run(run_id)
            if row and row.get("status") == "finalizing":
                completed_now = time.time()
                db_state.update_run(run_id,
                    status="completed",
                    status_message=f"Pipeline completed! Processed {len(counties)} counties. Enriched {len(contacts_enriched)} contacts.",
                    completed_at=completed_now,
                )
                if not row.get("notify_sent"):
                    duration = completed_now - (row.get("start_time") or completed_now)
                    send_run_complete_email(
                        run_id, state, len(counties), len(counties),
                        row.get("total_contacts", 0),
                        row.get("total_contacts_with_emails", 0),
                        duration,
                    )
                    db_state.update_run(run_id, notify_sent=True)

        finalize_thread = threading.Thread(target=finalize_completion, daemon=True)
        finalize_thread.start()
        
    except Exception as e:
        db_state.update_run(run_id,
            status="error",
            error=str(e),
            status_message=f"Failed to aggregate results: {str(e)}"
        )
        import traceback
        traceback.print_exc()


def run_streaming_pipeline(
    state: str,
    run_id: str,
    resume_from_checkpoint: bool = False,
    queue_job_id: Optional[int] = None,
):
    """
    Initialize a distributed pipeline run.

    Creates the run in Postgres and publishes all counties to the county_tasks table.
    Actual processing is handled by _distributed_county_worker_loop on every replica.
    This function returns immediately — it does NOT process counties locally.
    """
    try:
        # Initialize progress tracking in Postgres
        db_state.create_run(
            run_id, state, SCRAPER_TYPE,
            display_name=_run_display_name(state, SCRAPER_TYPE),
            queue_job_id=queue_job_id
        )

        # Load counties for state
        counties = load_counties_from_state(state)
        total_counties = len(counties)

        # Estimate time (distributed across all replicas)
        initial_estimate = int(max(1, total_counties / (MAX_WORKERS * 4)) * CHURCH_AVG_SECONDS_PER_COUNTY_SLOT)

        db_state.update_run(run_id,
            total_counties=total_counties,
            status_message=f"Dispatching {total_counties} counties across all replicas...",
            current_county="Dispatching...",
            counties_processed=0,
            initial_estimated_time_remaining=initial_estimate,
        )

        # Publish all counties to distributed task queue
        db_state.dispatch_counties(run_id, state, counties, SCRAPER_TYPE)

        db_state.update_run(run_id,
            status_message=f"Processing 0/{total_counties} counties...",
            current_county="Waiting for workers...",
        )

        print(f"[{run_id}] Dispatched {total_counties} counties for {state} — all replicas will process")

    except FileNotFoundError as e:
        error_msg = f"State file not found. Please ensure assets/data/state_counties/{state.lower().replace(' ', '_')}.txt exists in the repository."
        db_state.update_run(run_id, status="error", error=error_msg, status_message=f"Pipeline failed: {error_msg}")
    except Exception as e:
        error_msg = str(e)[:500]
        print(f"[{run_id}] Error dispatching pipeline: {error_msg}")
        import traceback
        traceback.print_exc()
        db_state.update_run(run_id, status="error", error=error_msg, status_message=f"Pipeline failed: {error_msg}")


@app.route("/", methods=["GET"])
def root():
    """Root endpoint"""
    return jsonify({
        "status": "ok",
        "service": "Church Scraper API",
        "endpoints": {
            "health": "/health",
            "notify-test-email": "/notify/test-email (POST, auth)",
            "run-pipeline": "/run-pipeline (POST)",
            "pipeline-status": "/pipeline-status/<run_id> (GET)"
        }
    }), 200


@app.route("/health", methods=["GET", "OPTIONS"])
def health():
    """Health check endpoint - public, no authentication required"""
    # Flask-CORS handles CORS headers automatically, but we can add explicit headers if needed
    response = jsonify({"status": "healthy"})
    return response, 200


@app.route("/notify/test-email", methods=["POST", "OPTIONS"])
@require_auth
def notify_test_email():
    """Send a dummy email via Resend to NOTIFY_EMAIL (no scrape). Auth required."""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", ALLOWED_ORIGIN if ALLOWED_ORIGIN != "*" else "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    r = send_test_notification_email("Church Scraper")
    if r.get("ok"):
        resp = jsonify({
            "status": "ok",
            "message": "Test email sent to NOTIFY_EMAIL (check inbox and spam).",
        })
        resp.headers.add("Access-Control-Allow-Origin", "*")
        return resp, 200
    err = jsonify({"status": "error", "error": r.get("error", "Unknown error")})
    err.headers.add("Access-Control-Allow-Origin", "*")
    return err, 400


@app.route("/debug/volume", methods=["GET", "OPTIONS"])
@require_auth
def debug_volume():
    """List /data (persistent volume) contents - runs, CSVs, metadata. Auth required."""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", ALLOWED_ORIGIN if ALLOWED_ORIGIN != "*" else "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        return response, 200
    try:
        data_path = Path(os.getenv("PERSISTENT_DATA_DIR", "/data"))
        if not data_path.exists():
            return jsonify({
                "path": str(data_path),
                "exists": False,
                "error": "Path does not exist"
            }), 200
        entries = []
        total_size = 0
        file_count = 0
        csv_files = []
        for item in sorted(data_path.iterdir()):
            try:
                if item.is_dir():
                    sub_count = 0
                    sub_size = 0
                    sub_csvs = []
                    for sub in sorted(item.iterdir()):
                        try:
                            if sub.is_file():
                                sub_count += 1
                                sz = sub.stat().st_size
                                sub_size += sz
                                if sub.suffix.lower() == ".csv":
                                    sub_csvs.append({"name": sub.name, "size": sz})
                            elif sub.is_dir():
                                sub_count += 1
                                for f in sub.rglob("*"):
                                    if f.is_file():
                                        sub_count += 1
                                        sz = f.stat().st_size
                                        sub_size += sz
                                        if f.suffix.lower() == ".csv":
                                            sub_csvs.append({"name": f.name, "path": str(f.relative_to(data_path)), "size": sz})
                        except OSError:
                            pass
                    entries.append({
                        "name": item.name,
                        "type": "dir",
                        "entries": sub_count,
                        "size_bytes": sub_size,
                        "size_mb": round(sub_size / (1024 * 1024), 2),
                        "csvs": sub_csvs[:20]
                    })
                    total_size += sub_size
                    file_count += sub_count
                else:
                    sz = item.stat().st_size
                    total_size += sz
                    file_count += 1
                    entries.append({
                        "name": item.name,
                        "type": "file",
                        "size_bytes": sz,
                        "size_mb": round(sz / (1024 * 1024), 2)
                    })
                    if item.suffix.lower() == ".csv":
                        csv_files.append({"name": item.name, "size": sz})
            except OSError as e:
                entries.append({"name": item.name, "error": str(e)})
        return jsonify({
            "path": str(data_path),
            "exists": True,
            "entries": entries,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "file_count": file_count,
            "csv_files": csv_files[:50]
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Rate limiting for login endpoint (simple in-memory implementation)
login_attempts = {}
LOGIN_RATE_LIMIT = 5  # Max attempts per IP
LOGIN_RATE_WINDOW = 300  # 5 minutes in seconds

def check_rate_limit(ip_address: str) -> bool:
    """Check if IP has exceeded rate limit"""
    current_time = time.time()
    if ip_address not in login_attempts:
        login_attempts[ip_address] = []
    
    # Clean old attempts
    login_attempts[ip_address] = [
        attempt_time for attempt_time in login_attempts[ip_address]
        if current_time - attempt_time < LOGIN_RATE_WINDOW
    ]
    
    # Check if limit exceeded
    if len(login_attempts[ip_address]) >= LOGIN_RATE_LIMIT:
        return False
    
    # Record this attempt
    login_attempts[ip_address].append(current_time)
    return True


@app.route("/login", methods=["POST", "OPTIONS"])
def login():
    """Login endpoint - authenticate user and return JWT token"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", ALLOWED_ORIGIN if ALLOWED_ORIGIN != "*" else "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    try:
        # Rate limiting
        client_ip = request.remote_addr or "unknown"
        if not check_rate_limit(client_ip):
            return jsonify({
                "status": "error",
                "error": "Too many login attempts. Please try again later."
            }), 429
        
        data = request.get_json() or {}
        username = data.get("username", "").strip()
        password = data.get("password", "")
        
        if not username or not password:
            return jsonify({
                "status": "error",
                "error": "Username and password are required"
            }), 400
        
        # Verify credentials
        if not verify_password(username, password):
            return jsonify({
                "status": "error",
                "error": "Invalid username or password"
            }), 401
        
        # Generate token
        token = generate_token(username)
        
        response = jsonify({
            "status": "success",
            "token": token,
            "username": username
        })
        response.headers.add("Access-Control-Allow-Origin", ALLOWED_ORIGIN if ALLOWED_ORIGIN != "*" else "*")
        return response, 200
        
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", ALLOWED_ORIGIN if ALLOWED_ORIGIN != "*" else "*")
        return error_response, 500


@app.route("/run-pipeline", methods=["POST", "OPTIONS"])
@require_auth
def run_pipeline():
    """Start the pipeline and return run ID for status polling"""
    # Flask-CORS handles OPTIONS preflight requests automatically
    # But we need to ensure OPTIONS returns early to avoid hitting POST validation
    if request.method == "OPTIONS":
        # Flask-CORS will add the headers automatically
        return jsonify({}), 200
    
    # Ensure POST method (should never reach here for OPTIONS since handled above)
    if request.method != "POST":
        return jsonify({
            "status": "error",
            "error": f"Method {request.method} not allowed. Use POST.",
            "received_method": request.method,
            "allowed_methods": ["POST"]
        }), 405
    
    try:
        data = request.get_json() or {}
        state = data.get("state", "").lower()
        type_param = data.get("type", "church")
        
        if not state:
            return jsonify({
                "status": "error",
                "error": "State parameter is required"
            }), 400
        
        if type_param == "school":
            return jsonify({
                "status": "error",
                "error": "This is the Church scraper. Use type=church"
            }), 400
        
        # Check concurrency via Postgres
        # Clean up stale runs first
        unique_active_states = _unique_running_states_after_stale_cleanup()

        if db_state.is_state_running(state, SCRAPER_TYPE):
            return jsonify({
                "status": "error",
                "error": f"Another run for {state} is already in progress. Please wait for it to complete or stop it first.",
                "state": state
            }), 409

        if db_state.is_state_finalizing(state, SCRAPER_TYPE):
            return jsonify({
                "status": "error",
                "error": f"A run for {state} is currently finalizing. Please wait 2 minutes before starting a new run for this state.",
                "isFinalizing": True,
                "state": state
            }), 409

        # At capacity: enqueue (third+ state) or 409
        if state.lower() not in unique_active_states and len(unique_active_states) >= 2:
            dn = _run_display_name(state, SCRAPER_TYPE)
            q = db_state.queue_enqueue(state, SCRAPER_TYPE, dn)
            resp = jsonify({
                "status": "queued",
                "jobId": q["job_id"],
                "position": q["position"],
                "message": f"Queued: {len(unique_active_states)} states already running ({', '.join(sorted(unique_active_states))})",
                "activeStates": sorted(unique_active_states),
            })
            resp.headers.add("Access-Control-Allow-Origin", "*")
            return resp, 202
        
        # Generate unique run ID
        run_id = str(uuid.uuid4())
        
        # Start pipeline in background thread
        thread = threading.Thread(target=run_streaming_pipeline, args=(state, run_id, False, None))
        thread.daemon = True
        thread.start()
        
        # Return run ID immediately
        response = jsonify({
            "status": "started",
            "runId": run_id,
            "message": "Pipeline started"
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
        
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/queue", methods=["GET", "OPTIONS"])
@require_auth
def church_queue_list():
    """List SQLite queue jobs for this scraper (requires SQLITE_PATH)."""
    if request.method == "OPTIONS":
        r = jsonify({})
        r.headers.add("Access-Control-Allow-Origin", "*")
        r.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        r.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        return r, 200
    jobs = db_state.queue_list_jobs(SCRAPER_TYPE)
    resp = jsonify({"status": "ok", "jobs": jobs, "count": len(jobs)})
    resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp, 200


@app.route("/queue/<int:job_id>", methods=["DELETE", "OPTIONS"])
@require_auth
def church_queue_cancel(job_id: int):
    """Cancel a queued (not yet running) job."""
    if request.method == "OPTIONS":
        r = jsonify({})
        r.headers.add("Access-Control-Allow-Origin", "*")
        r.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        r.headers.add("Access-Control-Allow-Methods", "DELETE, OPTIONS")
        return r, 200
    ok = db_state.queue_cancel_job(job_id, SCRAPER_TYPE)
    if not ok:
        return jsonify({
            "status": "error",
            "error": "Job not found or not in queued status",
            "jobId": job_id,
        }), 404
    resp = jsonify({"status": "ok", "message": "Job cancelled", "jobId": job_id})
    resp.headers.add("Access-Control-Allow-Origin", "*")
    return resp, 200


# Removed /process-county endpoint - no longer needed with multiprocessing pool approach


@app.route("/pipeline-status/<run_id>", methods=["GET"])
@require_auth
def pipeline_status(run_id):
    """Get status of a running pipeline"""
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    row = db_state.get_run(run_id)
    if not row:
        # Track repeated 404 requests to prevent spam from stale browser tabs
        current_time = time.time()
        if run_id in not_found_runs:
            first_time, count = not_found_runs[run_id]
            if count >= 3 and (current_time - first_time) < 30:
                return jsonify({
                    "status": "error",
                    "error": "Run ID not found. This run no longer exists."
                }), 410
            not_found_runs[run_id] = (first_time, count + 1)
        else:
            not_found_runs[run_id] = (current_time, 1)

        to_remove = [rid for rid, (ft, _) in not_found_runs.items() if current_time - ft > 300]
        for rid in to_remove:
            not_found_runs.pop(rid, None)

        return jsonify({"status": "error", "error": "Run ID not found"}), 404

    run_data = db_state.row_to_pipeline_format(row)

    # If completed, return 410 after grace period
    if run_data.get("status") == "completed":
        completed_time = run_data.get("completedAt")
        if completed_time:
            time_since_completion = time.time() - completed_time
            if time_since_completion > 120:
                return jsonify({
                    "status": "completed",
                    "message": "Run completed. Status no longer available."
                }), 410

    # Calculate elapsed time
    start_time = run_data.get("startTime")
    if start_time:
        elapsed_seconds = time.time() - start_time
        run_data["elapsedTime"] = int(elapsed_seconds)
    else:
        run_data["elapsedTime"] = 0

    # Calculate estimated time remaining
    if run_data["status"] == "running":
        initial_estimate = run_data.get("initialEstimatedTimeRemaining")
        elapsed_seconds = run_data.get("elapsedTime", 0)

        if initial_estimate is not None and initial_estimate > 0:
            remaining_seconds = max(0, initial_estimate - elapsed_seconds)
            remaining_minutes = int((remaining_seconds + 59) // 60)
            run_data["estimatedTimeRemaining"] = remaining_minutes * 60
        else:
            counties_processed = run_data.get("countiesProcessed", 0)
            total_counties = run_data.get("totalCounties", 0)
            if total_counties > 0:
                remaining_counties = max(0, total_counties - counties_processed)
                effective_remaining = max(1, remaining_counties / MAX_WORKERS)
                estimated_remaining = effective_remaining * CHURCH_AVG_SECONDS_PER_COUNTY_SLOT
                remaining_minutes = int((estimated_remaining + 59) // 60)
                run_data["estimatedTimeRemaining"] = remaining_minutes * 60
            else:
                run_data["estimatedTimeRemaining"] = 0
    else:
        run_data["estimatedTimeRemaining"] = 0

    response = jsonify(run_data)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response, 200


# Error handler for 405 Method Not Allowed
@app.errorhandler(405)
def method_not_allowed(e):
    response = jsonify({
        "status": "error",
        "error": f"Method not allowed: {request.method}",
        "path": request.path,
        "received_method": request.method,
        "allowed_methods": ["POST", "OPTIONS"] if "/run-pipeline" in request.path else ["GET"]
    })
    response.headers.add("Access-Control-Allow-Origin", ALLOWED_ORIGIN if ALLOWED_ORIGIN != "*" else "*")
    if "/run-pipeline" in request.path:
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
    return response, 405

# Error handler for 404 Not Found
@app.route("/runs", methods=["GET", "OPTIONS"])
@require_auth
def list_runs():
    """List all runs from persistent storage"""
    # Flask-CORS handles OPTIONS preflight requests automatically
    if request.method == "OPTIONS":
        return jsonify({}), 200
    
    try:
        include_archived = request.args.get('include_archived', 'false').lower() == 'true'
        runs = list_all_runs(include_archived)

        response_data = {
            "status": "ok",
            "runs": runs,
            "count": len(runs)
        }
        
        # Flask-CORS will add CORS headers automatically
        response = jsonify(response_data)
        return response, 200
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        # Flask-CORS will add CORS headers automatically
        return error_response, 500


@app.route("/runs/<run_id>/stop", methods=["POST", "OPTIONS"])
@require_auth
def stop_run(run_id: str):
    """Stop a running pipeline"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    try:
        row = db_state.get_run(run_id)
        if not row:
            return jsonify({"status": "error", "error": "Run not found"}), 404

        current_status = row.get("status", "unknown")
        if current_status != "running":
            return jsonify({
                "status": "error",
                "error": f"Run is not running (current status: {current_status})"
            }), 400

        # Set cancellation flag for local thread
        if run_id in running_threads:
            running_threads[run_id]['cancelled'] = True

        # Cancel all pending/running county tasks in distributed dispatch
        db_state.cancel_dispatch(run_id)

        db_state.update_run(run_id,
            status="cancelled",
            status_message="Pipeline cancelled by user",
            cancelled_at=datetime.now().isoformat()
        )

        print(f"[{run_id}] Pipeline stop requested (dispatch cancelled)")
        
        response = jsonify({
            "status": "success",
            "message": "Pipeline stop requested"
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/runs/<run_id>/resume", methods=["POST", "OPTIONS"])
@require_auth
def resume_run(run_id: str):
    """Resume a run from checkpoint. Loads checkpoint and skips counties that have data files."""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    try:
        row = db_state.get_run(run_id)
        if not row:
            return jsonify({"status": "error", "error": "Run not found"}), 404

        state = row.get("state")
        if not state:
            return jsonify({"status": "error", "error": "State not found in run metadata"}), 400

        # Check if another run for the SAME STATE is currently running
        if db_state.is_state_running(state, SCRAPER_TYPE, exclude_run_id=run_id):
            return jsonify({
                "status": "error",
                "error": f"Another run for {state} is already running. Please stop it first before resuming.",
                "state": state
            }), 409

        # Check if THIS run is currently running
        if row.get("status") == "running":
            if run_id in running_threads:
                thread = running_threads[run_id].get('thread')
                if thread and thread.is_alive():
                    return jsonify({
                        "status": "error",
                        "error": "This run is already running. Please stop it first before resuming."
                    }), 409
        
        # Check if checkpoint exists
        checkpoint = load_checkpoint(run_id)
        if not checkpoint:
            return jsonify({
                "status": "error",
                "error": "No checkpoint found for this run. Cannot resume."
            }), 400
        
        # Check if already completed
        completed_counties = checkpoint.get('completed_counties', [])
        total_counties = checkpoint.get('total_counties', 0)
        if len(completed_counties) >= total_counties:
            return jsonify({
                "status": "error",
                "error": "Run is already complete. Cannot resume."
            }), 400
        
        # Start pipeline with resume flag
        thread = threading.Thread(target=run_streaming_pipeline, args=(state, run_id, True, None))
        thread.daemon = True
        
        # Track the thread for cancellation
        running_threads[run_id] = {'thread': thread, 'cancelled': False}
        
        thread.start()
        
        response = jsonify({
            "status": "resumed",
            "runId": run_id,
            "message": f"Run resumed from checkpoint: {len(completed_counties)}/{total_counties} counties already completed"
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
        
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/runs/<run_id>/aggregate", methods=["POST", "OPTIONS"])
@require_auth
def aggregate_run(run_id: str):
    """Manually trigger aggregation for a run, skipping wait for incomplete counties."""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    try:
        row = db_state.get_run(run_id)
        if not row:
            return jsonify({"status": "error", "error": "Run not found"}), 404

        state = row.get("state")
        if not state:
            return jsonify({"status": "error", "error": "State not found in run metadata"}), 400

        current_status = row.get("status")
        if current_status == "finalizing":
            return jsonify({"status": "error", "error": "Run is already finalizing/aggregating. Please wait."}), 409
        elif current_status == "completed":
            return jsonify({"status": "error", "error": "Run is already completed."}), 409

        run_dir = RUNS_DIR / run_id
        if not run_dir.exists():
            return jsonify({"status": "error", "error": "Run directory not found"}), 404

        db_state.update_run(run_id,
            status="finalizing",
            status_message="Manually triggering aggregation...",
        )

        def run_aggregation():
            try:
                aggregate_final_results(run_id, state, skip_wait=True)
            except Exception as e:
                print(f"[{run_id}] Error during manual aggregation: {e}")
                import traceback
                traceback.print_exc()
                db_state.update_run(run_id,
                    status="error",
                    error=f"Aggregation failed: {str(e)}",
                    status_message=f"Failed to aggregate: {str(e)}"
                )
        
        aggregation_thread = threading.Thread(target=run_aggregation, daemon=True)
        aggregation_thread.start()
        
        response = jsonify({
            "status": "aggregating",
            "runId": run_id,
            "state": state,
            "message": "Aggregation started. This will proceed with available county data, skipping incomplete counties."
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
        
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/runs/<run_id>/delete", methods=["DELETE", "OPTIONS"])
@require_auth
def delete_run(run_id: str):
    """Delete a run - marks as deleted in metadata instead of actually deleting files.
    If run is running, stops it first."""
    if request.method == "OPTIONS":
        response = jsonify({})
        # CORS preflight: allow auth header for DELETE requests
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "DELETE, OPTIONS")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        return response, 200
    
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    try:
        row = db_state.get_run_including_deleted(run_id)
        if not row:
            return jsonify({"status": "error", "error": "Run not found"}), 404

        status = row.get("status")
        if status == "running":
            if run_id in running_threads:
                running_threads[run_id]['cancelled'] = True
            db_state.update_run(run_id, status="cancelled", cancelled_at=datetime.now().isoformat())
            print(f"[{run_id}] Run stopped for deletion")

        # Delete ephemeral files
        run_dir = RUNS_DIR / run_id
        try:
            if run_dir.exists():
                shutil.rmtree(run_dir)
                print(f"[{run_id}] Deleted ephemeral run directory: {run_dir}")

            final_csv_path = row.get("final_csv_path")
            if final_csv_path and Path(final_csv_path).exists():
                try:
                    Path(final_csv_path).unlink()
                    print(f"[{run_id}] Deleted final CSV from volume: {final_csv_path}")
                except Exception:
                    pass
        except Exception as e:
            print(f"[{run_id}] Warning: Error deleting files: {e}")

        # Soft-delete in Postgres + delete checkpoint
        db_state.delete_run(run_id)
        db_state.delete_checkpoint(run_id)
        running_threads.pop(run_id, None)

        print(f"[{run_id}] Run deleted and files removed")
        
        response = jsonify({
            "status": "success",
            "message": "Run deleted successfully"
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/runs/<run_id>/archive", methods=["POST", "OPTIONS"])
@require_auth
def archive_run(run_id: str):
    """Archive a completed run"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    try:
        row = db_state.get_run(run_id)
        if not row:
            return jsonify({"status": "error", "error": "Run not found"}), 404

        if row.get("deleted"):
            return jsonify({"status": "error", "error": "Cannot archive a deleted run"}), 400

        db_state.update_run(run_id, archived=True)
        print(f"[{run_id}] Run archived")
        
        response = jsonify({
            "status": "success",
            "message": "Run archived successfully"
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/runs/<run_id>/unarchive", methods=["POST", "OPTIONS"])
@require_auth
def unarchive_run(run_id: str):
    """Unarchive a run"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    try:
        row = db_state.get_run_including_deleted(run_id)
        if not row:
            return jsonify({"status": "error", "error": "Run not found"}), 404

        if row.get("deleted"):
            return jsonify({"status": "error", "error": "Cannot unarchive a deleted run"}), 400

        db_state.update_run(run_id, archived=False)
        print(f"[{run_id}] Run unarchived")
        
        response = jsonify({
            "status": "success",
            "message": "Run unarchived successfully"
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/runs/<run_id>/download", methods=["GET"])
@require_auth
def download_run_csv(run_id: str):
    """Download the final CSV for a completed run"""
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return jsonify({
            "status": "error",
            "error": "Invalid run ID format"
        }), 400
    
    try:
        csv_path = None
        download_name = f"run_{run_id}.csv"
        row = db_state.get_run_including_deleted(run_id)
        if row:
            csv_path = row.get("final_csv_path")
            if csv_path and os.path.exists(csv_path):
                download_name = row.get("csv_filename") or download_name
            else:
                csv_path = None
        # Fallback: search volume for CSV containing run_id (metadata may be lost on container restart)
        if not csv_path or not os.path.exists(csv_path):
            for pattern in (f"*_{run_id}_*.csv", f"*{run_id}*.csv"):
                for f in VOLUME_DIR.glob(pattern):
                    if run_id in f.stem:
                        csv_path = str(f)
                        download_name = f.name
                        break
                if csv_path:
                    break
        if not csv_path or not os.path.exists(csv_path):
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        from flask import send_file
        return send_file(
            csv_path,
            mimetype='text/csv',
            as_attachment=True,
            download_name=download_name
        )
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.errorhandler(404)
def not_found(e):
    return jsonify({
        "status": "error",
        "error": f"Endpoint not found: {request.path}",
        "available_endpoints": ["/", "/health", "/notify/test-email", "/run-pipeline", "/queue", "/queue/<job_id>", "/pipeline-status/<run_id>", "/runs", "/runs/<run_id>/download", "/runs/<run_id>/stop", "/runs/<run_id>/delete", "/runs/<run_id>/resume", "/runs/<run_id>/archive", "/runs/<run_id>/unarchive"]
    }), 404

# Initialize Postgres tables and start queue worker
print("[STARTUP] Initializing Postgres state tables...")
db_state.init_tables()

print("[STARTUP] Running cleanup of old runs...")
cleanup_old_runs()

# Start queue worker thread (uses advisory lock for leader election across replicas)
_church_q_thread = threading.Thread(target=_church_queue_worker_loop, daemon=True)
_church_q_thread.start()
print("[STARTUP] Queue worker thread started")

# Start distributed county worker thread (runs on EVERY replica)
_county_worker_thread = threading.Thread(target=_distributed_county_worker_loop, daemon=True)
_county_worker_thread.start()
print(f"[STARTUP] County worker thread started (replica: {db_state.REPLICA_ID})")

# Production: Use Waitress server
# NOTE: If Railway runs this file directly (bypassing Dockerfile ENTRYPOINT),
# we need to exec into dumb-init ourselves to ensure proper process reaping.
if __name__ == "__main__":
    import subprocess
    import sys
    import os
    
    # Check if dumb-init is PID 1 (it should be if Dockerfile ENTRYPOINT is respected)
    needs_dumb_init = False
    if HAS_PSUTIL:
        try:
            pid1 = psutil.Process(1)
            pid1_name = pid1.name().lower()
            if 'dumb-init' not in pid1_name and 'init' not in pid1_name:
                needs_dumb_init = True
        except:
            pass  # Can't verify, continue anyway
    
    # If dumb-init is not PID 1, exec into it with waitress-serve as the command
    # This ensures dumb-init becomes PID 1 and properly reaps zombie processes
    # Do this silently - no warning messages
    if needs_dumb_init:
        # Find dumb-init
        dumb_init_path = None
        for path in ['/usr/bin/dumb-init', '/usr/local/bin/dumb-init', '/bin/dumb-init']:
            if os.path.exists(path) and os.access(path, os.X_OK):
                dumb_init_path = path
                break
        
        if dumb_init_path:
            port = os.environ.get("PORT", "8080")
            # Exec into dumb-init silently - no log messages
            # This replaces the current Python process with dumb-init, which then runs waitress
            os.execv(dumb_init_path, [
                dumb_init_path, '--',
                'waitress-serve',
                '--host=0.0.0.0',
                f'--port={port}',
                '--threads=4',
                '--channel-timeout=300',
                'external_services.api:app'
            ])
        # If dumb-init not found, fall through to regular startup (but this shouldn't happen)
    
    # If we reach here, either dumb-init is already PID 1, or we couldn't find it
    port = os.environ.get("PORT", "8080")
    # Start Waitress silently - no startup messages
    subprocess.run([
        sys.executable, "-m", "waitress",
        "--host=0.0.0.0",
        f"--port={port}",
        "--threads=4",
        "--channel-timeout=300",
        "external_services.api:app"
    ])
