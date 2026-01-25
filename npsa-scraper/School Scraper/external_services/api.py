"""
Backend API for School Scraper Pipeline
Deploy this to Railway to handle POST requests from Vercel frontend
Processes states county-by-county to avoid timeout issues
Last updated: 2025-12-13 - Force Railway redeploy
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

# In-memory storage for pipeline runs (use Redis or database in production)
pipeline_runs = {}

# Track running threads and cancellation flags
# Format: {run_id: {'thread': Thread, 'cancelled': bool}}
running_threads = {}

# Track 404 requests for non-existent run IDs to prevent spam from stale browser tabs
# Format: {run_id: (first_404_time, count)}
not_found_runs = {}

# Container restart logic removed - container stays running after completion (matches backup branch)

# Persistent storage configuration
# Use /data for Railway volume, fallback to /tmp for local development
PERSISTENT_DATA_DIR = Path(os.getenv("PERSISTENT_DATA_DIR", "/data"))
PERSISTENT_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Subdirectories for persistent storage
RUNS_DIR = PERSISTENT_DATA_DIR / "runs"
CHECKPOINTS_DIR = PERSISTENT_DATA_DIR / "checkpoints"
METADATA_DIR = PERSISTENT_DATA_DIR / "metadata"

# Create directories if they don't exist
RUNS_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# Batch size for checkpointing (save checkpoint every N counties)
# Set to 1 to save after every county for better recovery
CHECKPOINT_BATCH_SIZE = int(os.getenv("CHECKPOINT_BATCH_SIZE", "1"))

# Number of parallel workers for processing counties
# Default to 1 for sequential processing
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))

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

def validate_run_id(run_id: str) -> bool:
    """
    Validate that run_id is a valid UUID format.
    This prevents path traversal attacks (e.g., '../../../etc/passwd').
    """
    if not run_id or not isinstance(run_id, str):
        return False
    return bool(UUID_PATTERN.match(run_id.strip()))


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
    """Save checkpoint to persistent storage"""
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        raise ValueError(f"Invalid run_id format: {run_id}")
    checkpoint_path = CHECKPOINTS_DIR / f"{run_id}.json"
    checkpoint_data = {
        "run_id": run_id,
        "state": state,
        "completed_counties": completed_counties,
        "next_county_index": next_county_index,
        "total_counties": total_counties,
        "timestamp": time.time(),
        "updated_at": datetime.now().isoformat()
    }
    try:
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        print(f"[{run_id}] Checkpoint saved: {len(completed_counties)}/{total_counties} counties completed")
        return True
    except Exception as e:
        print(f"[{run_id}] Error saving checkpoint: {e}")
        return False


def load_checkpoint(run_id: str) -> dict:
    """Load checkpoint from persistent storage"""
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return None
    checkpoint_path = CHECKPOINTS_DIR / f"{run_id}.json"
    if not checkpoint_path.exists():
        return None
    try:
        with open(checkpoint_path, 'r') as f:
            checkpoint_data = json.load(f)
        print(f"[{run_id}] Checkpoint loaded: {len(checkpoint_data.get('completed_counties', []))}/{checkpoint_data.get('total_counties', 0)} counties completed")
        return checkpoint_data
    except Exception as e:
        print(f"[{run_id}] Error loading checkpoint: {e}")
        return None


def save_run_metadata(run_id: str, metadata: dict):
    """Save run metadata to persistent storage"""
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        raise ValueError(f"Invalid run_id format: {run_id}")
    metadata_path = METADATA_DIR / f"{run_id}.json"
    try:
        # Add timestamp if not present
        if "created_at" not in metadata:
            metadata["created_at"] = datetime.now().isoformat()
        metadata["updated_at"] = datetime.now().isoformat()
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        return True
    except Exception as e:
        print(f"[{run_id}] Error saving metadata: {e}")
        return False


def load_run_metadata(run_id: str) -> dict:
    """Load run metadata from persistent storage"""
    # Security: Validate run_id to prevent path traversal
    if not validate_run_id(run_id):
        return None
    metadata_path = METADATA_DIR / f"{run_id}.json"
    if not metadata_path.exists():
        return None
    try:
        with open(metadata_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"[{run_id}] Error loading metadata: {e}")
        return None


def list_all_runs() -> list:
    """List all runs from persistent storage, excluding deleted runs"""
    runs = []
    try:
        # Get all metadata files
        for metadata_file in METADATA_DIR.glob("*.json"):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    # Skip deleted runs (filter them out from view)
                    if metadata.get("deleted", False):
                        continue
                    
                    metadata["run_id"] = metadata_file.stem  # Add run_id from filename
                    # Ensure archived field exists (default to False for backwards compatibility)
                    if "archived" not in metadata:
                        metadata["archived"] = False
                    runs.append(metadata)
            except Exception as e:
                print(f"Error reading metadata file {metadata_file}: {e}")
                continue
        
        # Sort by created_at (newest first)
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
        
        # Initialize pipeline for this county
        pipeline = StreamingPipeline(
            google_api_key=os.getenv("GOOGLE_PLACES_API_KEY", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            global_max_api_calls=None,  # No limit for full state runs
            max_pages_per_school=2,  # Reduced from 3 to 2 for faster processing
            state=state
        )
        
        # Run pipeline for this single county - collects all contacts
        pipeline.run(
            counties=[county],  # Process only this county
            batch_size=0,  # Process all schools in county
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
            'schools': pipeline.stats.get('schools_processed', 0),
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
        
        # Brief delay after cleanup to provide buffer
        time.sleep(1.0)


def process_single_county(state: str, county: str, run_id: str, county_index: int, total_counties: int):
    """
    Process a single county in a subprocess.
    The subprocess will die naturally after completion, taking all Chrome processes with it.
    
    Returns:
        dict with results: {'schools': count, 'contacts': count, 'contacts_no_emails': count, 'success': bool, 'csv_path': str}
    """
    # Create result file for subprocess to write to
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    result_file = str(run_dir / f"{county.replace(' ', '_')}_result.json")
    
    # Update progress
    pipeline_runs[run_id]["statusMessage"] = f"Processing {county} County ({county_index + 1}/{total_counties})..."
    pipeline_runs[run_id]["currentCounty"] = county
    pipeline_runs[run_id]["currentCountyIndex"] = county_index + 1
    pipeline_runs[run_id]["currentStep"] = 1
    
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


def aggregate_final_results(run_id: str, state: str):
    """Aggregate all county results, run global Steps 11-13, and generate final CSV"""
    try:
        counties = load_counties_from_state(state)
        run_dir = RUNS_DIR / run_id
        
        pipeline_runs[run_id]["statusMessage"] = "Waiting for all counties to complete..."
        
        # Wait for all counties to complete (check that all CSV files exist)
        # With multiprocessing pool, counties should complete more reliably, but add timeout as safety
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
        
        pipeline_runs[run_id]["statusMessage"] = "Aggregating results from all counties..."
        
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
                                school_name=sval(row.get('school_name', '') or row.get('School Name', '')),
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
            # Set to finalizing for 2-minute cooldown
            pipeline_runs[run_id]["status"] = "finalizing"
            pipeline_runs[run_id]["statusMessage"] = "Pipeline completed but no contacts found. Finalizing..."
            pipeline_runs[run_id]["finalizingAt"] = time.time()
            
            # Create restart marker for start.sh to detect restart
            try:
                with open("/tmp/run_completed_marker", "w") as f:
                    f.write(str(time.time()))
            except:
                pass  # Ignore if can't write marker
            
            # Start background thread to transition to completed after 2 minutes
            def finalize_completion():
                time.sleep(120)  # 2-minute cooldown
                if run_id in pipeline_runs and pipeline_runs[run_id].get("status") == "finalizing":
                    pipeline_runs[run_id]["status"] = "completed"
                    pipeline_runs[run_id]["statusMessage"] = "Pipeline completed but no contacts found."
                    pipeline_runs[run_id]["completedAt"] = time.time()
            
            finalize_thread = threading.Thread(target=finalize_completion, daemon=True)
            finalize_thread.start()
            pipeline_runs[run_id]["totalContacts"] = 0
            pipeline_runs[run_id]["completedAt"] = time.time()
            return
        
        print(f"[{run_id}] Total contacts collected: {len(all_contacts)}")
        
        # STEP 11: Split contacts into with/without emails
        pipeline_runs[run_id]["statusMessage"] = "Step 11: Splitting contacts..."
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
            pipeline_runs[run_id]["statusMessage"] = f"Step 12: Enriching {len(contacts_without_emails_deduped)} contacts with Hunter.io..."
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
        pipeline_runs[run_id]["statusMessage"] = "Step 13: Compiling final CSV..."
        final_output_csv = str(run_dir / f"{state.title()}_leads_final.csv")
        
        final_csv_path = compiler.compile_contacts_to_csv(
            contacts_with_emails=contacts_with_emails_deduped,
            contacts_enriched=contacts_enriched,
            output_csv=final_output_csv,
            state=state,
            already_deduplicated=True
        )
        
        print(f"[{run_id}] Step 13 complete: Final CSV saved to {final_csv_path}")
        
        # Read final CSV for download
        if os.path.exists(final_csv_path):
            with open(final_csv_path, 'r') as f:
                csv_content = f.read()
            
            pipeline_runs[run_id]["csvData"] = csv_content
            pipeline_runs[run_id]["csvFilename"] = f"{state.title()}_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
            # Count final contacts
            final_df = pd.read_csv(final_csv_path)
            email_col = 'email' if 'email' in final_df.columns else 'Email'
            if email_col in final_df.columns:
                contacts_with_emails_final = final_df[final_df[email_col].notna() & (final_df[email_col] != '') & (final_df[email_col].str.strip() != '')]
                contacts_without_emails_final = final_df[final_df[email_col].isna() | (final_df[email_col] == '') | (final_df[email_col].str.strip() == '')]
            else:
                contacts_with_emails_final = pd.DataFrame()
                contacts_without_emails_final = final_df
            
            pipeline_runs[run_id]["totalContacts"] = len(final_df)
            pipeline_runs[run_id]["totalContactsWithEmails"] = len(contacts_with_emails_final)
            pipeline_runs[run_id]["totalContactsWithoutEmails"] = len(contacts_without_emails_final)
            
            # Save final CSV path to metadata
            metadata = load_run_metadata(run_id) or {}
            metadata.update({
                "final_csv_path": final_csv_path,
                "csv_filename": pipeline_runs[run_id]["csvFilename"],
                "total_contacts": len(final_df),
                "total_contacts_with_emails": len(contacts_with_emails_final),
                "total_contacts_without_emails": len(contacts_without_emails_final)
            })
            save_run_metadata(run_id, metadata)
        else:
            print(f"[{run_id}] ERROR: Final CSV not created at {final_csv_path}")
            pipeline_runs[run_id]["totalContacts"] = len(deduplicated)
            pipeline_runs[run_id]["totalContactsWithEmails"] = len(contacts_with_emails_deduped) + len(contacts_enriched)
            pipeline_runs[run_id]["totalContactsWithoutEmails"] = len(contacts_without_emails_deduped) - len(contacts_enriched)
        
        # Update final stats - set to finalizing for 2-minute cooldown
        pipeline_runs[run_id]["status"] = "finalizing"
        pipeline_runs[run_id]["statusMessage"] = f"Pipeline completed! Processed {len(counties)} counties. Enriched {len(contacts_enriched)} contacts. Finalizing..."
        pipeline_runs[run_id]["currentStep"] = 13
        pipeline_runs[run_id]["progress"] = 100
        pipeline_runs[run_id]["countiesProcessed"] = len(counties)
        pipeline_runs[run_id]["finalizingAt"] = time.time()  # Track when finalizing started
        
        # Create restart marker for start.sh to detect restart
        try:
            with open("/tmp/run_completed_marker", "w") as f:
                f.write(str(time.time()))
        except:
            pass  # Ignore if can't write marker
        
        # Start background thread to transition to completed after 2 minutes
        def finalize_completion():
            time.sleep(120)  # 2-minute cooldown
            if run_id in pipeline_runs and pipeline_runs[run_id].get("status") == "finalizing":
                pipeline_runs[run_id]["status"] = "completed"
                pipeline_runs[run_id]["statusMessage"] = f"Pipeline completed! Processed {len(counties)} counties. Enriched {len(contacts_enriched)} contacts."
                pipeline_runs[run_id]["completedAt"] = time.time()
        
        finalize_thread = threading.Thread(target=finalize_completion, daemon=True)
        finalize_thread.start()
        
    except Exception as e:
        pipeline_runs[run_id]["status"] = "error"
        pipeline_runs[run_id]["error"] = str(e)
        pipeline_runs[run_id]["statusMessage"] = f"Failed to aggregate results: {str(e)}"
        import traceback
        traceback.print_exc()


def run_streaming_pipeline(state: str, run_id: str, resume_from_checkpoint: bool = False):
    """
    Initialize the pipeline and process all counties (sequentially or in parallel).
    
    Processing mode is controlled by MAX_WORKERS environment variable:
    - MAX_WORKERS=1: Sequential processing (one county at a time)
    - MAX_WORKERS=2: Parallel processing with 2 workers (50% time reduction)
    - MAX_WORKERS=4: Parallel processing with 4 workers (75% time reduction)
    
    Both sequential and parallel modes use multiprocessing.Pool with maxtasksperchild=1:
    - Each worker process terminates after one county, automatically cleaning up Chrome processes
    - When worker dies, OS automatically reaps all child processes (Chrome/ChromeDriver)
    - Checkpoint coordination (locks prevent race conditions)
    - Progress tracking (locks ensure accurate updates)
    - Out-of-order county completion handling (for parallel mode)
    
    This unified approach prevents Chrome process accumulation by ensuring workers
    die after each task, eliminating zombie processes in both sequential and parallel modes.
    """
    # Initialize progress tracking FIRST, before any operations that might fail
    pipeline_runs[run_id] = {
        "status": "running",
        "progress": 0,
        "currentStep": 1,
        "totalSteps": 7,
        "statusMessage": f"Starting pipeline for {state}...",
        "steps": [],
        "totalContacts": 0,
        "totalContactsNoEmails": 0,
        "schoolsFound": 0,
        "schoolsProcessed": 0,
        "csvData": None,
        "csvFilename": None,
        "csvNoEmailsData": None,
        "csvNoEmailsFilename": None,
        "error": None,
        "countiesProcessed": 0,
        "totalCounties": 0,
        "currentCounty": None,
        "currentCountyIndex": 0,
        "countyTimes": [],  # Track processing times for each county
        "countyContacts": [],  # Track contacts per county for graphs
        "countySchools": [],  # Track schools per county for graphs
        "startTime": time.time(),  # Track overall start time
    }
    
    def process_all_counties():
        """
        Process all counties (sequentially or in parallel) with checkpointing.
        
        Processing mode depends on MAX_WORKERS:
        - Sequential (MAX_WORKERS=1): One county at a time, simple loop
        - Parallel (MAX_WORKERS>1): Multiple counties simultaneously with multiprocessing.Pool
        
        If resume_from_checkpoint is True, loads checkpoint and skips counties that have data files.
        Thread locks ensure safe concurrent access to shared state.
        """
        try:
            # Load counties for state
            counties = load_counties_from_state(state)
            total_counties = len(counties)
            
            # Check if resuming from checkpoint
            completed_counties = []
            start_index = 0
            
            if resume_from_checkpoint:
                # Load checkpoint
                checkpoint = load_checkpoint(run_id)
                if checkpoint:
                    completed_counties = checkpoint.get('completed_counties', [])
                    start_index = checkpoint.get('next_county_index', 0)
                    print(f"[{run_id}] Resuming from checkpoint: {len(completed_counties)}/{total_counties} counties already completed")
                
                # Also check data files to catch counties that completed but didn't checkpoint
                # (e.g., Prince George's that completed but hung during cleanup)
                run_dir = RUNS_DIR / run_id
                if run_dir.exists():
                    for county in counties:
                        county_dir = run_dir / county.replace(' ', '_')
                        county_csv = county_dir / "final_contacts.csv"
                        if county_csv.exists() and county not in completed_counties:
                            print(f"[{run_id}] Found completed county (data file exists): {county}")
                            completed_counties.append(county)
                    
                    # Recalculate start_index based on actual completed counties
                    start_index = 0
                    for i, c in enumerate(counties):
                        if c in completed_counties:
                            start_index = i + 1
                        else:
                            break
                    
                    if completed_counties:
                        print(f"[{run_id}] Resuming: {len(completed_counties)}/{total_counties} counties completed, starting from index {start_index}")
            
            # Save initial metadata for new run or resume
            initial_metadata = {
                "run_id": run_id,
                "state": state,
                "status": "running",
                "total_counties": total_counties,
                "completed_counties": completed_counties,
                "start_time": time.time(),
                "created_at": datetime.now().isoformat()
            }
            save_run_metadata(run_id, initial_metadata)
            
            if resume_from_checkpoint and completed_counties:
                print(f"[{run_id}] Resuming run: {len(completed_counties)}/{total_counties} counties already completed")
            else:
                print(f"[{run_id}] New run started: {total_counties} counties to process")
            
            # Update progress tracking with county info
            pipeline_runs[run_id]["totalCounties"] = total_counties
            pipeline_runs[run_id]["statusMessage"] = f"Processing {state} ({len(completed_counties)}/{total_counties} counties completed)..."
            pipeline_runs[run_id]["currentCounty"] = "Starting..." if not completed_counties else f"Resuming from {len(completed_counties)}/{total_counties}"
            pipeline_runs[run_id]["countiesProcessed"] = len(completed_counties)
            
            # Determine processing mode message
            remaining_count = total_counties - len(completed_counties)
            if MAX_WORKERS == 1:
                print(f"[{run_id}] Processing {remaining_count} remaining counties sequentially...")
            else:
                print(f"[{run_id}] Processing {remaining_count} remaining counties with {MAX_WORKERS} parallel workers...")
            
            # Process remaining counties (skip completed ones if resuming)
            remaining_counties = [(idx, county) for idx, county in enumerate(counties) if county not in completed_counties]
            
            if not remaining_counties:
                print(f"[{run_id}] All counties already completed!")
                # Update progress to show all complete
                pipeline_runs[run_id]["progress"] = 100
                pipeline_runs[run_id]["countiesProcessed"] = total_counties
                pipeline_runs[run_id]["statusMessage"] = f"All {total_counties} counties already completed"
                # Continue to aggregation - skip the pool processing
            else:
                # UNIFIED PROCESSING: Use Pool for both sequential (MAX_WORKERS=1) and parallel (MAX_WORKERS>1)
                # maxtasksperchild=1 forces each worker to terminate after one county,
                # which automatically cleans up all Chrome child processes when the worker dies
                with Pool(processes=MAX_WORKERS, maxtasksperchild=1) as pool:
                    # Submit all remaining counties to the pool
                    pool_args = [(idx, county, state, run_id, total_counties) for idx, county in remaining_counties]
                    
                    # Use imap_unordered to get results as they complete (out-of-order is handled)
                    # Track which counties we've submitted to handle failures gracefully
                    failed_counties = []
                    try:
                        for idx, county, result, processing_time in pool.imap_unordered(process_county_worker_multiprocessing, pool_args):
                            # Check for cancellation
                            if running_threads.get(run_id, {}).get('cancelled', False):
                                pool.terminate()  # Force kill all workers
                                pool.join()
                                pipeline_runs[run_id]["status"] = "cancelled"
                                pipeline_runs[run_id]["statusMessage"] = "Pipeline cancelled by user"
                                print(f"[{run_id}] Pipeline cancelled during processing")
                                return
                            
                            # Handle worker failures gracefully - continue processing other counties
                            if not result.get('success', False):
                                error_msg = result.get('error', 'Unknown error')
                                print(f"[{run_id}] WARNING: {county} County failed: {error_msg}")
                                failed_counties.append(county)
                                # Still mark as completed to avoid infinite retry, but log the failure
                                with checkpoint_lock:
                                    if county not in completed_counties:
                                        completed_counties.append(county)
                                continue
                            
                            # Update progress in main process (thread-safe)
                            with checkpoint_lock:
                                if county not in completed_counties:
                                    completed_counties.append(county)
                            
                            completed = len(completed_counties)
                            progress_pct = int((completed / total_counties) * 100)
                            
                            # Update pipeline_runs state
                            with progress_lock:
                                pipeline_runs[run_id]["progress"] = progress_pct
                                pipeline_runs[run_id]["statusMessage"] = f"Processing {completed}/{total_counties} counties..."
                                pipeline_runs[run_id]["countiesProcessed"] = completed
                                # Set currentCounty to show progress (since we're processing in parallel, show the count)
                                # This replaces "Initializing..." with actual progress
                                if completed < total_counties:
                                    pipeline_runs[run_id]["currentCounty"] = f"Processing {completed}/{total_counties} counties"
                                else:
                                    pipeline_runs[run_id]["currentCounty"] = f"All {total_counties} counties completed"
                                pipeline_runs[run_id]["schoolsFound"] = pipeline_runs[run_id].get("schoolsFound", 0) + result.get('schools', 0)
                                pipeline_runs[run_id]["schoolsProcessed"] = pipeline_runs[run_id].get("schoolsProcessed", 0) + result.get('schools', 0)
                                
                                # Track county timing for average calculation
                                if "countyTimes" not in pipeline_runs[run_id]:
                                    pipeline_runs[run_id]["countyTimes"] = []
                                pipeline_runs[run_id]["countyTimes"].append(processing_time)
                                
                                # Track per-county contacts and schools for graphs
                                if "countyContacts" not in pipeline_runs[run_id]:
                                    pipeline_runs[run_id]["countyContacts"] = []
                                if "countySchools" not in pipeline_runs[run_id]:
                                    pipeline_runs[run_id]["countySchools"] = []
                                
                                pipeline_runs[run_id]["countyContacts"].append(result.get('contacts', 0))
                                pipeline_runs[run_id]["countySchools"].append(result.get('schools', 0))
                            
                            print(f"[{run_id}] Completed {county} County in {processing_time:.1f} seconds")
                            print(f"[{run_id}] Progress: {completed}/{total_counties} counties completed")
                            
                            # Save checkpoint after every county (CHECKPOINT_BATCH_SIZE=1) or at completion
                            # This is for progress tracking only - runs always start fresh, no resume logic
                            is_checkpoint = completed % CHECKPOINT_BATCH_SIZE == 0 or completed == total_counties
                            if is_checkpoint:
                                # Calculate next county index: find the highest index of completed counties + 1
                                # This handles out-of-order completion in parallel mode
                                next_index = 0
                                for i, c in enumerate(counties):
                                    if c in completed_counties:
                                        next_index = i + 1
                                    else:
                                        # Found first incomplete county
                                        break
                                
                                save_checkpoint(run_id, state, completed_counties, next_index, total_counties)
                                
                                # Update metadata
                                metadata = load_run_metadata(run_id) or {}
                                metadata.update({
                                    "status": "running",
                                    "completed_counties": completed_counties,
                                    "progress": progress_pct,
                                    "last_checkpoint": time.time()
                                })
                                save_run_metadata(run_id, metadata)
                                
                                print(f"[{run_id}] Checkpoint saved after {completed} counties")
                            
                            # Health check after each county (lists remaining processes)
                            check_health()
                            
                            # Resource monitoring
                            log_resource_usage()
                            
                            # EXPLICIT GARBAGE COLLECTION: Force cleanup after each county
                            gc.collect()
                            
                            # 2-second delay between counties to provide buffer for cleanup
                            if completed < total_counties:
                                time.sleep(2.0)
                    
                    except KeyboardInterrupt:
                        print(f"[{run_id}] Interrupted by user, cleaning up...")
                        pool.terminate()
                        pool.join()
                        # Save checkpoint before exiting
                        save_checkpoint(run_id, state, completed_counties, start_index + len(completed_counties), total_counties)
                        pipeline_runs[run_id]["status"] = "cancelled"
                        pipeline_runs[run_id]["statusMessage"] = "Pipeline cancelled by user"
                        return
                    except Exception as e:
                        print(f"[{run_id}] Error in pool processing: {e}")
                        import traceback
                        traceback.print_exc()
                        # Save checkpoint before terminating
                        save_checkpoint(run_id, state, completed_counties, start_index + len(completed_counties), total_counties)
                        pool.terminate()
                        pool.join()
                        # Don't crash - try to continue to aggregation if we have some results
                        if len(completed_counties) > 0:
                            print(f"[{run_id}] Continuing to aggregation with {len(completed_counties)} completed counties despite error")
                        else:
                            # No progress made, mark as error
                            if run_id in pipeline_runs:
                                pipeline_runs[run_id]["status"] = "error"
                                pipeline_runs[run_id]["error"] = f"Pool processing failed: {str(e)}"
                                pipeline_runs[run_id]["statusMessage"] = f"Pipeline failed: {str(e)}"
                    
                    # Log any failed counties after all processing completes
                    if failed_counties:
                        print(f"[{run_id}] WARNING: {len(failed_counties)} counties failed: {', '.join(failed_counties)}")
                        if run_id in pipeline_runs:
                            pipeline_runs[run_id]["statusMessage"] = f"Completed with {len(failed_counties)} failures"
                            return
            
            # All counties completed, aggregate results
            print(f"[{run_id}] All counties completed ({len(completed_counties)}/{total_counties}), starting aggregation...")
            try:
                aggregate_final_results(run_id, state)
            except Exception as e:
                print(f"[{run_id}] Error during aggregation: {e}")
                import traceback
                traceback.print_exc()
                # Still mark as completed if we have results
                if run_id in pipeline_runs:
                    pipeline_runs[run_id]["status"] = "error"
                    pipeline_runs[run_id]["error"] = f"Aggregation failed: {str(e)}"
            
            # Final checkpoint - mark as completed (always save, even if aggregation failed)
            save_checkpoint(run_id, state, completed_counties, len(counties), total_counties)
            final_metadata = load_run_metadata(run_id) or {}
            final_metadata.update({
                "status": "completed" if run_id in pipeline_runs and pipeline_runs[run_id].get("status") != "error" else "error",
                "completed_counties": completed_counties,
                "progress": 100,
                "completed_at": datetime.now().isoformat(),
                "completion_time": time.time()
            })
            save_run_metadata(run_id, final_metadata)
            
            # Final status update - set to finalizing for 2-minute cooldown
            if run_id in pipeline_runs:
                if pipeline_runs[run_id].get("status") != "error":
                    pipeline_runs[run_id]["status"] = "finalizing"
                    pipeline_runs[run_id]["statusMessage"] = f"Pipeline completed: {len(completed_counties)}/{total_counties} counties processed. Finalizing..."
                    pipeline_runs[run_id]["finalizingAt"] = time.time()
                    pipeline_runs[run_id]["containerResetRequested"] = True
                    
                    # Create restart marker for start.sh to detect restart
                    try:
                        with open("/tmp/run_completed_marker", "w") as f:
                            f.write(str(time.time()))
                    except:
                        pass  # Ignore if can't write marker
                    
                    # Start background thread to transition to completed after 2 minutes
                    def finalize_completion():
                        time.sleep(120)  # 2-minute cooldown
                        if run_id in pipeline_runs and pipeline_runs[run_id].get("status") == "finalizing":
                            pipeline_runs[run_id]["status"] = "completed"
                            pipeline_runs[run_id]["statusMessage"] = f"Pipeline completed: {len(completed_counties)}/{total_counties} counties processed"
                            pipeline_runs[run_id]["completedAt"] = time.time()
                    
                    finalize_thread = threading.Thread(target=finalize_completion, daemon=True)
                    finalize_thread.start()
                    
                    # Pipeline completed successfully - no container reset needed
                    # The backup branch doesn't force container restarts, allowing the container to stay running
                    # This prevents Railway from logging crashes and allows the container to handle multiple runs
                    print(f"[{run_id}] Pipeline run complete: {len(completed_counties)}/{total_counties} counties processed")
                    print(f"[{run_id}] Container will remain running and ready for next run")
                    pipeline_runs[run_id]["progress"] = 100
            else:
                print(f"[{run_id}] Pipeline run complete: {len(completed_counties)}/{total_counties} counties processed")
        
        except FileNotFoundError as e:
            # State file not found
            error_msg = f"State file not found. Please ensure assets/data/state_counties/{state.lower().replace(' ', '_')}.txt exists in the repository."
            # Only update pipeline_runs if run_id still exists (may have been cleaned up)
            if run_id in pipeline_runs:
                pipeline_runs[run_id]["status"] = "error"
                pipeline_runs[run_id]["error"] = error_msg
                pipeline_runs[run_id]["statusMessage"] = f"Pipeline failed: {error_msg}"
            import traceback
            traceback.print_exc()
        except Exception as e:
            # Any other error - save checkpoint before exiting
            error_msg = str(e)[:500]  # Limit error message length
            print(f"[{run_id}] Fatal error in pipeline: {error_msg}")
            import traceback
            traceback.print_exc()
            
            # Try to save checkpoint with current progress
            try:
                checkpoint = load_checkpoint(run_id)
                if checkpoint:
                    completed_counties = checkpoint.get("completed_counties", [])
                    total_counties = checkpoint.get("total_counties", 0)
                    if len(completed_counties) > 0:
                        save_checkpoint(run_id, state, completed_counties, len(completed_counties), total_counties)
                        print(f"[{run_id}] Checkpoint saved after fatal error: {len(completed_counties)}/{total_counties} counties")
            except Exception as checkpoint_error:
                print(f"[{run_id}] Failed to save checkpoint after error: {checkpoint_error}")
            
            # Only update pipeline_runs if run_id still exists (may have been cleaned up)
            if run_id in pipeline_runs:
                pipeline_runs[run_id]["status"] = "error"
                pipeline_runs[run_id]["error"] = error_msg
                pipeline_runs[run_id]["statusMessage"] = f"Pipeline failed: {error_msg}"
    
    # Wrapper to ensure thread always completes and updates status
    def process_all_counties_with_error_handling():
        try:
            process_all_counties()
        except Exception as e:
            print(f"[{run_id}] Unhandled exception in process_all_counties: {e}")
            import traceback
            traceback.print_exc()
            # Ensure status is updated even on unhandled exceptions
            if run_id in pipeline_runs:
                pipeline_runs[run_id]["status"] = "error"
                pipeline_runs[run_id]["error"] = f"Unhandled exception: {str(e)}"
                pipeline_runs[run_id]["statusMessage"] = f"Pipeline crashed: {str(e)}"
        finally:
            # Always clean up thread tracking
            if run_id in running_threads:
                # Don't remove, just mark as not running
                running_threads[run_id]['cancelled'] = True
            print(f"[{run_id}] Pipeline thread completed")
    
    # Start processing in a background thread
    # Note: daemon=True means thread dies if main process exits, but we save checkpoints
    # so progress is preserved even if container restarts
    thread = threading.Thread(target=process_all_counties_with_error_handling)
    thread.daemon = True
    
    # Track the thread for cancellation
    running_threads[run_id] = {'thread': thread, 'cancelled': False}
    
    thread.start()


@app.route("/", methods=["GET"])
def root():
    """Root endpoint"""
    return jsonify({
        "status": "ok",
        "service": "School Scraper API",
        "endpoints": {
            "health": "/health",
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


@app.route("/debug/volume", methods=["GET", "OPTIONS"])
@require_auth
def debug_volume():
    """List files in the mounted volume (/data). Runs inside the deployed container."""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        return response, 200

    def _list_dir(p: Path, max_depth: int = 3, depth: int = 0) -> dict:
        if depth > max_depth:
            return {"_truncated": f"max depth {max_depth}"}
        out = {"path": str(p), "exists": p.exists()}
        if not p.exists():
            return out
        if not p.is_dir():
            try:
                out["size"] = p.stat().st_size
            except OSError:
                out["size"] = None
            return out
        try:
            entries = []
            for c in sorted(p.iterdir()):
                e = {"name": c.name}
                if c.is_file():
                    try:
                        e["size"] = c.stat().st_size
                    except OSError:
                        e["size"] = None
                else:
                    e["contents"] = _list_dir(c, max_depth, depth + 1)
                entries.append(e)
            out["entries"] = entries
            return out
        except OSError as err:
            out["error"] = str(err)
            return out

    try:
        data_dir = PERSISTENT_DATA_DIR
        total_size = 0
        file_count = 0
        if data_dir.exists():
            for f in data_dir.rglob("*"):
                if f.is_file():
                    try:
                        total_size += f.stat().st_size
                        file_count += 1
                    except OSError:
                        pass
        listing = _list_dir(data_dir)
        listing["total_size_bytes"] = total_size
        listing["total_size_mb"] = round(total_size / (1024 * 1024), 2)
        listing["file_count"] = file_count
        response = jsonify(listing)
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        response = jsonify({"error": str(e)})
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 500


@app.route("/runs/<run_id>/verify", methods=["GET", "OPTIONS"])
@require_auth
def verify_run_data(run_id: str):
    """
    Verify that a run's data was saved to the volume (metadata, checkpoint, county CSVs).
    Uses the same paths as the pipeline. Call this to confirm data exists before deploying.
    """
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
        response.headers.add("Access-Control-Allow-Methods", "GET, OPTIONS")
        return response, 200

    if not validate_run_id(run_id):
        return jsonify({"status": "error", "error": "Invalid run ID"}), 400

    try:
        # 1. Metadata (same as stop/resume/download)
        meta = load_run_metadata(run_id)
        metadata_ok = meta is not None
        if not metadata_ok:
            response = jsonify({
                "status": "error",
                "error": "Run not found",
                "metadata_ok": False,
                "checkpoint_ok": False,
                "data_saved": False,
            })
            response.headers.add("Access-Control-Allow-Origin", "*")
            return response, 404

        # 2. Checkpoint
        cp = load_checkpoint(run_id)
        checkpoint_ok = cp is not None
        completed = list(cp.get("completed_counties", [])) if cp else []

        # 3. County CSVs (same paths as aggregate_final_results)
        run_dir = RUNS_DIR / run_id
        csvs = []
        total_bytes = 0
        for county in completed:
            county_dir = run_dir / county.replace(" ", "_")
            csv_path = county_dir / "final_contacts.csv"
            exists = csv_path.exists()
            size = csv_path.stat().st_size if exists else 0
            if exists:
                total_bytes += size
            csvs.append({"county": county, "exists": exists, "size_bytes": size})

        data_saved = metadata_ok and checkpoint_ok and all(c["exists"] for c in csvs)
        response = jsonify({
            "status": "ok",
            "run_id": run_id,
            "metadata_ok": metadata_ok,
            "checkpoint_ok": checkpoint_ok,
            "completed_counties": len(completed),
            "csvs": csvs,
            "total_csv_bytes": total_bytes,
            "total_csv_mb": round(total_bytes / (1024 * 1024), 2),
            "data_saved": data_saved,
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        response = jsonify({
            "status": "error",
            "error": str(e),
            "data_saved": False,
        })
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 500


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
        type_param = data.get("type", "school")
        
        if not state:
            return jsonify({
                "status": "error",
                "error": "State parameter is required"
            }), 400
        
        if type_param == "church":
            return jsonify({
                "status": "error",
                "error": "Church scraping is not yet available"
            }), 400
        
        # Check if another run is already active or finalizing
        active_runs = []
        finalizing_runs = []
        current_time = time.time()
        
        for rid, run_data in pipeline_runs.items():
            status = run_data.get("status")
            
            # Check for running runs
            if status == "running":
                # Verify thread is actually alive
                if rid in running_threads:
                    thread = running_threads[rid].get('thread')
                    if thread and thread.is_alive():
                        active_runs.append(rid)
                else:
                    # Thread missing but status is running - mark as stale
                    pipeline_runs[rid]["status"] = "cancelled"
            
            # Check for finalizing runs (2-minute cooldown)
            elif status == "finalizing":
                finalizing_at = run_data.get("finalizingAt", 0)
                elapsed = current_time - finalizing_at
                if elapsed < 120:  # Still in 2-minute cooldown
                    finalizing_runs.append(rid)
                else:
                    # Cooldown expired, mark as completed
                    pipeline_runs[rid]["status"] = "completed"
                    if "completedAt" not in pipeline_runs[rid]:
                        pipeline_runs[rid]["completedAt"] = current_time
        
        if active_runs:
            return jsonify({
                "status": "error",
                "error": f"Another run is already in progress. Please wait for it to complete or stop it first.",
                "activeRunId": active_runs[0]
            }), 409  # Conflict status code
        
        if finalizing_runs:
            return jsonify({
                "status": "error",
                "error": f"A run is currently finalizing. Please wait 2 minutes for the container to restart before starting a new run.",
                "activeRunId": finalizing_runs[0],
                "isFinalizing": True
            }), 409  # Conflict status code
        
        # Generate unique run ID
        run_id = str(uuid.uuid4())
        
        # Start pipeline in background thread
        thread = threading.Thread(target=run_streaming_pipeline, args=(state, run_id))
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
    
    if run_id not in pipeline_runs:
        # Track repeated 404 requests to prevent spam from stale browser tabs
        current_time = time.time()
        if run_id in not_found_runs:
            first_time, count = not_found_runs[run_id]
            # If we've seen 3+ requests for this non-existent run ID within 30 seconds, return 410 Gone
            # This stops polling spam from stale browser tabs quickly
            if count >= 3 and (current_time - first_time) < 30:
                return jsonify({
                    "status": "error",
                    "error": "Run ID not found. This run no longer exists."
                }), 410  # Gone - stop polling
            # Increment count
            not_found_runs[run_id] = (first_time, count + 1)
        else:
            # First time seeing this non-existent run ID
            not_found_runs[run_id] = (current_time, 1)
        
        # Clean up old entries (older than 5 minutes)
        to_remove = [rid for rid, (ft, _) in not_found_runs.items() if current_time - ft > 300]
        for rid in to_remove:
            not_found_runs.pop(rid, None)
        
        return jsonify({
            "status": "error",
            "error": "Run ID not found"
        }), 404
    
    run_data = pipeline_runs[run_id].copy()
    
    # If run is completed, return 410 Gone after grace period to stop polling
    # Allow a 2-minute grace period for the final status fetch, then return 410 Gone
    if run_data.get("status") == "completed":
        completed_time = run_data.get("completedAt")
        if completed_time:
            time_since_completion = time.time() - completed_time
            if time_since_completion > 120:  # 2 minutes grace period, then return 410 Gone
                # Clean up old completed runs from memory (older than 1 hour)
                if time_since_completion > 3600:  # 1 hour
                    pipeline_runs.pop(run_id, None)
                return jsonify({
                    "status": "completed",
                    "message": "Run completed. Status no longer available."
                }), 410  # Gone status code
    
    # Calculate estimated time remaining based on average county processing time
    if run_data["status"] == "running":
        counties_processed = run_data.get("countiesProcessed", 0)
        total_counties = run_data.get("totalCounties", 0)
        county_times = run_data.get("countyTimes", [])
        
        if total_counties > 0:
            remaining_counties = max(0, total_counties - counties_processed)
            
            # Calculate average time per county from completed counties
            if len(county_times) > 0:
                avg_time_per_county = sum(county_times) / len(county_times)
            else:
                # Fallback: use 11.75 minutes (705 seconds) based on actual Illinois run (Dec 11-12, 2025)
                # Actual performance: 101 counties in 19h 47m = 11.75 min/county average
                avg_time_per_county = 705  # 11.75 minutes average from actual production run
            
            # Account for parallel processing (MAX_WORKERS)
            # With N workers, remaining time = (remaining_counties / N) * avg_time
            # This assumes perfect parallelization (reality is ~75% efficiency)
            effective_remaining = max(1, remaining_counties / MAX_WORKERS)
            estimated_remaining = effective_remaining * avg_time_per_county
        
            run_data["estimatedTimeRemaining"] = int(estimated_remaining)
            run_data["averageCountyTime"] = int(avg_time_per_county) if len(county_times) > 0 else None
        else:
            run_data["estimatedTimeRemaining"] = 0
            run_data["averageCountyTime"] = None
    else:
        run_data["estimatedTimeRemaining"] = 0
        run_data["averageCountyTime"] = None
    
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
        runs = list_all_runs()
        
        # Format response
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
        # Check metadata first (persistent storage)
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
        # Check if run is actually running
        current_status = metadata.get("status", pipeline_runs.get(run_id, {}).get("status", "unknown"))
        if current_status != "running":
            return jsonify({
                "status": "error",
                "error": f"Run is not running (current status: {current_status})"
            }), 400
        
        # Set cancellation flag
        if run_id in running_threads:
            running_threads[run_id]['cancelled'] = True
        
        # Update in-memory status if present
        if run_id in pipeline_runs:
            pipeline_runs[run_id]["status"] = "cancelled"
            pipeline_runs[run_id]["statusMessage"] = "Pipeline cancelled by user"
        
        # Update metadata
        metadata.update({
            "status": "cancelled",
            "cancelled_at": datetime.now().isoformat()
        })
        save_run_metadata(run_id, metadata)
        
        print(f"[{run_id}] Pipeline stop requested")
        
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
        # Check metadata first (persistent storage)
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
        state = metadata.get("state")
        if not state:
            return jsonify({
                "status": "error",
                "error": "State not found in run metadata"
            }), 400
        
        # Check if run is currently running
        if run_id in pipeline_runs and pipeline_runs[run_id].get("status") == "running":
            # Check if thread is actually alive
            if run_id in running_threads:
                thread = running_threads[run_id].get('thread')
                if thread and thread.is_alive():
                    return jsonify({
                        "status": "error",
                        "error": "Run is already running. Please stop it first before resuming."
                    }), 409  # Conflict
        
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
        thread = threading.Thread(target=run_streaming_pipeline, args=(state, run_id, True))
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
        # Load metadata to check if run exists
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
        # If run is running, stop it first
        status = metadata.get("status")
        if status == "running":
            # Set cancellation flag to stop the run
            if run_id in running_threads:
                running_threads[run_id]['cancelled'] = True
            
            # Update in-memory status if present
            if run_id in pipeline_runs:
                pipeline_runs[run_id]["status"] = "cancelled"
                pipeline_runs[run_id]["statusMessage"] = "Pipeline cancelled by user"
            
            # Update metadata to cancelled first
            metadata["status"] = "cancelled"
            metadata["cancelled_at"] = datetime.now().isoformat()
            print(f"[{run_id}] Run stopped for deletion")
        
        # Mark as deleted in metadata (don't actually delete files)
        metadata["deleted"] = True
        metadata["deleted_at"] = datetime.now().isoformat()
        save_run_metadata(run_id, metadata)
        
        # Remove from in-memory storage if present
        pipeline_runs.pop(run_id, None)
        running_threads.pop(run_id, None)
        
        print(f"[{run_id}] Run marked as deleted")
        
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
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
        # Cannot archive deleted runs
        if metadata.get("deleted", False):
            return jsonify({
                "status": "error",
                "error": "Cannot archive a deleted run"
            }), 400
        
        # Update metadata to mark as archived
        metadata["archived"] = True
        save_run_metadata(run_id, metadata)
        
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
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
        # Cannot unarchive deleted runs
        if metadata.get("deleted", False):
            return jsonify({
                "status": "error",
                "error": "Cannot unarchive a deleted run"
            }), 400
        
        # Update metadata to remove archived flag
        metadata["archived"] = False
        save_run_metadata(run_id, metadata)
        
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
        # Load metadata to get CSV path
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
        # Get CSV path from metadata or construct it
        csv_path = metadata.get("final_csv_path")
        if not csv_path:
            # Try to construct path
            state = metadata.get("state", "").title()
            run_dir = RUNS_DIR / run_id
            csv_path = str(run_dir / f"{state}_leads_final.csv")
        
        # Check if file exists
        if not os.path.exists(csv_path):
            return jsonify({
                "status": "error",
                "error": "CSV file not found for this run"
            }), 404
        
        # Read and return CSV
        from flask import send_file
        return send_file(
            csv_path,
            mimetype='text/csv',
            as_attachment=True,
            download_name=metadata.get("csv_filename", f"run_{run_id}.csv")
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
        "available_endpoints": ["/", "/health", "/run-pipeline", "/pipeline-status/<run_id>", "/runs", "/runs/<run_id>/download", "/runs/<run_id>/stop", "/runs/<run_id>/delete"]
    }), 404

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
