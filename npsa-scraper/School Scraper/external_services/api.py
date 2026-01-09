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

# Try to import psutil for process tree killing (optional)
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Add parent directory to path to import Pipeline
sys.path.insert(0, str(Path(__file__).parent.parent))
from Pipeline import StreamingPipeline
from assets.shared.models import Contact

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

# Enable CORS - allow all origins for now (restrict in production)
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS"], "allow_headers": ["Content-Type"]}})

# In-memory storage for pipeline runs (use Redis or database in production)
pipeline_runs = {}

# Track running threads and cancellation flags
# Format: {run_id: {'thread': Thread, 'cancelled': bool}}
running_threads = {}

# Track 404 requests for non-existent run IDs to prevent spam from stale browser tabs
# Format: {run_id: (first_404_time, count)}
not_found_runs = {}

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
# Default to 3 for better recovery with parallel workers
CHECKPOINT_BATCH_SIZE = int(os.getenv("CHECKPOINT_BATCH_SIZE", "3"))

# Number of parallel workers for processing counties
# Set to 1 for sequential processing (simplified for debugging)
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
        print(f"{bold('[HEALTH]')} Error listing processes: {e}")
        return {
            'chrome_count': 0,
            'chromedriver_count': 0,
            'total_count': 0,
            'chrome_processes': [],
            'chromedriver_processes': []
        }


def check_health():
    """
    Health check function that lists Chrome and ChromeDriver processes.
    Does NOT kill processes - only reports what exists.
    """
    try:
        process_info = list_chrome_processes()
        print(f"{bold('[HEALTH]')} Chrome: {process_info['chrome_count']}, ChromeDriver: {process_info['chromedriver_count']}, Total: {process_info['total_count']}")
        
        return True
    except Exception as e:
        print(f"{bold('[HEALTH]')} Error in health check: {e}")
        return True  # Don't fail on health check errors


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
    """List all runs from persistent storage"""
    runs = []
    try:
        # Get all metadata files
        for metadata_file in METADATA_DIR.glob("*.json"):
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
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
        # CRITICAL: Aggressive cleanup to prevent Chrome process accumulation
        # Chrome processes can be reparented to PID 1 in containers, so we need to be thorough
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
        
        # Kill Chrome processes using psutil - be aggressive since processes may be reparented
        if HAS_PSUTIL:
            try:
                current_pid = os.getpid()
                killed_count = 0
                max_workers = int(os.getenv("MAX_WORKERS", "1"))
                
                # First pass: Kill direct children (always safe)
                try:
                    current_process = psutil.Process(current_pid)
                    for child in current_process.children(recursive=True):
                        try:
                            name = child.info['name'].lower()
                            if 'chrome' in name or 'chromium' in name or 'chromedriver' in name:
                                child.kill()
                                killed_count += 1
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            continue
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                
                # Second pass: Kill ALL Chrome processes (only in sequential mode)
                # In sequential mode (MAX_WORKERS=1), we're the only worker, so safe to kill all
                # In parallel mode, only kill direct children to avoid killing other workers' Chrome
                if max_workers == 1:
                    for proc in psutil.process_iter(['name', 'pid']):
                        try:
                            name = proc.info['name'].lower()
                            pid = proc.info['pid']
                            
                            # Skip our own process
                            if pid == current_pid:
                                continue
                            
                            # Kill all Chrome-related processes
                            if 'chrome' in name or 'chromium' in name or 'chromedriver' in name:
                                try:
                                    proc.kill()
                                    killed_count += 1
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            continue
                
                # Give processes a moment to die, then force kill any remaining
                if killed_count > 0:
                    time.sleep(1.0)  # Reduced from 3×3s to 1×1s after dumb-init fix
                    try:
                        current_process = psutil.Process(current_pid)
                        for child in current_process.children(recursive=True):
                            try:
                                name = child.info['name'].lower()
                                if 'chrome' in name or 'chromium' in name or 'chromedriver' in name:
                                    if child.is_running():
                                        child.terminate()  # SIGTERM for graceful shutdown
                            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                                continue
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                    
                    if max_workers == 1:
                        # In sequential mode, also check all processes
                        for proc in psutil.process_iter(['name', 'pid']):
                            try:
                                name = proc.info['name'].lower()
                                pid = proc.info['pid']
                                if pid == current_pid:
                                    continue
                                if ('chrome' in name or 'chromium' in name or 'chromedriver' in name):
                                    try:
                                        if proc.is_running():
                                            proc.terminate()  # SIGTERM for graceful shutdown
                                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                                        pass
                            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                                continue
                    
                    print(f"{bold('[CLEANUP]')} Killed {killed_count} Chrome processes using psutil")
            except Exception as e:
                print(f"{bold('[CLEANUP]')} Error killing Chrome processes with psutil: {e}")
        
        # 2-second delay after cleanup to provide buffer
        time.sleep(2.0)
        
        # Fallback: Use pkill to kill Chrome processes
        try:
            import subprocess
            max_workers = int(os.getenv("MAX_WORKERS", "1"))
            
            # Always kill chromedriver (should be safe)
            subprocess.run(['pkill', '-9', '-f', 'chromedriver'], 
                          stdout=subprocess.DEVNULL, 
                          stderr=subprocess.DEVNULL, 
                          timeout=2)
            
            # Only kill all Chrome in sequential mode
            if max_workers == 1:
                subprocess.run(['pkill', '-9', '-f', 'chrome'], 
                              stdout=subprocess.DEVNULL, 
                              stderr=subprocess.DEVNULL, 
                              timeout=2)
                subprocess.run(['pkill', '-9', '-f', 'chromium'], 
                              stdout=subprocess.DEVNULL, 
                              stderr=subprocess.DEVNULL, 
                              timeout=2)
            else:
                # In parallel mode, only kill children of this process
                subprocess.run(['pkill', '-9', '-P', str(os.getpid()), '-f', 'chrome'], 
                              stdout=subprocess.DEVNULL, 
                              stderr=subprocess.DEVNULL, 
                              timeout=2)
                subprocess.run(['pkill', '-9', '-P', str(os.getpid()), '-f', 'chromium'], 
                              stdout=subprocess.DEVNULL, 
                              stderr=subprocess.DEVNULL, 
                              timeout=2)
        except Exception:
            pass  # pkill may not be available or may fail


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
            pipeline_runs[run_id]["status"] = "completed"
            pipeline_runs[run_id]["statusMessage"] = "Pipeline completed but no contacts found."
            pipeline_runs[run_id]["totalContacts"] = 0
            pipeline_runs[run_id]["completedAt"] = time.time()
            return
        
        print(f"[{run_id}] Total contacts collected: {len(all_contacts)}")
        
        # STEP 11: Split contacts into with/without emails
        pipeline_runs[run_id]["statusMessage"] = "Step 11: Splitting contacts..."
        splitter = step11_contact_splitter.ContactSplitter()
        contacts_with_emails, contacts_without_emails = splitter.split_contacts(all_contacts)
        
        print(f"[{run_id}] Step 11 complete: {len(contacts_with_emails)} with emails, {len(contacts_without_emails)} without emails")
        
        # STEP 12: Email Enrichment with Hunter.io (optional)
        contacts_enriched = []
        hunter_io_enabled = os.getenv('HUNTER_IO_API_KEY') is not None
        
        if hunter_io_enabled and contacts_without_emails:
            pipeline_runs[run_id]["statusMessage"] = f"Step 12: Enriching {len(contacts_without_emails)} contacts with Hunter.io..."
            try:
                enricher = step12_hunter_io.HunterIOEnricher(
                    api_key=os.getenv('HUNTER_IO_API_KEY'),
                    verify_emails=False,
                    score_threshold=70
                )
                contacts_enriched = enricher.enrich_contact_objects(
                    contacts=contacts_without_emails,
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
        elif not contacts_without_emails:
            print(f"[{run_id}] Step 12 skipped: No contacts without emails to enrich")
        
        # STEP 13: Compile final CSV
        pipeline_runs[run_id]["statusMessage"] = "Step 13: Compiling final CSV..."
        final_output_csv = str(run_dir / f"{state.title()}_leads_final.csv")
        
        compiler = step13_final_compiler.FinalCompiler()
        final_csv_path = compiler.compile_contacts_to_csv(
            contacts_with_emails=contacts_with_emails,
            contacts_enriched=contacts_enriched,
            output_csv=final_output_csv,
            state=state
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
            pipeline_runs[run_id]["totalContacts"] = len(all_contacts)
            pipeline_runs[run_id]["totalContactsWithEmails"] = len(contacts_with_emails)
            pipeline_runs[run_id]["totalContactsWithoutEmails"] = len(contacts_without_emails) - len(contacts_enriched)
        
        # Update final stats
        pipeline_runs[run_id]["status"] = "completed"
        pipeline_runs[run_id]["statusMessage"] = f"Pipeline completed! Processed {len(counties)} counties. Enriched {len(contacts_enriched)} contacts."
        pipeline_runs[run_id]["currentStep"] = 13
        pipeline_runs[run_id]["progress"] = 100
        pipeline_runs[run_id]["countiesProcessed"] = len(counties)
        pipeline_runs[run_id]["completedAt"] = time.time()  # Track completion time
        
    except Exception as e:
        pipeline_runs[run_id]["status"] = "error"
        pipeline_runs[run_id]["error"] = str(e)
        pipeline_runs[run_id]["statusMessage"] = f"Failed to aggregate results: {str(e)}"
        import traceback
        traceback.print_exc()


def run_streaming_pipeline(state: str, run_id: str):
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
        
        Uses thread-safe checkpointing to enable resume after restarts.
        Thread locks ensure safe concurrent access to shared state.
        """
        try:
            # Load counties for state
            counties = load_counties_from_state(state)
            total_counties = len(counties)
            
            # Check for existing checkpoint to resume
            checkpoint = load_checkpoint(run_id)
            completed_counties = []
            start_index = 0
            
            if checkpoint:
                # Resume from checkpoint
                completed_counties = checkpoint.get("completed_counties", [])
                start_index = checkpoint.get("next_county_index", 0)
                print(f"[{run_id}] Resuming from checkpoint: {len(completed_counties)}/{total_counties} counties already completed")
                print(f"[{run_id}] Resuming from county index {start_index}: {counties[start_index] if start_index < len(counties) else 'N/A'}")
            else:
                # New run - save initial metadata
                initial_metadata = {
                    "run_id": run_id,
                    "state": state,
                    "status": "running",
                    "total_counties": total_counties,
                    "completed_counties": [],
                    "start_time": time.time(),
                    "created_at": datetime.now().isoformat()
                }
                save_run_metadata(run_id, initial_metadata)
                print(f"[{run_id}] New run started: {total_counties} counties to process")
            
            # Update progress tracking with county info
            pipeline_runs[run_id]["totalCounties"] = total_counties
            pipeline_runs[run_id]["statusMessage"] = f"Processing {state} ({len(completed_counties)}/{total_counties} counties completed, resuming from {start_index})..."
            
            # Determine processing mode message
            if MAX_WORKERS == 1:
                print(f"[{run_id}] Processing {total_counties} counties sequentially (starting from index {start_index})...")
            else:
                print(f"[{run_id}] Processing {total_counties} counties with {MAX_WORKERS} parallel workers (starting from index {start_index})...")
            
            # Filter out already completed counties
            remaining_counties = [(idx, county) for idx, county in enumerate(counties) 
                                 if idx >= start_index and county not in completed_counties]
            
            # UNIFIED PROCESSING: Use Pool for both sequential (MAX_WORKERS=1) and parallel (MAX_WORKERS>1)
            # maxtasksperchild=1 forces each worker to terminate after one county,
            # which automatically cleans up all Chrome child processes when the worker dies
            with Pool(processes=MAX_WORKERS, maxtasksperchild=1) as pool:
                # Submit all remaining counties to the pool
                pool_args = [(idx, county, state, run_id, total_counties) for idx, county in remaining_counties]
                
                # Use imap_unordered to get results as they complete (out-of-order is handled)
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
                        
                        # Update progress in main process (thread-safe)
                        with checkpoint_lock:
                            if county not in completed_counties:
                                completed_counties.append(county)
                            
                            completed = len(completed_counties)
                            progress_pct = int((completed / total_counties) * 100)
                            
                            # Update pipeline_runs state
                            with progress_lock:
                                pipeline_runs[run_id]["progress"] = progress_pct
                                pipeline_runs[run_id]["statusMessage"] = f"Processed {completed}/{total_counties} counties..."
                                pipeline_runs[run_id]["countiesProcessed"] = pipeline_runs[run_id].get("countiesProcessed", 0) + 1
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
                            
                            # Save checkpoint after every N counties (batch size)
                            is_checkpoint = completed % CHECKPOINT_BATCH_SIZE == 0 or completed == total_counties
                            if is_checkpoint:
                                save_checkpoint(run_id, state, completed_counties, max(idx + 1, len(completed_counties)), total_counties)
                                
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
                    pipeline_runs[run_id]["status"] = "cancelled"
                    pipeline_runs[run_id]["statusMessage"] = "Pipeline cancelled by user"
                    return
                except Exception as e:
                    print(f"[{run_id}] Error in pool processing: {e}")
                    import traceback
                    traceback.print_exc()
                    pool.terminate()
                    pool.join()
                    # Continue to aggregation if possible, don't crash the entire pipeline
            
            # All counties completed, aggregate results
            print(f"[{run_id}] All counties completed, starting aggregation...")
            aggregate_final_results(run_id, state)
            
            # Final checkpoint - mark as completed
            save_checkpoint(run_id, state, completed_counties, len(counties), total_counties)
            final_metadata = load_run_metadata(run_id) or {}
            final_metadata.update({
                "status": "completed",
                "completed_counties": completed_counties,
                "progress": 100,
                "completed_at": datetime.now().isoformat(),
                "completion_time": time.time()
            })
            save_run_metadata(run_id, final_metadata)
        
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
            # Any other error
            error_msg = str(e)[:500]  # Limit error message length
            # Only update pipeline_runs if run_id still exists (may have been cleaned up)
            if run_id in pipeline_runs:
                pipeline_runs[run_id]["status"] = "error"
                pipeline_runs[run_id]["error"] = error_msg
                pipeline_runs[run_id]["statusMessage"] = f"Pipeline failed: {error_msg}"
            import traceback
            traceback.print_exc()
    
    # Start processing in a background thread
    thread = threading.Thread(target=process_all_counties)
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


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200


@app.route("/run-pipeline", methods=["POST", "OPTIONS"])
def run_pipeline():
    """Start the pipeline and return run ID for status polling"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    if request.method != "POST":
        return jsonify({
            "status": "error",
            "error": f"Method {request.method} not allowed. Use POST."
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
def pipeline_status(run_id):
    """Get status of a running pipeline"""
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
            
            # Account for sequential processing (1 worker)
            # With 1 worker, remaining time = remaining_counties * avg_time
            max_workers = 1
            effective_remaining = max(1, remaining_counties / max_workers)
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
    return jsonify({
        "status": "error",
        "error": f"Method not allowed: {request.method}",
        "path": request.path,
        "allowed_methods": ["POST"] if "/run-pipeline" in request.path else ["GET"]
    }), 405

# Error handler for 404 Not Found
@app.route("/runs", methods=["GET"])
def list_runs():
    """List all runs from persistent storage"""
    try:
        runs = list_all_runs()
        
        # Format response
        response_data = {
            "status": "ok",
            "runs": runs,
            "count": len(runs)
        }
        
        response = jsonify(response_data)
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500


@app.route("/runs/<run_id>/stop", methods=["POST", "OPTIONS"])
def stop_run(run_id: str):
    """Stop a running pipeline"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
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
def resume_run(run_id: str):
    """Resume a stopped or stuck run from its checkpoint"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    try:
        # Check if checkpoint exists
        checkpoint = load_checkpoint(run_id)
        if not checkpoint:
            return jsonify({
                "status": "error",
                "error": "No checkpoint found for this run. Cannot resume."
            }), 404
        
        state = checkpoint.get("state", "").lower()
        if not state:
            return jsonify({
                "status": "error",
                "error": "Invalid checkpoint: missing state"
            }), 400
        
        # Check if run is actually running (thread must be alive)
        if run_id in pipeline_runs and pipeline_runs[run_id].get("status") == "running":
            # Check if thread is actually alive - if dead, allow resume
            if run_id in running_threads:
                thread = running_threads[run_id].get('thread')
                if thread and thread.is_alive():
                    return jsonify({
                        "status": "error",
                        "error": "Run is already in progress"
                    }), 400
            # Thread is dead but status is still "running" - clear it and allow resume
            print(f"[{run_id}] Thread appears dead but status is 'running' - clearing and allowing resume")
            pipeline_runs[run_id]["status"] = "cancelled"
            running_threads.pop(run_id, None)
        
        # Start pipeline in background thread (will auto-resume from checkpoint)
        thread = threading.Thread(target=run_streaming_pipeline, args=(state, run_id))
        thread.daemon = True
        thread.start()
        
        # Track the thread
        running_threads[run_id] = {'thread': thread, 'cancelled': False}
        
        print(f"[{run_id}] Resume requested - restarting pipeline from checkpoint")
        
        response = jsonify({
            "status": "success",
            "message": "Run resumed from checkpoint",
            "runId": run_id,
            "checkpoint": {
                "completed_counties": len(checkpoint.get("completed_counties", [])),
                "total_counties": checkpoint.get("total_counties", 0),
                "next_county_index": checkpoint.get("next_county_index", 0)
            }
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
def delete_run(run_id: str):
    """Delete a finished run (metadata, checkpoint, CSV files)"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "DELETE, OPTIONS")
        return response, 200
    
    try:
        # Load metadata to check if run exists
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
        # Only allow deletion of completed, error, or cancelled runs
        status = metadata.get("status")
        if status == "running":
            return jsonify({
                "status": "error",
                "error": "Cannot delete a running run. Stop it first."
            }), 400
        
        # Delete metadata file
        metadata_path = METADATA_DIR / f"{run_id}.json"
        if metadata_path.exists():
            metadata_path.unlink()
        
        # Delete checkpoint file
        checkpoint_path = CHECKPOINTS_DIR / f"{run_id}.json"
        if checkpoint_path.exists():
            checkpoint_path.unlink()
        
        # Delete CSV file if it exists
        csv_path = metadata.get("final_csv_path")
        if csv_path and os.path.exists(csv_path):
            os.remove(csv_path)
        
        # Delete run directory if it exists
        run_dir = RUNS_DIR / run_id
        if run_dir.exists():
            import shutil
            shutil.rmtree(run_dir)
        
        # Remove from in-memory storage if present
        pipeline_runs.pop(run_id, None)
        running_threads.pop(run_id, None)
        
        print(f"[{run_id}] Run deleted")
        
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
def archive_run(run_id: str):
    """Archive a completed run"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    try:
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
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
def unarchive_run(run_id: str):
    """Unarchive a run"""
    if request.method == "OPTIONS":
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    try:
        metadata = load_run_metadata(run_id)
        if not metadata:
            return jsonify({
                "status": "error",
                "error": "Run not found"
            }), 404
        
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
def download_run_csv(run_id: str):
    """Download the final CSV for a completed run"""
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

# Production: Always use Waitress, even if file is run directly
# This ensures Railway (or any environment) uses Waitress instead of Flask dev server
if __name__ == "__main__":
    import subprocess
    import sys
    port = os.environ.get("PORT", "8080")
    print(f"Starting Waitress WSGI server on port {port}")
    print(f"Using production WSGI server (Waitress) instead of Flask dev server")
    # Use Waitress even when file is run directly
    subprocess.run([
        sys.executable, "-m", "waitress",
        "--host=0.0.0.0",
        f"--port={port}",
        "--threads=4",
        "--channel-timeout=300",
        "external_services.api:app"
    ])
