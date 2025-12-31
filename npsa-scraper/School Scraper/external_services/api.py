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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
# Default to 2 for better recovery with parallel workers
CHECKPOINT_BATCH_SIZE = int(os.getenv("CHECKPOINT_BATCH_SIZE", "2"))

# Number of parallel workers for processing counties
# Default to 2 for 50% time reduction while maintaining stability
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "2"))

# Thread locks for thread-safe operations
checkpoint_lock = threading.Lock()
progress_lock = threading.Lock()


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


def process_single_county(state: str, county: str, run_id: str, county_index: int, total_counties: int):
    """
    Process a single county through the entire pipeline using StreamingPipeline class
    
    Returns:
        dict with results: {'schools': count, 'contacts': count, 'contacts_no_emails': count, 'success': bool, 'csv_path': str}
    """
    county_run_id = f"{run_id}_county_{county_index}"
    county_path_prefix = f"runs/{run_id}/counties/{county.replace(' ', '_')}"
    
    pipeline = None
    try:
        # Update progress for this county
        print(f"[{run_id}] {county} County ({county_index + 1}/{total_counties})")
        pipeline_runs[run_id]["statusMessage"] = f"Processing {county} County ({county_index + 1}/{total_counties})..."
        pipeline_runs[run_id]["currentCounty"] = county
        pipeline_runs[run_id]["currentCountyIndex"] = county_index + 1
        pipeline_runs[run_id]["currentStep"] = 1
        
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
        print(f"[{run_id}] ✓ {county}: {len(all_contacts)} contacts")
        
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
        
        return results
            
    except Exception as e:
        import traceback
        error_msg = str(e)[:500]
        print(f"Error processing {county}: {error_msg}")
        traceback.print_exc()
        return {'success': False, 'error': error_msg}
    finally:
        # CRITICAL: Always cleanup pipeline resources (especially Selenium drivers)
        # This prevents resource leaks that cause degradation after long runs
        if pipeline:
            try:
                print(f"[{run_id}] Cleaning up resources for {county}...")
                pipeline.cleanup()
                print(f"[{run_id}] ✓ Cleanup successful for {county}")
                
                # EXPLICIT GARBAGE COLLECTION: Force cleanup after each county
                # This ensures Selenium driver and Chrome processes are fully released
                gc.collect()
                
            except Exception as cleanup_error:
                # Log but don't raise - cleanup failures shouldn't mask original errors
                print(f"[{run_id}] Warning: Error during pipeline cleanup for {county}: {cleanup_error}")
        else:
            print(f"[{run_id}] No pipeline instance to cleanup for {county}")


def process_county_with_timing(state: str, run_id: str, county: str, county_index: int, total_counties: int):
    """
    Process a single county and track timing for average calculation.
    Returns tuple: (county_index, result_dict, processing_time_seconds)
    """
    start_time = time.time()
    try:
        # Update progress
        progress_pct = int((county_index / total_counties) * 100)
        pipeline_runs[run_id]["progress"] = progress_pct
        pipeline_runs[run_id]["currentCounty"] = county
        pipeline_runs[run_id]["currentCountyIndex"] = county_index + 1
        pipeline_runs[run_id]["statusMessage"] = f"Processing {county} County ({county_index + 1}/{total_counties})..."
        
        # Process this county
        result = process_single_county(state, county, run_id, county_index, total_counties)
        
        processing_time = time.time() - start_time
        
        if not result.get('success'):
            print(f"[{run_id}] ❌ {county}: {result.get('error', 'Unknown error')}")
            pipeline_runs[run_id]["statusMessage"] = f"Warning: {county} County failed, continuing..."
        
        # Update progress after county completion
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
        
        return (county_index, result, processing_time)
        
    except Exception as e:
        processing_time = time.time() - start_time
        error_msg = f"Pipeline failed at county {county_index}: {str(e)}"
        print(f"[{run_id}] {error_msg}")
        pipeline_runs[run_id]["statusMessage"] = f"Warning: {county} County failed, continuing..."
        import traceback
        traceback.print_exc()
        return (county_index, {'success': False, 'error': str(e)}, processing_time)


def aggregate_final_results(run_id: str, state: str):
    """Aggregate all county results, run global Steps 11-13, and generate final CSV"""
    try:
        counties = load_counties_from_state(state)
        run_dir = RUNS_DIR / run_id
        
        pipeline_runs[run_id]["statusMessage"] = "Waiting for all counties to complete..."
        
        # Wait for all counties to complete (check that all CSV files exist)
        # With thread pool, counties should complete more reliably, but add timeout as safety
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
                print(f"[{run_id}] ⚠️  Email enrichment failed: {e}")
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
    
    Parallel processing uses ThreadPoolExecutor with thread-safe:
    - Checkpoint coordination (locks prevent race conditions)
    - Progress tracking (locks ensure accurate updates)
    - Out-of-order county completion handling
    
    This approach balances speed with resource management and prevents
    the thread exhaustion issues seen after ~12+ hours.
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
        - Parallel (MAX_WORKERS>1): Multiple counties simultaneously with ThreadPoolExecutor
        
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
            
            # Determine processing mode based on MAX_WORKERS
            if MAX_WORKERS == 1:
                print(f"[{run_id}] Processing {total_counties} counties sequentially (starting from index {start_index})...")
                processing_mode = "sequential"
            else:
                print(f"[{run_id}] Processing {total_counties} counties with {MAX_WORKERS} parallel workers (starting from index {start_index})...")
                processing_mode = "parallel"
            
            # Filter out already completed counties for parallel processing
            remaining_counties = [(idx, county) for idx, county in enumerate(counties) 
                                 if idx >= start_index and county not in completed_counties]
            
            if processing_mode == "sequential":
                # SEQUENTIAL PROCESSING: Process counties one at a time
                for idx, county in remaining_counties:
                    # Check for cancellation before each county
                    if running_threads.get(run_id, {}).get('cancelled', False):
                        pipeline_runs[run_id]["status"] = "cancelled"
                        pipeline_runs[run_id]["statusMessage"] = "Pipeline cancelled by user"
                        print(f"[{run_id}] Pipeline cancelled during processing")
                        return
                    
                    try:
                        # Process this county with timing
                        county_index, result, processing_time = process_county_with_timing(
                            state, run_id, county, idx, total_counties
                        )
                        
                        # Mark county as completed
                        with checkpoint_lock:
                            if county not in completed_counties:
                                completed_counties.append(county)
                            
                            # Update progress
                            completed = len(completed_counties)
                            progress_pct = int((completed / total_counties) * 100)
                            
                            with progress_lock:
                                pipeline_runs[run_id]["progress"] = progress_pct
                                pipeline_runs[run_id]["statusMessage"] = f"Processed {completed}/{total_counties} counties..."
                            
                            print(f"[{run_id}] Progress: {completed}/{total_counties} counties completed")
                            
                            # Save checkpoint after every N counties (batch size)
                            if completed % CHECKPOINT_BATCH_SIZE == 0 or completed == total_counties:
                                save_checkpoint(run_id, state, completed_counties, idx + 1, total_counties)
                                
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
                        
                        # EXPLICIT GARBAGE COLLECTION: Force cleanup after each county
                        gc.collect()
                        
                    except Exception as e:
                        print(f"[{run_id}] County {county} generated an exception: {e}")
                        import traceback
                        traceback.print_exc()
                        continue
            else:
                # PARALLEL PROCESSING: Process counties with multiple workers
                def process_with_tracking(idx, county):
                    """Wrapper to add thread-safe tracking for parallel processing"""
                    try:
                        # Process this county with timing
                        county_index, result, processing_time = process_county_with_timing(
                            state, run_id, county, idx, total_counties
                        )
                        
                        # Thread-safe progress update
                        with checkpoint_lock:
                            if county not in completed_counties:
                                completed_counties.append(county)
                            
                            completed = len(completed_counties)
                            progress_pct = int((completed / total_counties) * 100)
                            
                            with progress_lock:
                                pipeline_runs[run_id]["progress"] = progress_pct
                                pipeline_runs[run_id]["statusMessage"] = f"Processed {completed}/{total_counties} counties..."
                            
                            print(f"[{run_id}] Progress: {completed}/{total_counties} counties completed")
                            
                            # Save checkpoint after every N counties (batch size)
                            if completed % CHECKPOINT_BATCH_SIZE == 0 or completed == total_counties:
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
                        
                        # EXPLICIT GARBAGE COLLECTION: Force cleanup after each county
                        gc.collect()
                        
                        return (county_index, result, processing_time)
                    except Exception as e:
                        print(f"[{run_id}] County {county} generated an exception: {e}")
                        import traceback
                        traceback.print_exc()
                        return (idx, {'success': False, 'error': str(e)}, 0)
                
                # Process with ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    # Submit all remaining counties to the pool
                    future_to_county = {
                        executor.submit(process_with_tracking, idx, county): (idx, county)
                        for idx, county in remaining_counties
                    }
                    
                    # Process results as they complete (out-of-order is handled)
                    for future in as_completed(future_to_county):
                        # Check for cancellation
                        if running_threads.get(run_id, {}).get('cancelled', False):
                            # Cancel remaining futures
                            for f in future_to_county:
                                f.cancel()
                            pipeline_runs[run_id]["status"] = "cancelled"
                            pipeline_runs[run_id]["statusMessage"] = "Pipeline cancelled by user"
                            print(f"[{run_id}] Pipeline cancelled during parallel processing")
                            return
                        
                        idx, county = future_to_county[future]
                        try:
                            future.result()  # Wait for completion
                        except Exception as e:
                            print(f"[{run_id}] Future for {county} County raised exception: {e}")
                            import traceback
                            traceback.print_exc()
            
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
            pipeline_runs[run_id]["status"] = "error"
            pipeline_runs[run_id]["error"] = error_msg
            pipeline_runs[run_id]["statusMessage"] = f"Pipeline failed: {error_msg}"
            import traceback
            traceback.print_exc()
        except Exception as e:
            # Any other error
            error_msg = str(e)[:500]  # Limit error message length
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


# Removed /process-county endpoint - no longer needed with thread pool approach


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
        "--threads=1",
        "--channel-timeout=300",
        "external_services.api:app"
    ])
