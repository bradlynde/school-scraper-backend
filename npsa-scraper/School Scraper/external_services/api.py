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

# Track 404 requests for non-existent run IDs to prevent spam from stale browser tabs
# Format: {run_id: (first_404_time, count)}
not_found_runs = {}


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
    
    try:
        # Update progress for this county
        print(f"[{run_id}] Starting processing for {county} County ({county_index + 1}/{total_counties})")
        pipeline_runs[run_id]["statusMessage"] = f"Processing {county} County ({county_index + 1}/{total_counties})..."
        pipeline_runs[run_id]["currentCounty"] = county
        pipeline_runs[run_id]["currentCountyIndex"] = county_index + 1
        pipeline_runs[run_id]["currentStep"] = 1
        
        # Create temporary output directory for this run
        run_dir = Path(f"/tmp/runs/{run_id}")
        run_dir.mkdir(parents=True, exist_ok=True)
        county_dir = run_dir / county.replace(' ', '_')
        county_dir.mkdir(parents=True, exist_ok=True)
        
        # Create temporary output file (single file for all contacts)
        output_csv = str(county_dir / "final_contacts.csv")
        
        # Initialize pipeline for this county
        print(f"[{run_id}] Initializing pipeline for {county} County...")
        pipeline = StreamingPipeline(
            google_api_key=os.getenv("GOOGLE_PLACES_API_KEY", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            global_max_api_calls=None,  # No limit for full state runs
            max_pages_per_school=2,  # Reduced from 3 to 2 for faster processing
            state=state
        )
        
        # Run pipeline for this single county - collects all contacts
        print(f"[{run_id}] Running pipeline for {county} County (this may take 15-30 minutes)...")
        pipeline.run(
            counties=[county],  # Process only this county
            batch_size=0,  # Process all schools in county
            output_csv=output_csv
        )
        
        # Pipeline.run() already handles all compilation and writes to output_csv
        # Just verify the file was created
        all_contacts = pipeline.all_contacts
        print(f"[{run_id}] Pipeline completed for {county} County - {len(all_contacts)} contacts extracted")
        
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
            print(f"County {county} failed: {result.get('error', 'Unknown error')}")
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
        run_dir = Path(f"/tmp/runs/{run_id}")
        
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
    Initialize the pipeline and process all counties using a thread pool.
    Uses ThreadPoolExecutor with 4 workers to process counties in parallel.
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
        """Process all counties using thread pool"""
        try:
            # Load counties for state
            counties = load_counties_from_state(state)
            total_counties = len(counties)
            
            # Update progress tracking with county info
            pipeline_runs[run_id]["totalCounties"] = total_counties
            pipeline_runs[run_id]["statusMessage"] = f"Starting pipeline for {state} ({total_counties} counties)..."
            
            # Use ThreadPoolExecutor with 1 worker (prevents Selenium Chrome crashes)
            # 1 worker = sequential processing, stable but slower
            max_workers = 1
            print(f"[{run_id}] Processing {total_counties} counties with {max_workers} concurrent workers...")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all counties to the thread pool
                future_to_county = {
                    executor.submit(process_county_with_timing, state, run_id, county, idx, total_counties): (idx, county)
                    for idx, county in enumerate(counties)
                }
                
                # Process completed counties as they finish
                completed = 0
                for future in as_completed(future_to_county):
                    county_index, county = future_to_county[future]
                    try:
                        idx, result, processing_time = future.result()
                        completed += 1
                        
                        # Update progress
                        progress_pct = int((completed / total_counties) * 100)
                        pipeline_runs[run_id]["progress"] = progress_pct
                        pipeline_runs[run_id]["statusMessage"] = f"Processed {completed}/{total_counties} counties..."
                        
                        print(f"[{run_id}] Progress: {completed}/{total_counties} counties completed")
                        
                    except Exception as e:
                        print(f"[{run_id}] County {county} generated an exception: {e}")
                        import traceback
                        traceback.print_exc()
            
            # All counties completed, aggregate results
            print(f"[{run_id}] All counties completed, starting aggregation...")
            aggregate_final_results(run_id, state)
        
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
@app.errorhandler(404)
def not_found(e):
    return jsonify({
        "status": "error",
        "error": f"Endpoint not found: {request.path}",
        "available_endpoints": ["/", "/health", "/run-pipeline", "/pipeline-status/<run_id>"]
    }), 404

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting Flask server on port {port}")
    print(f"Available routes:")
    for rule in app.url_map.iter_rules():
        print(f"  {rule.rule} -> {rule.endpoint} [{', '.join(rule.methods)}]")
    app.run(host="0.0.0.0", port=port, debug=False)
