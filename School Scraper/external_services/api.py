"""
Backend API for School Scraper Pipeline
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

# Add parent directory to path to import Pipeline
sys.path.insert(0, str(Path(__file__).parent.parent))
from Pipeline import StreamingPipeline
from assets.shared.models import Contact
import step11_contact_splitter
import step12_hunter_io
import step13_final_compiler

app = Flask(__name__)

# Enable CORS - allow all origins for now (restrict in production)
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS"], "allow_headers": ["Content-Type"]}})

# In-memory storage for pipeline runs (use Redis or database in production)
pipeline_runs = {}


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


def process_next_county(state: str, run_id: str, county_index: int):
    """
    Process a single county, then process the next one in a background thread.
    Railway supports long-running processes, so no need for HTTP self-requests.
    """
    try:
        # Load counties
        counties = load_counties_from_state(state)
        total_counties = len(counties)
        
        if county_index >= total_counties:
            # All counties done, aggregate results
            aggregate_final_results(run_id, state)
            return
        
        county = counties[county_index]
        
        # Update progress
        progress_pct = int((county_index / total_counties) * 100)
        pipeline_runs[run_id]["progress"] = progress_pct
        pipeline_runs[run_id]["countiesProcessed"] = county_index
        pipeline_runs[run_id]["currentCounty"] = county
        pipeline_runs[run_id]["currentCountyIndex"] = county_index + 1
        pipeline_runs[run_id]["statusMessage"] = f"Processing {county} County ({county_index + 1}/{total_counties})..."
        
        # Process this county
        result = process_single_county(state, county, run_id, county_index, total_counties)
        
        if not result.get('success'):
            print(f"County {county} failed: {result.get('error', 'Unknown error')}")
            pipeline_runs[run_id]["statusMessage"] = f"Warning: {county} County failed, continuing..."
        
        # Update progress after county completion
        pipeline_runs[run_id]["countiesProcessed"] = county_index + 1
        pipeline_runs[run_id]["schoolsFound"] = pipeline_runs[run_id].get("schoolsFound", 0) + result.get('schools', 0)
        pipeline_runs[run_id]["schoolsProcessed"] = pipeline_runs[run_id].get("schoolsProcessed", 0) + result.get('schools', 0)
        
        # Process next county in background thread (Railway supports long-running processes)
        # No need for HTTP self-requests - just continue in thread
        thread = threading.Thread(target=process_next_county, args=(state, run_id, county_index + 1))
        thread.daemon = True
        thread.start()
        print(f"[{run_id}] Started processing next county in background thread")
        
    except Exception as e:
        pipeline_runs[run_id]["status"] = "error"
        pipeline_runs[run_id]["error"] = str(e)
        pipeline_runs[run_id]["statusMessage"] = f"Pipeline failed at county {county_index}: {str(e)}"
        import traceback
        traceback.print_exc()


def aggregate_final_results(run_id: str, state: str):
    """Aggregate all county results, run global Steps 11-13, and generate final CSV"""
    try:
        counties = load_counties_from_state(state)
        run_dir = Path(f"/tmp/runs/{run_id}")
        
        pipeline_runs[run_id]["statusMessage"] = "Waiting for all counties to complete..."
        
        # Wait for all counties to complete (check that all CSV files exist)
        max_wait_time = 3600  # 1 hour max wait
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
                            # Handle email - convert empty strings to None
                            email_val = row.get('email', '') or row.get('Email', '') or ''
                            if email_val and email_val.strip():
                                email_val = email_val.strip()
                            else:
                                email_val = None
                            
                            contact = Contact(
                                first_name=str(row.get('first_name', '') or row.get('First Name', '') or '').strip(),
                                last_name=str(row.get('last_name', '') or row.get('Last Name', '') or '').strip(),
                                title=str(row.get('title', '') or row.get('Title', '') or '').strip(),
                                email=email_val,
                                phone=str(row.get('phone', '') or row.get('Phone', '') or '').strip(),
                                school_name=str(row.get('school_name', '') or row.get('School Name', '') or '').strip(),
                                source_url=str(row.get('source_url', '') or row.get('Source URL', '') or '').strip()
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
    Initialize the pipeline and start processing the first county.
    Each county will trigger the next one via HTTP request.
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
    }
    
    try:
        # Load counties for state
        counties = load_counties_from_state(state)
        total_counties = len(counties)
        
        # Update progress tracking with county info
        pipeline_runs[run_id]["totalCounties"] = total_counties
        pipeline_runs[run_id]["statusMessage"] = f"Starting pipeline for {state} ({total_counties} counties)..."
        
        # Start processing first county
        process_next_county(state, run_id, 0)
        
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


@app.route("/process-county", methods=["POST"])
def process_county():
    """Process a single county (called internally to avoid timeout)"""
    try:
        data = request.get_json() or {}
        state = data.get("state", "").lower()
        run_id = data.get("run_id")
        county_index = data.get("county_index", 0)
        
        if not state or not run_id:
            return jsonify({"status": "error", "error": "Missing parameters"}), 400
        
        # Process county in background thread
        thread = threading.Thread(target=process_next_county, args=(state, run_id, county_index))
        thread.daemon = True
        thread.start()
        
        return jsonify({"status": "processing", "county_index": county_index}), 200
        
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/pipeline-status/<run_id>", methods=["GET"])
def pipeline_status(run_id):
    """Get status of a running pipeline"""
    if run_id not in pipeline_runs:
        return jsonify({
            "status": "error",
            "error": "Run ID not found"
        }), 404
    
    run_data = pipeline_runs[run_id].copy()
    
    # If run is completed and has been completed for more than 5 minutes, return 410 Gone
    # This prevents unnecessary polling after completion
    if run_data.get("status") == "completed":
        completed_time = run_data.get("completedAt")
        if completed_time:
            time_since_completion = time.time() - completed_time
            if time_since_completion > 300:  # 5 minutes
                return jsonify({
                    "status": "completed",
                    "message": "Run completed. Status no longer available."
                }), 410  # Gone status code
    
    # Calculate estimated time remaining based on schools (1 minute per school)
    if run_data["status"] == "running":
        schools_found = run_data.get("schoolsFound", 0)
        schools_processed = run_data.get("schoolsProcessed", 0)
        
        # Estimate: 1 minute per school
        avg_time_per_school = 60  # seconds
        remaining_schools = max(0, schools_found - schools_processed)
        estimated_remaining = remaining_schools * avg_time_per_school
        
        # If we don't have school data yet, estimate based on counties
        if schools_found == 0 and run_data.get("totalCounties", 0) > 0:
            counties_processed = run_data.get("countiesProcessed", 0)
            total_counties = run_data.get("totalCounties", 1)
            # Estimate ~10 schools per county, 1 minute per school
            estimated_schools_remaining = (total_counties - counties_processed) * 10
            estimated_remaining = estimated_schools_remaining * avg_time_per_school
        
        run_data["estimatedTimeRemaining"] = int(estimated_remaining)
    else:
        run_data["estimatedTimeRemaining"] = 0
    
    response = jsonify(run_data)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response, 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
