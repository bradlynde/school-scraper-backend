"""
Backend API for School Scraper Pipeline
Deploy this to Cloud Run to handle POST requests from Vercel frontend
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
from cloud_storage_helper import CloudStorageHelper

app = Flask(__name__)

# Enable CORS - allow all origins for now (restrict in production)
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS"], "allow_headers": ["Content-Type"]}})

# In-memory storage for pipeline runs (use Redis or database in production)
pipeline_runs = {}


def load_counties_from_state(state: str) -> list:
    """Load counties for a given state from data/states/{state}.txt"""
    state_normalized = state.lower().replace(' ', '_')
    script_dir = Path(__file__).parent
    state_file = script_dir / 'data' / 'states' / f'{state_normalized}.txt'
    
    if not state_file.exists():
        raise FileNotFoundError(f"State file not found: {state_file}")
    
    counties = []
    with open(state_file, 'r', encoding='utf-8') as f:
        for line in f:
            county = line.strip()
            if county and not county.startswith('#'):
                counties.append(county)
    
    return counties


def process_single_county(state: str, county: str, run_id: str, gcs_helper: CloudStorageHelper, county_index: int, total_counties: int):
    """
    Process a single county through the entire pipeline using StreamingPipeline class
    
    Returns:
        dict with results: {'schools': count, 'contacts': count, 'contacts_no_emails': count, 'success': bool, 'csv_path': str}
    """
    from streaming_pipeline import StreamingPipeline
    
    county_run_id = f"{run_id}_county_{county_index}"
    county_path_prefix = f"runs/{run_id}/counties/{county.replace(' ', '_')}"
    
    try:
        # Update progress for this county
        pipeline_runs[run_id]["statusMessage"] = f"Processing {county} County ({county_index + 1}/{total_counties})..."
        pipeline_runs[run_id]["currentCounty"] = county
        pipeline_runs[run_id]["currentCountyIndex"] = county_index + 1
        
        # Create temporary output files
        output_csv = f"/tmp/final_contacts_{county_run_id}.csv"
        output_no_emails_csv = f"/tmp/final_contacts_no_emails_{county_run_id}.csv"
        
        # Initialize pipeline for this county
        pipeline = StreamingPipeline(
            google_api_key=os.getenv("GOOGLE_PLACES_API_KEY", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            global_max_api_calls=None,  # No limit for full state runs
            max_pages_per_school=3,
            state=state
        )
        
        # Run pipeline for this single county
        pipeline.run(
            counties=[county],  # Process only this county
            cities=None,
            batch_size=0,  # Process all schools in county
            output_csv=output_csv,
            output_no_emails_csv=output_no_emails_csv
        )
        
        # Read results
        results = {
            'success': True,
            'schools': pipeline.stats.get('schools_processed', 0),
            'contacts': 0,
            'contacts_no_emails': 0,
            'csv_path': None,
            'csv_no_emails_path': None
        }
        
        # Process contacts with emails
        if os.path.exists(output_csv):
            df = pd.read_csv(output_csv)
            results['contacts'] = len(df)
            results['csv_path'] = output_csv
            # Upload to Cloud Storage
            gcs_helper.upload_csv(output_csv, f"{county_path_prefix}/final_contacts.csv")
        
        # Process contacts without emails
        if os.path.exists(output_no_emails_csv):
            df = pd.read_csv(output_no_emails_csv)
            results['contacts_no_emails'] = len(df)
            results['csv_no_emails_path'] = output_no_emails_csv
            # Upload to Cloud Storage
            gcs_helper.upload_csv(output_no_emails_csv, f"{county_path_prefix}/final_contacts_no_emails.csv")
        
        return results
            
    except Exception as e:
        import traceback
        error_msg = str(e)[:500]
        print(f"Error processing {county}: {error_msg}")
        traceback.print_exc()
        return {'success': False, 'error': error_msg}


def process_next_county(state: str, run_id: str, county_index: int):
    """
    Process a single county, then trigger the next one via HTTP request.
    This ensures each county is processed in a separate request, avoiding timeout.
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
        
        # Initialize Cloud Storage helper
        bucket_name = os.getenv("BUCKET_NAME", "school-scraper-data")
        gcs_helper = CloudStorageHelper(bucket_name)
        
        # Update progress
        progress_pct = int((county_index / total_counties) * 100)
        pipeline_runs[run_id]["progress"] = progress_pct
        pipeline_runs[run_id]["countiesProcessed"] = county_index
        pipeline_runs[run_id]["currentCounty"] = county
        pipeline_runs[run_id]["currentCountyIndex"] = county_index + 1
        pipeline_runs[run_id]["statusMessage"] = f"Processing {county} County ({county_index + 1}/{total_counties})..."
        
        # Process this county
        result = process_single_county(state, county, run_id, gcs_helper, county_index, total_counties)
        
        if not result.get('success'):
            print(f"County {county} failed: {result.get('error', 'Unknown error')}")
            pipeline_runs[run_id]["statusMessage"] = f"Warning: {county} County failed, continuing..."
        
        # Trigger next county via HTTP request to ourselves
        # This ensures each county is a separate request (avoids timeout)
        # Get Cloud Run URL from environment (set automatically by Cloud Run)
        service_url = os.getenv("CLOUD_RUN_SERVICE_URL")
        if not service_url:
            # Fallback: try to construct from known patterns or use localhost for testing
            service_url = os.getenv("SERVICE_URL", "http://localhost:8080")
        
        next_url = f"{service_url}/process-county"
        
        try:
            # Make async request to process next county
            requests.post(next_url, json={
                "state": state,
                "run_id": run_id,
                "county_index": county_index + 1
            }, timeout=2)  # Fire and forget
        except Exception as e:
            # If self-request fails, continue in background thread as fallback
            # This handles cases where we can't make HTTP requests to ourselves
            print(f"Self-request failed, using background thread: {e}")
            thread = threading.Thread(target=process_next_county, args=(state, run_id, county_index + 1))
            thread.daemon = True
            thread.start()
        
    except Exception as e:
        pipeline_runs[run_id]["status"] = "error"
        pipeline_runs[run_id]["error"] = str(e)
        pipeline_runs[run_id]["statusMessage"] = f"Pipeline failed at county {county_index}: {str(e)}"
        import traceback
        traceback.print_exc()


def aggregate_final_results(run_id: str, state: str):
    """Aggregate all county results into final CSVs"""
    try:
        counties = load_counties_from_state(state)
        bucket_name = os.getenv("BUCKET_NAME", "school-scraper-data")
        gcs_helper = CloudStorageHelper(bucket_name)
        
        pipeline_runs[run_id]["statusMessage"] = "Aggregating results from all counties..."
        
        all_contacts_with_emails = []
        all_contacts_no_emails = []
        total_schools = 0
        
        # Download and combine all county CSVs
        for i, county in enumerate(counties):
            county_path_prefix = f"runs/{run_id}/counties/{county.replace(' ', '_')}"
            
            # Try to read contacts with emails
            df_with = gcs_helper.read_csv_to_dataframe(f"{county_path_prefix}/final_contacts.csv")
            if df_with is not None and len(df_with) > 0:
                if 'email' in df_with.columns:
                    df_with_emails = df_with[df_with['email'].notna() & (df_with['email'] != '') & (df_with['email'] != '')]
                    if len(df_with_emails) > 0:
                        all_contacts_with_emails.append(df_with_emails)
                    
                    df_no_emails = df_with[df_with['email'].isna() | (df_with['email'] == '') | (df_with['email'] == '')]
                    if len(df_no_emails) > 0:
                        all_contacts_no_emails.append(df_no_emails)
                else:
                    all_contacts_with_emails.append(df_with)
            
            # Also check for separate no-emails file
            df_no_emails_separate = gcs_helper.read_csv_to_dataframe(f"{county_path_prefix}/final_contacts_no_emails.csv")
            if df_no_emails_separate is not None and len(df_no_emails_separate) > 0:
                all_contacts_no_emails.append(df_no_emails_separate)
        
        # Combine all results
        if all_contacts_with_emails:
            final_df_with_emails = pd.concat(all_contacts_with_emails, ignore_index=True)
            final_df_with_emails = final_df_with_emails.drop_duplicates(
                subset=['first_name', 'last_name', 'school_name'],
                keep='first'
            )
            pipeline_runs[run_id]["totalContacts"] = len(final_df_with_emails)
            
            gcs_path = f"runs/{run_id}/final_contacts.csv"
            gcs_helper.write_dataframe_to_csv(final_df_with_emails, gcs_path)
            csv_content = final_df_with_emails.to_csv(index=False)
            pipeline_runs[run_id]["csvData"] = csv_content
            pipeline_runs[run_id]["csvFilename"] = f"school_contacts_with_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        if all_contacts_no_emails:
            final_df_no_emails = pd.concat(all_contacts_no_emails, ignore_index=True)
            final_df_no_emails = final_df_no_emails.drop_duplicates(
                subset=['first_name', 'last_name', 'school_name'],
                keep='first'
            )
            pipeline_runs[run_id]["totalContactsNoEmails"] = len(final_df_no_emails)
            
            gcs_path = f"runs/{run_id}/final_contacts_no_emails.csv"
            gcs_helper.write_dataframe_to_csv(final_df_no_emails, gcs_path)
            csv_content = final_df_no_emails.to_csv(index=False)
            pipeline_runs[run_id]["csvNoEmailsData"] = csv_content
            pipeline_runs[run_id]["csvNoEmailsFilename"] = f"school_contacts_no_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Update final stats
        pipeline_runs[run_id]["status"] = "completed"
        pipeline_runs[run_id]["statusMessage"] = f"Pipeline completed! Processed {len(counties)} counties."
        pipeline_runs[run_id]["currentStep"] = 7
        pipeline_runs[run_id]["progress"] = 100
        pipeline_runs[run_id]["countiesProcessed"] = len(counties)
        
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
        error_msg = f"State file not found. Please ensure data/states/{state.lower().replace(' ', '_')}.txt exists in the repository."
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
    
    # Calculate estimated time remaining based on counties
    if run_data["status"] == "running" and run_data.get("totalCounties", 0) > 0:
        counties_processed = run_data.get("countiesProcessed", 0)
        total_counties = run_data.get("totalCounties", 1)
        
        if counties_processed > 0:
            # Estimate: ~20 minutes per county average
            avg_time_per_county = 20 * 60  # seconds
            remaining_counties = total_counties - counties_processed
            estimated_remaining = remaining_counties * avg_time_per_county
            run_data["estimatedTimeRemaining"] = int(estimated_remaining)
        else:
            run_data["estimatedTimeRemaining"] = total_counties * 20 * 60
    else:
        run_data["estimatedTimeRemaining"] = 0
    
    response = jsonify(run_data)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response, 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
