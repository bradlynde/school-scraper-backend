"""
Backend API for School Scraper Pipeline
Deploy this to Cloud Run to handle POST requests from Vercel frontend
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import subprocess
import os
import json
from datetime import datetime
import pandas as pd

app = Flask(__name__)

# Enable CORS - allow all origins for now (restrict in production)
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS"], "allow_headers": ["Content-Type"]}})

def run_pipeline_steps(max_schools=5):
    """
    Run all 5 pipeline steps and return summary
    """
    summary = {
        "status": "success",
        "steps": [],
        "totalContacts": 0,
        "runId": datetime.now().isoformat()
    }
    
    try:
        # Step 1: School Discovery
        print("Running Step 1: School Discovery...")
        subprocess.run([
            "python3", "step1.py",
            "--api-key", os.getenv("GOOGLE_PLACES_API_KEY", ""),
            "--state", "Texas",
            "--max-counties", "1",
            "--max-search-terms", "2",
            "--max-api-calls", "10",
            "--output", "step1_schools.csv"
        ], check=True, capture_output=True)
        
        # Read Step 1 results
        df1 = pd.read_csv("step1_schools.csv")
        schools_found = len(df1)
        summary["steps"].append({
            "name": "Step 1: School Discovery",
            "schoolsFound": schools_found
        })
        
        # Limit to max_schools for testing
        if len(df1) > max_schools:
            df1.head(max_schools).to_csv("step1_schools.csv", index=False)
            schools_found = max_schools
        
        # Step 2: Page Discovery
        print("Running Step 2: Page Discovery...")
        subprocess.run([
            "python3", "step2.py",
            "--input", "step1_schools.csv",
            "--output", "step2_pages.csv",
            "--max-pages-per-school", "30",
            "--top-pages-limit", "5"
        ], check=True, capture_output=True)
        
        df2 = pd.read_csv("step2_pages.csv")
        pages_found = len(df2)
        summary["steps"].append({
            "name": "Step 2: Page Discovery",
            "pagesDiscovered": pages_found
        })
        
        # Step 3: Content Collection
        print("Running Step 3: Content Collection...")
        subprocess.run([
            "python3", "step3.py",
            "--input", "step2_pages.csv",
            "--output", "step3_content.csv"
        ], check=True, capture_output=True)
        
        df3 = pd.read_csv("step3_content.csv")
        emails_found = df3['email_count'].sum() if 'email_count' in df3.columns else 0
        summary["steps"].append({
            "name": "Step 3: Content Collection",
            "emailsFound": int(emails_found)
        })
        
        # Step 4: LLM Parsing
        print("Running Step 4: LLM Parsing...")
        subprocess.run([
            "python3", "step4.py",
            "--input", "step3_content.csv",
            "--output", "step4_parsed_contacts.csv",
            "--api-key", os.getenv("OPENAI_API_KEY", ""),
            "--model", "gpt-3.5-turbo"
        ], check=True, capture_output=True)
        
        df4 = pd.read_csv("step4_parsed_contacts.csv")
        contacts_extracted = len(df4)
        summary["steps"].append({
            "name": "Step 4: LLM Parsing",
            "contactsExtracted": contacts_extracted
        })
        
        # Step 5: Final Compilation
        print("Running Step 5: Final Compilation...")
        subprocess.run([
            "python3", "step5.py",
            "--input", "step4_parsed_contacts.csv",
            "--output", "step5_final_contacts.csv"
        ], check=True, capture_output=True)
        
        df5 = pd.read_csv("step5_final_contacts.csv")
        final_contacts = len(df5)
        summary["totalContacts"] = final_contacts
        summary["steps"].append({
            "name": "Step 5: Final Compilation",
            "finalContacts": final_contacts
        })
        
    except subprocess.CalledProcessError as e:
        summary["status"] = "error"
        summary["error"] = f"Step failed: {e.stderr.decode() if e.stderr else str(e)}"
    except Exception as e:
        summary["status"] = "error"
        summary["error"] = str(e)
    
    return summary

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route("/run-pipeline", methods=["POST", "OPTIONS"])
def run_pipeline():
    """Run the full pipeline and return summary with CSV data"""
    if request.method == "OPTIONS":
        # Handle CORS preflight
        response = jsonify({})
        response.headers.add("Access-Control-Allow-Origin", "*")
        response.headers.add("Access-Control-Allow-Headers", "Content-Type")
        response.headers.add("Access-Control-Allow-Methods", "POST, OPTIONS")
        return response, 200
    
    try:
        data = request.get_json() or {}
        max_schools = data.get("maxSchools", 5)
        
        # Run pipeline (this will take 2-5 minutes)
        summary = run_pipeline_steps(max_schools=max_schools)
        
        # Read the final CSV file and include it in response
        csv_data = None
        if summary.get("status") == "success" and os.path.exists("step5_final_contacts.csv"):
            with open("step5_final_contacts.csv", "r", encoding="utf-8") as f:
                csv_data = f.read()
            summary["csvData"] = csv_data
            summary["csvFilename"] = f"school_contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Add CORS headers to response
        response = jsonify(summary)
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200
    except Exception as e:
        error_response = jsonify({
            "status": "error",
            "error": str(e)
        })
        error_response.headers.add("Access-Control-Allow-Origin", "*")
        return error_response, 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

