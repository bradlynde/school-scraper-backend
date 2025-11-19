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

def run_pipeline_steps(max_schools=100):
    """
    Run all 5 pipeline steps and return summary
    """
    summary = {
        "status": "success",
        "steps": [],
        "totalContacts": 0,
        "totalContactsNoEmails": 0,
        "schoolsFound": 0,
        "runId": datetime.now().isoformat()
    }
    
    try:
        # Step 1: School Discovery - Search multiple random counties to get target schools
        print("Running Step 1: School Discovery...")
        subprocess.run([
            "python3", "step1.py",
            "--api-key", os.getenv("GOOGLE_PLACES_API_KEY", ""),
            "--state", "Texas",
            "--multiple-random-counties", str(max_schools),
            "--max-search-terms", "5",  # More search terms per county for better coverage
            "--max-api-calls", "500",  # Increased API call limit for 100 schools (20 counties × 5 terms × 5 = 500 max)
            "--output", "step1_schools.csv"
        ], check=True, capture_output=True)
        
        # Read Step 1 results
        df1 = pd.read_csv("step1_schools.csv")
        schools_found = len(df1)
        
        summary["schoolsFound"] = schools_found
        summary["steps"].append({
            "name": "Step 1: School Discovery",
            "schoolsFound": schools_found
        })
        
        # Step 2: Page Discovery
        print("Running Step 2: Page Discovery...")
        subprocess.run([
            "python3", "step2.py",
            "--input", "step1_schools.csv",
            "--output", "step2_pages.csv",
            "--max-pages-per-school", "50",  # Increased for better coverage
            "--top-pages-limit", "5"  # Keep top 5 pages per school
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
            "--output", "step4_contacts_with_emails.csv",
            "--output-no-emails", "step4_contacts_no_emails.csv",
            "--api-key", os.getenv("OPENAI_API_KEY", ""),
            "--model", "gpt-4o-mini"
        ], check=True, capture_output=True)
        
        # Read both CSV files from Step 4
        contacts_with_emails = 0
        contacts_without_emails = 0
        if os.path.exists("step4_contacts_with_emails.csv"):
            df4_with = pd.read_csv("step4_contacts_with_emails.csv")
            contacts_with_emails = len(df4_with)
        if os.path.exists("step4_contacts_no_emails.csv"):
            df4_without = pd.read_csv("step4_contacts_no_emails.csv")
            contacts_without_emails = len(df4_without)
        
        summary["steps"].append({
            "name": "Step 4: LLM Parsing",
            "contactsWithEmails": contacts_with_emails,
            "contactsWithoutEmails": contacts_without_emails
        })
        
        # Step 5: Final Compilation (process contacts WITH emails)
        print("Running Step 5: Final Compilation...")
        csv_output_path = "step5_final_contacts.csv"
        csv_no_emails_path = "step5_final_contacts_no_emails.csv"
        
        # Process contacts with emails
        if os.path.exists("step4_contacts_with_emails.csv"):
            subprocess.run([
                "python3", "step5.py",
                "--input", "step4_contacts_with_emails.csv",
                "--output", csv_output_path
            ], check=True, capture_output=True)
        
        # Process contacts without emails (for enrichment)
        if os.path.exists("step4_contacts_no_emails.csv"):
            subprocess.run([
                "python3", "step5.py",
                "--input", "step4_contacts_no_emails.csv",
                "--output", csv_no_emails_path
            ], check=True, capture_output=True)
        
        # Read final results
        final_contacts_with_emails = 0
        final_contacts_without_emails = 0
        if os.path.exists(csv_output_path):
            df5 = pd.read_csv(csv_output_path)
            final_contacts_with_emails = len(df5)
        if os.path.exists(csv_no_emails_path):
            df5_no_emails = pd.read_csv(csv_no_emails_path)
            final_contacts_without_emails = len(df5_no_emails)
        
        summary["totalContacts"] = final_contacts_with_emails
        summary["totalContactsNoEmails"] = final_contacts_without_emails
        summary["steps"].append({
            "name": "Step 5: Final Compilation",
            "finalContactsWithEmails": final_contacts_with_emails,
            "finalContactsWithoutEmails": final_contacts_without_emails
        })
        
        # Store CSV paths for later retrieval
        summary["_csvPath"] = csv_output_path
        summary["_csvNoEmailsPath"] = csv_no_emails_path
        
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
        max_schools = data.get("maxSchools", 100)  # Default to 100 schools
        
        # Run pipeline (this will take 2-5 minutes)
        summary = run_pipeline_steps(max_schools=max_schools)
        
        # Read the final CSV files and include them in response
        if summary.get("status") == "success":
            # Read contacts WITH emails
            csv_path = summary.get("_csvPath", "step5_final_contacts.csv")
            if not os.path.exists(csv_path):
                csv_path = os.path.join(os.getcwd(), "step5_final_contacts.csv")
            
            if os.path.exists(csv_path):
                try:
                    with open(csv_path, "r", encoding="utf-8") as f:
                        csv_data = f.read()
                    csv_filename = f"school_contacts_with_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    summary["csvData"] = csv_data
                    summary["csvFilename"] = csv_filename
                    print(f"CSV file (with emails) included: {len(csv_data)} bytes")
                except Exception as e:
                    print(f"ERROR: Error reading CSV file: {e}")
            
            # Read contacts WITHOUT emails
            csv_no_emails_path = summary.get("_csvNoEmailsPath", "step5_final_contacts_no_emails.csv")
            if not os.path.exists(csv_no_emails_path):
                csv_no_emails_path = os.path.join(os.getcwd(), "step5_final_contacts_no_emails.csv")
            
            if os.path.exists(csv_no_emails_path):
                try:
                    with open(csv_no_emails_path, "r", encoding="utf-8") as f:
                        csv_no_emails_data = f.read()
                    csv_no_emails_filename = f"school_contacts_no_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    summary["csvNoEmailsData"] = csv_no_emails_data
                    summary["csvNoEmailsFilename"] = csv_no_emails_filename
                    print(f"CSV file (no emails) included: {len(csv_no_emails_data)} bytes")
                except Exception as e:
                    print(f"ERROR: Error reading CSV file (no emails): {e}")
        
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

