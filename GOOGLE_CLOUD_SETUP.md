# Google Cloud Setup Guide

Complete step-by-step guide to set up Google Cloud for the school scraper pipeline.

## Prerequisites

- Google Cloud account (sign up at https://cloud.google.com)
- Billing account enabled (required for Cloud Run)
- `gcloud` CLI installed (or use Cloud Console)

---

## Step 1: Create/Select Google Cloud Project

### Option A: Using Cloud Console (Web UI)
1. Go to https://console.cloud.google.com
2. Click project dropdown at top
3. Click "New Project"
4. Enter project name: `school-scraper` (or your preferred name)
5. Click "Create"
6. Wait for project creation (30 seconds)

### Option B: Using gcloud CLI
```bash
gcloud projects create school-scraper --name="School Scraper"
gcloud config set project school-scraper
```

**Note your Project ID** - you'll need it later (e.g., `school-scraper-123456`)

---

## Step 2: Enable Required APIs

### Using Cloud Console:
1. Go to "APIs & Services" → "Library"
2. Search and enable each of these:
   - **Cloud Run API**
   - **Cloud Storage API**
   - **Cloud Build API**
   - **Artifact Registry API** (or Container Registry API)

### Using gcloud CLI:
```bash
gcloud services enable run.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable artifactregistry.googleapis.com
```

---

## Step 3: Create Cloud Storage Bucket

### Using Cloud Console:
1. Go to "Cloud Storage" → "Buckets"
2. Click "Create Bucket"
3. Bucket name: `school-scraper-data` (must be globally unique - add random suffix if needed)
4. Location type: **Region**
5. Region: **us-central1** (or your preferred region)
6. Storage class: **Standard**
7. Access control: **Uniform**
8. Click "Create"

### Using gcloud CLI:
```bash
# Create bucket
gsutil mb -p school-scraper -l us-central1 gs://school-scraper-data

# Set lifecycle policy (optional - auto-delete files after 7 days)
cat > lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 7}
      }
    ]
  }
}
EOF
gsutil lifecycle set lifecycle.json gs://school-scraper-data
```

**Note your bucket name** - you'll need it for the `BUCKET_NAME` environment variable

---

## Step 4: Set Up Service Account & Permissions

### Create Service Account:
1. Go to "IAM & Admin" → "Service Accounts"
2. Click "Create Service Account"
3. Name: `school-scraper-runner`
4. Description: "Service account for Cloud Run pipeline"
5. Click "Create and Continue"
6. Grant roles:
   - **Cloud Run Invoker** (if using Cloud Functions/Workflows)
   - **Storage Object Admin** (to read/write Cloud Storage)
7. Click "Continue" → "Done"

### Create and Download Key:
1. Click on the service account you just created
2. Go to "Keys" tab
3. Click "Add Key" → "Create new key"
4. Choose **JSON**
5. Click "Create" - key will download automatically
6. **Save this file securely** - you'll need it for local testing

**For Cloud Run**: You don't need to download the key - Cloud Run will use the service account automatically.

---

## Step 5: Set Up Authentication (for Local Testing)

### Option A: Use Application Default Credentials (Recommended)
```bash
# Authenticate
gcloud auth application-default login

# Set default project
gcloud config set project school-scraper
```

### Option B: Use Service Account Key
```bash
# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

---

## Step 6: Prepare Environment Variables

You'll need these values for Cloud Run deployment:

1. **GOOGLE_PLACES_API_KEY** - Your Google Places API key
2. **OPENAI_API_KEY** - Your OpenAI API key
3. **BUCKET_NAME** - Your Cloud Storage bucket name (e.g., `school-scraper-data`)

**Keep these ready** - you'll set them when deploying to Cloud Run.

---

## Step 7: Build and Push Docker Image

### 7.1 Build Container Image

**Option A: Using Cloud Build (Recommended)**
```bash
# From your project root directory
gcloud builds submit --tag gcr.io/PROJECT_ID/school-scraper

# Replace PROJECT_ID with your actual project ID
# Example: gcloud builds submit --tag gcr.io/school-scraper-123456/school-scraper
```

**Option B: Using Docker (Local Build)**
```bash
# Build locally
docker build -t gcr.io/PROJECT_ID/school-scraper .

# Push to Container Registry
docker push gcr.io/PROJECT_ID/school-scraper
```

### 7.2 Verify Image
```bash
# List images
gcloud container images list

# Should see: gcr.io/PROJECT_ID/school-scraper
```

---

## Step 8: Deploy to Cloud Run

### Using Cloud Console:
1. Go to "Cloud Run" → "Create Service"
2. **Service name**: `school-scraper-api`
3. **Region**: `us-central1` (same as bucket)
4. **Authentication**: Allow unauthenticated invocations (for Vercel access)
5. Click "Next"
6. **Container image**: Click "Select" → Choose your image
7. **Container port**: `8080`
8. **CPU allocation**: 2 vCPU
9. **Memory**: 4 GiB
10. **Timeout**: 3600 seconds (1 hour - per county)
11. **Max instances**: 1 (for now)
12. **Min instances**: 0
13. Click "Container" tab:
    - **Environment variables**:
      - `GOOGLE_PLACES_API_KEY` = `your-key-here`
      - `OPENAI_API_KEY` = `your-key-here`
      - `BUCKET_NAME` = `school-scraper-data`
      - `PORT` = `8080`
14. Click "Create"

### Using gcloud CLI:
```bash
gcloud run deploy school-scraper-api \
  --image gcr.io/PROJECT_ID/school-scraper \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 1 \
  --min-instances 0 \
  --set-env-vars GOOGLE_PLACES_API_KEY=your-key-here,OPENAI_API_KEY=your-key-here,BUCKET_NAME=school-scraper-data,PORT=8080
```

**Replace:**
- `PROJECT_ID` with your actual project ID
- `your-key-here` with your actual API keys

---

## Step 9: Get Cloud Run URL

### Using Cloud Console:
1. Go to "Cloud Run" → Your service
2. Copy the **URL** (e.g., `https://school-scraper-api-xxxxx-uc.a.run.app`)

### Using gcloud CLI:
```bash
gcloud run services describe school-scraper-api \
  --region us-central1 \
  --format 'value(status.url)'
```

**This URL is your `NEXT_PUBLIC_API_URL` for Vercel!**

---

## Step 10: Test the Deployment

### Test Health Endpoint:
```bash
curl https://YOUR-CLOUD-RUN-URL/health
# Should return: {"status":"healthy"}
```

### Test Pipeline Endpoint (Small Test):
```bash
curl -X POST https://YOUR-CLOUD-RUN-URL/run-pipeline \
  -H "Content-Type: application/json" \
  -d '{"state": "delaware", "type": "school"}'

# Should return: {"status":"started","runId":"...","message":"Pipeline started"}
```

---

## Step 11: Set Up Vercel Environment Variable

1. Go to your Vercel project dashboard
2. Go to "Settings" → "Environment Variables"
3. Add new variable:
   - **Name**: `NEXT_PUBLIC_API_URL`
   - **Value**: Your Cloud Run URL (from Step 9)
   - **Environment**: Production, Preview, Development (check all)
4. Click "Save"
5. **Redeploy** your frontend for the change to take effect

---

## Step 12: Monitor and Debug

### View Logs:
```bash
# Stream logs
gcloud run services logs read school-scraper-api --region us-central1 --follow

# Or use Cloud Console:
# Cloud Run → Your service → "Logs" tab
```

### View Metrics:
- Go to Cloud Run → Your service → "Metrics" tab
- Monitor: Requests, Latency, Errors, CPU/Memory usage

### Check Cloud Storage:
```bash
# List files in bucket
gsutil ls gs://school-scraper-data/runs/

# Or use Cloud Console:
# Cloud Storage → Your bucket → Browse
```

---

## Troubleshooting

### Issue: "Permission denied" errors
**Solution**: Ensure service account has "Storage Object Admin" role

### Issue: "Bucket not found"
**Solution**: Check bucket name matches `BUCKET_NAME` environment variable

### Issue: "API key invalid"
**Solution**: Verify API keys are set correctly in Cloud Run environment variables

### Issue: Container timeout
**Solution**: Increase timeout or process fewer counties per run

### Issue: "Module not found" errors
**Solution**: Ensure all Python files are copied in Dockerfile

---

## Cost Monitoring

### Set Up Budget Alerts:
1. Go to "Billing" → "Budgets & alerts"
2. Click "Create Budget"
3. Set budget amount (e.g., $100/month)
4. Set alert thresholds (e.g., 50%, 90%, 100%)
5. Add email notifications

### View Current Costs:
- Go to "Billing" → "Reports"
- Filter by service: Cloud Run, Cloud Storage, Cloud Build

---

## Quick Reference Commands

```bash
# Set project
gcloud config set project PROJECT_ID

# Build and deploy (all in one)
gcloud builds submit --tag gcr.io/PROJECT_ID/school-scraper && \
gcloud run deploy school-scraper-api \
  --image gcr.io/PROJECT_ID/school-scraper \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600

# View logs
gcloud run services logs read school-scraper-api --region us-central1 --follow

# Update environment variables
gcloud run services update school-scraper-api \
  --region us-central1 \
  --update-env-vars KEY=value

# Get service URL
gcloud run services describe school-scraper-api \
  --region us-central1 \
  --format 'value(status.url)'
```

---

## Next Steps After Setup

1. ✅ Test with small state (Delaware - 3 counties)
2. ✅ Verify progress tracking works
3. ✅ Verify CSV downloads work
4. ✅ Test with medium state (Iowa - 99 counties)
5. ✅ Monitor costs and performance
6. ✅ Optimize based on results

---

## Estimated Setup Time

- **Steps 1-6**: 30-45 minutes
- **Step 7 (Build)**: 5-10 minutes
- **Step 8 (Deploy)**: 5 minutes
- **Step 9-12**: 10 minutes

**Total**: ~1 hour for complete setup

