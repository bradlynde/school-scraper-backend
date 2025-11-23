# Quick Start: Google Cloud Setup

## Prerequisites
- Google Cloud account with billing enabled
- `gcloud` CLI installed (or use Cloud Console web UI)

---

## Step-by-Step Setup (30 minutes)

### 1. Create Project
```bash
gcloud projects create school-scraper --name="School Scraper"
gcloud config set project school-scraper
```
**Note your Project ID** (e.g., `school-scraper-123456`)

### 2. Enable APIs
```bash
gcloud services enable run.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```

### 3. Create Storage Bucket
```bash
# Create bucket (replace with unique name if needed)
gsutil mb -p school-scraper -l us-central1 gs://school-scraper-data

# Verify
gsutil ls gs://school-scraper-data
```
**Note your bucket name** (e.g., `school-scraper-data`)

### 4. Create Service Account
```bash
# Create service account
gcloud iam service-accounts create school-scraper-runner \
  --display-name="School Scraper Runner"

# Grant Storage permissions
gsutil iam ch serviceAccount:school-scraper-runner@school-scraper.iam.gserviceaccount.com:roles/storage.objectAdmin gs://school-scraper-data
```

### 5. Authenticate (for local testing)
```bash
gcloud auth application-default login
```

### 6. Build Container
```bash
# From your project root directory
cd /Users/koenullrich/Documents/Altira/Cursor

# Build and push to Container Registry
gcloud builds submit --tag gcr.io/school-scraper/school-scraper

# Replace 'school-scraper' with your actual PROJECT_ID
```

### 7. Deploy to Cloud Run
```bash
gcloud run deploy school-scraper-api \
  --image gcr.io/school-scraper/school-scraper \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 1 \
  --set-env-vars \
    GOOGLE_PLACES_API_KEY="YOUR_GOOGLE_KEY",\
    OPENAI_API_KEY="YOUR_OPENAI_KEY",\
    BUCKET_NAME="school-scraper-data",\
    PORT="8080"
```

**Replace:**
- `school-scraper` with your PROJECT_ID
- `YOUR_GOOGLE_KEY` with your actual Google Places API key
- `YOUR_OPENAI_KEY` with your actual OpenAI API key
- `school-scraper-data` with your actual bucket name

### 8. Get Service URL
```bash
gcloud run services describe school-scraper-api \
  --region us-central1 \
  --format 'value(status.url)'
```

**Copy this URL** - it's your `NEXT_PUBLIC_API_URL` for Vercel!

### 9. Test Deployment
```bash
# Test health endpoint
curl https://YOUR-URL/health

# Should return: {"status":"healthy"}
```

### 10. Set Vercel Environment Variable
1. Go to Vercel Dashboard → Your Project → Settings → Environment Variables
2. Add: `NEXT_PUBLIC_API_URL` = `https://YOUR-URL`
3. Redeploy frontend

---

## Using Cloud Console (Web UI)

If you prefer the web interface:

### 1. Create Project
- Go to https://console.cloud.google.com
- Click project dropdown → "New Project"
- Name: `school-scraper`
- Click "Create"

### 2. Enable APIs
- Go to "APIs & Services" → "Library"
- Enable: Cloud Run, Cloud Storage, Cloud Build

### 3. Create Bucket
- Go to "Cloud Storage" → "Buckets"
- Click "Create Bucket"
- Name: `school-scraper-data`
- Region: `us-central1`
- Click "Create"

### 4. Build Container
- Go to "Cloud Build" → "Triggers"
- Or use: "Cloud Build" → "History" → "Run" → "Build from source"

### 5. Deploy to Cloud Run
- Go to "Cloud Run" → "Create Service"
- Follow the deployment steps from `GOOGLE_CLOUD_SETUP.md`

---

## Verify Everything Works

### Test Health:
```bash
curl https://YOUR-URL/health
```

### Test Pipeline (Small):
```bash
curl -X POST https://YOUR-URL/run-pipeline \
  -H "Content-Type: application/json" \
  -d '{"state": "delaware", "type": "school"}'
```

Should return: `{"status":"started","runId":"...","message":"Pipeline started"}`

### Check Status:
```bash
curl https://YOUR-URL/pipeline-status/RUN_ID
```

---

## Common Issues

**"Permission denied"**: Service account needs Storage Object Admin role
**"Bucket not found"**: Check `BUCKET_NAME` env var matches bucket name
**"Module not found"**: Ensure all files are in Docker image (check Dockerfile)
**"Timeout"**: Increase Cloud Run timeout or process fewer counties

---

## View Logs

```bash
# Stream logs
gcloud run services logs read school-scraper-api \
  --region us-central1 \
  --follow
```

---

## Update Environment Variables

```bash
gcloud run services update school-scraper-api \
  --region us-central1 \
  --update-env-vars KEY=value
```

---

## Cost Monitoring

- Go to "Billing" → "Budgets & alerts"
- Create budget with alerts at 50%, 90%, 100%

---

**You're ready to deploy!** 🚀

