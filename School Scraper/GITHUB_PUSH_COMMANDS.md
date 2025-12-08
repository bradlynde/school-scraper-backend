# GitHub Push Commands

## Summary of Changes

✅ **Removed Google Cloud dependencies:**
- Removed `google-cloud-storage` from requirements.txt
- Removed `CloudStorageHelper` usage from api.py
- Updated to use local file storage (`/tmp/runs/`)

✅ **Updated for Railway:**
- Created `railway.json` configuration
- Created `.railwayignore` file
- Updated Dockerfile to use `python3`
- Created `RAILWAY_DEPLOYMENT.md` guide

✅ **Fixed API:**
- Updated to use single output file (no separate with/without emails)
- Simplified county processing (no HTTP self-requests needed)
- Uses background threads for sequential county processing

## Files Ready to Push

- All pipeline steps (step1.py through step11.py)
- Pipeline.py (main orchestrator)
- external_services/api.py (Flask API)
- external_services/Dockerfile
- docs/requirements.txt (without GCS)
- assets/data/state_counties/*.txt (all state county files)
- railway.json, .railwayignore, RAILWAY_DEPLOYMENT.md

## Commands to Run

```bash
cd "/Users/koenullrich/Documents/Altira/Cursor/School Scraper"

# Review what will be committed
git status

# Commit all changes
git commit -m "Migrate to Railway: Remove GCS dependencies, update for Railway deployment"

# Push to GitHub
git push origin main
```

## After Pushing

1. **Deploy to Railway:**
   - Go to railway.app
   - New Project → Deploy from GitHub
   - Select your repo
   - Add environment variables (GOOGLE_PLACES_API_KEY, OPENAI_API_KEY)
   - Deploy!

2. **Update Vercel:**
   - Add `NEXT_PUBLIC_API_URL` = Your Railway URL
   - Redeploy frontend

## Verify Everything Works

The API expects a POST request to `/run-pipeline` with:
```json
{
  "state": "delaware",
  "type": "school"
}
```

It will return:
```json
{
  "status": "started",
  "runId": "uuid-here",
  "message": "Pipeline started"
}
```

Then poll `/pipeline-status/<runId>` to get progress.

