# 405 Error Troubleshooting Checklist

## Critical Issues Found

### 1. **Railway Configuration Conflict** ⚠️ CRITICAL
   - **Issue**: `railway.json` has `startCommand: "python3 external_services/api.py"` 
   - **Problem**: This overrides the Dockerfile CMD that uses Gunicorn
   - **Fix**: Remove the `startCommand` from `railway.json` OR update it to use Gunicorn
   - **Check**: Railway dashboard → Settings → Deploy → Start Command (should be empty or use Gunicorn)

### 2. **Frontend API URL** ⚠️ CRITICAL
   - **Issue**: Frontend defaults to old Google Cloud Run URL if env var not set
   - **Check**: Vercel dashboard → Settings → Environment Variables
   - **Verify**: `NEXT_PUBLIC_API_URL` = `https://npsa-scraper.up.railway.app` (no trailing slash)
   - **Test**: Check browser console Network tab - what URL is it actually calling?

## Railway Backend Checks

### 3. **Railway Service URL**
   - **Check**: Railway dashboard → Your service → Settings → Networking
   - **Verify**: Public domain is set (e.g., `npsa-scraper.up.railway.app`)
   - **Note**: Railway URL might be different after transfer - check the actual URL

### 4. **Railway Environment Variables**
   - **Check**: Railway dashboard → Variables tab
   - **Required Variables**:
     - `GOOGLE_PLACES_API_KEY` ✓
     - `OPENAI_API_KEY` ✓
     - `HUNTER_IO_API_KEY` (optional) ✓
     - `PORT` (auto-set by Railway, don't override)

### 5. **Railway Deployment Status**
   - **Check**: Railway dashboard → Deployments tab
   - **Verify**: Latest deployment is successful (green checkmark)
   - **Check logs**: Click on latest deployment → View logs
   - **Look for**: 
     - "Starting Flask server on port..." or Gunicorn startup messages
     - Any errors during startup
     - Routes being registered

### 6. **Railway Root Directory**
   - **Check**: Railway dashboard → Settings → Root Directory
   - **Should be**: Empty (blank) or `.` (uses repo root)
   - **NOT**: `School Scraper` or `npsa-scraper`

### 7. **Railway Build Configuration**
   - **Check**: Railway dashboard → Settings → Build
   - **Dockerfile Path**: Should be `Dockerfile` (at repo root)
   - **Build Command**: Should be empty (uses Dockerfile)

## API Endpoint Testing

### 8. **Test Health Endpoint**
   ```bash
   curl https://npsa-scraper.up.railway.app/health
   ```
   - **Expected**: `{"status": "healthy"}`
   - **If 405**: Railway routing issue or wrong URL

### 9. **Test Root Endpoint**
   ```bash
   curl https://npsa-scraper.up.railway.app/
   ```
   - **Expected**: JSON with available endpoints
   - **If 405**: Method not allowed (should work with GET)

### 10. **Test POST Endpoint (from terminal)**
   ```bash
   curl -X POST https://npsa-scraper.up.railway.app/run-pipeline \
     -H "Content-Type: application/json" \
     -d '{"state": "delaware", "type": "school"}'
   ```
   - **Expected**: `{"status": "started", "runId": "...", "message": "Pipeline started"}`
   - **If 405**: Method not allowed - check Railway proxy/routing

## Vercel Frontend Checks

### 11. **Vercel Environment Variables**
   - **Check**: Vercel dashboard → Settings → Environment Variables
   - **Verify**: `NEXT_PUBLIC_API_URL` = `https://npsa-scraper.up.railway.app`
   - **Note**: Must redeploy after adding/changing env vars

### 12. **Vercel Root Directory**
   - **Check**: Vercel dashboard → Settings → Root Directory
   - **Should be**: `frontend` or `npsa-scraper/frontend` (depending on repo structure)

### 13. **Vercel Deployment**
   - **Check**: Vercel dashboard → Deployments
   - **Verify**: Latest deployment includes env var changes
   - **Redeploy**: If env vars changed, trigger a new deployment

## Code-Level Checks

### 14. **Railway.json Start Command**
   - **File**: `railway.json` line 8
   - **Current**: `"startCommand": "python3 external_services/api.py"`
   - **Issue**: This overrides Dockerfile CMD
   - **Fix**: Remove this line OR change to Gunicorn command

### 15. **Dockerfile CMD**
   - **File**: `Dockerfile` line 36
   - **Current**: Uses Gunicorn (correct)
   - **Verify**: Command matches Railway's expected format

### 16. **CORS Configuration**
   - **File**: `api.py` line 47
   - **Check**: CORS allows all origins (`"origins": "*"`)
   - **Verify**: Methods include `["GET", "POST", "OPTIONS"]`

## Browser/Network Checks

### 17. **Browser Console**
   - **Check**: Open browser DevTools → Console tab
   - **Look for**: CORS errors, network errors, 405 errors
   - **Check**: Network tab → Find the failed request → Check:
     - Request URL (is it correct?)
     - Request Method (should be POST)
     - Response status (405?)
     - Response headers

### 18. **Network Request Details**
   - **Open**: Browser DevTools → Network tab
   - **Trigger**: Try to start a scrape
   - **Check**: The `/run-pipeline` request
     - **URL**: Should be `https://npsa-scraper.up.railway.app/run-pipeline`
     - **Method**: Should be `POST`
     - **Headers**: Should include `Content-Type: application/json`
     - **Response**: Check status code and error message

## Quick Fixes to Try

### Fix 1: Remove startCommand from railway.json
```json
{
  "$schema": "https://railway.app/railway.schema.json",
  "build": {
    "builder": "DOCKERFILE",
    "dockerfilePath": "Dockerfile"
  },
  "deploy": {
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 10
  }
}
```

### Fix 2: Verify Railway URL
- Check Railway dashboard for the actual public URL
- It might not be `npsa-scraper.up.railway.app` after transfer
- Update Vercel env var with correct URL

### Fix 3: Force Railway Redeploy
- Railway dashboard → Deployments → Click "Redeploy"
- Or push an empty commit: `git commit --allow-empty -m "Force redeploy" && git push`

## Most Likely Causes (in order)

1. **Railway.json startCommand overriding Dockerfile** (most likely)
2. **Wrong Railway URL in Vercel env vars**
3. **Railway service not properly deployed/started**
4. **CORS preflight OPTIONS request failing**

