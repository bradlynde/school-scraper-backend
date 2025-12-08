# Railway Deployment Guide

## Quick Setup (5 minutes)

### Step 1: Sign up for Railway
1. Go to [railway.app](https://railway.app)
2. Sign up with GitHub (recommended for easy repo connection)

### Step 2: Create New Project
1. Click "New Project"
2. Select "Deploy from GitHub repo"
3. Select your repository
4. Railway will auto-detect the Dockerfile

### Step 3: Configure Environment Variables
In Railway dashboard, go to your service → Variables tab, add:

```
GOOGLE_PLACES_API_KEY=your-key-here
OPENAI_API_KEY=your-key-here
PORT=8080
```

### Step 4: Deploy
1. Railway will automatically build and deploy
2. Wait ~5 minutes for first build
3. Copy the generated URL (e.g., `https://your-app.up.railway.app`)

### Step 5: Update Vercel
1. Go to Vercel dashboard → Your project → Settings → Environment Variables
2. Add/Update: `NEXT_PUBLIC_API_URL` = Your Railway URL
3. Redeploy frontend

## Resource Configuration

### Recommended Settings:
- **Memory**: 4 GB (for Selenium/Chrome)
- **CPU**: 2 vCPU
- **Timeout**: No limit (Railway supports long-running processes)

### How to Set Resources:
1. In Railway dashboard → Your service → Settings
2. Scroll to "Resources"
3. Adjust Memory and CPU as needed

## Monitoring Costs

Railway Hobby Plan ($5/month) includes $5 usage credit:
- **CPU**: $0.00000772 per vCPU-second
- **Memory**: $0.00000386 per GB-second

**Example for 1 Illinois run (101 counties, ~20 min each):**
- CPU: 2 vCPU × 1,200s × 101 = 242,400 vCPU-seconds = $1.87
- Memory: 4 GB × 1,200s × 101 = 484,800 GB-seconds = $1.87
- **Total: $3.74** (covered by $5 monthly credit)

## Viewing Logs

1. In Railway dashboard → Your service
2. Click "Deployments" tab
3. Click on latest deployment
4. Click "View Logs"

Or use Railway CLI:
```bash
railway logs
```

## Troubleshooting

### Issue: Build fails
- Check logs in Railway dashboard
- Ensure Dockerfile is in root or `external_services/` directory
- Verify `requirements.txt` path is correct

### Issue: Service crashes
- Check logs for errors
- Verify environment variables are set
- Ensure memory is set to at least 4GB

### Issue: Timeout errors
- Railway doesn't have request timeouts for long-running processes
- If you see timeouts, check your code for blocking operations

### Issue: Can't connect from Vercel
- Verify Railway service is deployed and running
- Check CORS settings in `api.py` (should allow all origins)
- Verify `NEXT_PUBLIC_API_URL` is set correctly in Vercel

## Updating Code

Railway automatically redeploys when you push to GitHub:
1. Make changes locally
2. Commit and push to GitHub
3. Railway detects changes and redeploys automatically
4. Check deployment status in Railway dashboard

## Cost Monitoring

1. Go to Railway dashboard → Settings → Usage
2. View current month's usage
3. Set up billing alerts if needed

## Next Steps

1. ✅ Deploy to Railway
2. ✅ Set environment variables
3. ✅ Update Vercel with Railway URL
4. ✅ Test with a small state (Delaware)
5. ✅ Monitor costs and performance

