# Railway Environment Variables Setup

## Step-by-Step Instructions

### Method 1: Service-Level Variables (Recommended)

1. **Go to Railway Dashboard**
   - Visit [railway.app](https://railway.app)
   - Log in and select your project

2. **Select Your Service**
   - Click on your deployed service (the one running `api.py`)

3. **Open Variables Tab**
   - Click on the **"Variables"** tab in the service view
   - You'll see a list of current variables (if any)

4. **Add Variables**
   - Click **"+ New Variable"** button
   - Add each variable one by one:

   **Variable 1:**
   - **Name:** `GOOGLE_PLACES_API_KEY`
   - **Value:** `your-google-places-api-key-here`
   - Click **"Add"**

   **Variable 2:**
   - **Name:** `OPENAI_API_KEY`
   - **Value:** `your-openai-api-key-here`
   - Click **"Add"**

   **Variable 3:**
   - **Name:** `HUNTER_IO_API_KEY`
   - **Value:** `your-hunter-io-api-key-here`
   - Click **"Add"**
   - *Note: Optional - Hunter.io email enrichment will be skipped if not set*

   **Variable 4 (Optional):**
   - **Name:** `PORT`
   - **Value:** `8080`
   - Click **"Add"**
   - *Note: Railway usually sets PORT automatically, but you can set it explicitly*

5. **Redeploy (if needed)**
   - Railway will automatically redeploy when you add variables
   - Or click **"Redeploy"** button if it doesn't auto-redeploy

### Method 2: Project-Level Shared Variables

If you have multiple services sharing the same variables:

1. **Go to Project Settings**
   - Click on your project name (top left)
   - Select **"Settings"** from the sidebar

2. **Navigate to Shared Variables**
   - Click **"Shared Variables"** in the settings menu
   - Select your environment (e.g., "production")

3. **Add Variables**
   - Click **"+ New Variable"**
   - Add the same variables as above
   - These will be available to all services in the project

## Required Variables

| Variable Name | Description | Example |
|--------------|-------------|---------|
| `GOOGLE_PLACES_API_KEY` | Your Google Places API key | `AIzaSy...` |
| `OPENAI_API_KEY` | Your OpenAI API key | `sk-proj-...` |
| `HUNTER_IO_API_KEY` | Your Hunter.io API key (optional) | `your-key...` |
| `PORT` | Server port (optional, Railway sets this) | `8080` |

## Verification

After adding variables:

1. **Check Logs**
   - Go to your service → **"Deployments"** tab
   - Click on the latest deployment
   - Click **"View Logs"**
   - Look for any errors about missing environment variables

2. **Test Health Endpoint**
   ```bash
   curl https://your-app.up.railway.app/health
   ```
   Should return: `{"status":"healthy"}`

3. **Test Pipeline**
   - Use your Vercel frontend to start a test run
   - Or use curl:
   ```bash
   curl -X POST https://your-app.up.railway.app/run-pipeline \
     -H "Content-Type: application/json" \
     -d '{"state": "delaware", "type": "school"}'
   ```

## Troubleshooting

**Issue: Variables not showing up**
- Make sure you're adding them to the correct service
- Check that you clicked "Add" after entering each variable
- Try redeploying the service

**Issue: Service crashes on startup**
- Check logs for "environment variable not found" errors
- Verify variable names are exact (case-sensitive)
- Ensure no extra spaces in variable names or values

**Issue: API calls failing**
- Verify API keys are correct and active
- Check Google Places API is enabled in your Google Cloud project
- Ensure OpenAI API key has credits available

