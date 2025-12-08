# School Contact Scraper Pipeline

A 7-step Python pipeline for discovering Christian schools and extracting decision-maker contact information (Principals, IT Directors, Facilities Directors, Security Directors, etc.) for security grant applications.

## 🎯 Overview

This pipeline automates the discovery and extraction of school administrator contacts from school websites. It's designed for organizations like [Nonprofit Security Advisors](https://www.nonprofitsecurityadvisors.com/) that need to identify decision-makers for security grant applications.

### Pipeline Flow

```
Step 1: School Discovery (Google Places API, random counties, capped calls)
    ↓
Step 2: School Filtering (remove churches/camps, enforce Texas-only)
    ↓
Step 3: Page Discovery (prioritized staff/admin pages)
    ↓
Step 4: Content Collection (Beautiful Soup + Selenium)
    ↓
Step 5: Contact Extraction (OpenAI GPT-4o-mini - extracts ALL contacts, no filtering)
    ↓
Step 6: Title Filtering (OpenAI GPT-4o-mini - filters by administrative/leadership roles)
    ↓
Step 7: Final Validation & Compilation
```

### Step Summaries

1. **Step 1 – School Discovery**: Randomly cycles through all 254 Texas counties (shuffled) and makes up to 25 Google Places API calls to gather every Christian school candidate it can find.
2. **Step 2 – Texas Filter**: Removes out-of-state hits, churches/camps, and anything that doesn't clearly identify as a school or academy.
3. **Step 3 – Page Discovery**: Crawls each school website, scoring and prioritizing staff/admin URLs, and caps the output to the top 3 high-value internal pages per school.
4. **Step 4 – Content Collection**: Downloads HTML for each prioritized page using requests first, then Selenium (click + hover) when emails are hidden behind interactions.
5. **Step 5 – Contact Extraction**: Extracts ALL contacts from HTML using GPT-4o-mini. No filtering - extracts everyone found (teachers, coaches, administrators, etc.).
6. **Step 6 – Title Filtering**: Uses GPT-4o-mini to filter contacts by title, keeping only administrative/leadership roles (principals, directors, superintendents, etc.) and excluding teachers, coaches, board members, etc.
7. **Step 7 – Final Compilation**: Validates names/emails, drops placeholders, deduplicates by email/name, assigns confidence scores, and produces final outreach-ready CSVs (emails + enrichment-ready leads).

## 📋 Requirements

### Python Dependencies

```bash
pip install pandas requests beautifulsoup4 selenium openai
```

Or use the project's `docs/requirements.txt`:

```
pandas>=2.0.0
requests>=2.31.0
beautifulsoup4>=4.12.0
selenium>=4.15.0
openai>=1.0.0
urllib3>=2.0.0
```

### API Keys Required

1. **Google Places API Key** - For Step 1 (school discovery)
   - Get from: [Google Cloud Console](https://console.cloud.google.com/)
   - Enable: Places API (New)

2. **OpenAI API Key** - For Step 5 (LLM parsing)
   - Get from: [OpenAI Platform](https://platform.openai.com/api-keys)

### System Requirements

- Python 3.9+
- Chrome/Chromium browser (for Selenium)
- ChromeDriver (automatically managed by Selenium 4+)

## 🚀 Quick Start

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd school-contact-scraper
```

### 2. Install Dependencies

```bash
pip install -r docs/requirements.txt
```

### 3. Run Pipeline

#### Step 1: Discover Schools (Random counties, capped API calls)

```bash
python3 step1.py \
  --api-key YOUR_GOOGLE_PLACES_API_KEY \
  --state Texas \
  --global-max-api-calls 25 \
  --output step1_schools.csv
```

**Options:**
- `--global-max-api-calls`: Absolute cap on Places API costs (default: 25 calls)
- `--max-search-terms`: Optional limit per county
- `--max-counties`: Optional hard cap on counties (default: all counties, randomized)

#### Step 2: Filter Non-Schools / Non-Texas

```bash
python3 step2.py \
  --input step1_schools.csv \
  --output step2_schools_filtered.csv
```

**Features:**
- Drops churches, camps, ministries unless they clearly contain a school keyword
- Enforces Texas-only by checking Google address components + formatted addresses

#### Step 3: Discover High-Value Pages

```bash
python3 step3.py \
  --input step2_schools_filtered.csv \
  --output step3_pages.csv \
  --max-pages-per-school 3 \
  --top-pages-limit 3 \
  --max-depth 3
```

**Features:**
- Prioritizes staff/admin/leadership URLs via scoring system
- Stops after 3 quality pages per school to control downstream costs

#### Step 4: Collect Content (Requests + Selenium)

```bash
python3 step4.py \
  --input step3_pages.csv \
  --output step4_content.csv
```

**Features:**
- Requests for fast/static sites, Selenium fallback for interactive content
- Click/hover routines to reveal hidden emails
- Stores HTML + metadata for every processed page

#### Step 5: Parse with LLM (GPT-4o-mini)

```bash
python3 step5.py \
  --input step4_content.csv \
  --output step5_contacts_with_emails.csv \
  --output-no-emails step5_contacts_no_emails.csv \
  --api-key YOUR_OPENAI_API_KEY \
  --model gpt-4o-mini
```

**Features:**
- Strict prompt (zero guessing) that filters forbidden titles
- Produces separate CSVs for contacts with and without visible emails
- HTML reduction + chunking to control token usage

#### Step 6: Final Validation & Compilation

```bash
python3 step6.py \
  --input step5_contacts_with_emails.csv \
  --output step6_final_contacts.csv
```

Run again with `step5_contacts_no_emails.csv` to prep enrichment lists.

**Features:**
- Email/phone validation, name cleaning, deduplication
- Confidence scoring + optional copy to Downloads folder

## Output Files

- `step1_schools.csv` - Discovered schools (raw Places output)
- `step2_schools_filtered.csv` - Clean Texas-only schools
- `step3_pages.csv` - Prioritized staff/admin pages per school
- `step4_content.csv` - HTML content gathered via requests/Selenium
- `step5_contacts_with_emails.csv` - LLM contacts with visible emails
- `step5_contacts_no_emails.csv` - LLM contacts missing emails (enrichment queue)
- `step6_final_contacts.csv` - Final validated contacts (ready for outreach)

## 🎯 Target Titles (Step 5 Filtering)

The pipeline filters for these decision-maker roles:

- **Administration**: Principal, Superintendent, Assistant Principal, Head of School, Acting Principal, Vice Principal, School Administrator, etc.
- **Finance/Operations**: Director of Finance, Business Manager, CFO, Director of Operations, COO
- **IT**: IT Director, Technology Director, CTO, Chief Technology Officer
- **Facilities**: Facilities Director, Facilities Manager, Director of Facilities
- **Security**: Security Director, Security Manager, Director of Security, Chief Security Officer

**Excluded roles**: Athletic Director, Coach, Academic Dean, Teacher, Librarian, etc.

## 💰 Cost Optimization

### Actual Costs (Full Texas Run - 254 Counties)
Based on production run producing **432 unique contacts with emails**:

- **Google Places API**: $31.91
  - Used for school discovery across all Texas counties
  - Cost: ~$0.017 per request
  
- **OpenAI API (GPT-4o-mini)**: $3.70
  - Used for contact extraction and title filtering
  - Cost: ~$0.003 per rich staff page (after HTML reduction/chunking)
  
- **Hunter.io Email Enrichment**: $6.30
  - Used 252 credits at $25 per 1000 credits
  - Cost: $0.025 per credit (only charged for successful email finds)
  
**Total Cost**: $41.91 for full Texas run (432 contacts = **$0.097 per contact**)

### Cost Controls

**Google Places API:**
- Use `--max-counties`, `--max-search-terms`, `--global-max-api-calls`
- Example: 10 API calls = ~$0.17

**OpenAI API:**
- Limit pages per school (`--max-pages-per-school`)
- Chunk large pages, skip low-priority URLs
- Example: 100 pages ≈ $0.30 (input + output tokens)

**Hunter.io:**
- Only charged for successful email finds (1 credit per email found)
- Bulk API endpoint available for batch processing

### Projected Costs (Scaled Estimates)
- **5 schools**: ~$0.02 (LLM) + ~$0.17 (Places API) = **~$0.19**
- **100 schools**: ~$0.46 (LLM) + ~$1.70 (Places API) = **~$2.16**
- **Full state (Texas)**: $3.70 (LLM) + $31.91 (Places API) + $6.30 (Hunter.io) = **~$41.91**

## Deployment

### Google Cloud Setup

1. **Create a Cloud Function or Cloud Run service**

```bash
# Install gcloud CLI
gcloud init
gcloud auth login

# Create a new project
gcloud projects create school-scraper --name="School Scraper"

# Set the project
gcloud config set project school-scraper

# Enable required APIs
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable run.googleapis.com
```

2. **Deploy as Cloud Function** (recommended for scheduled runs)

```bash
# Create function
gcloud functions deploy school-scraper \
  --runtime python39 \
  --trigger-http \
  --entry-point main \
  --memory 2GB \
  --timeout 540s \
  --set-env-vars GOOGLE_PLACES_API_KEY=YOUR_KEY,OPENAI_API_KEY=YOUR_KEY
```

3. **Or deploy as Cloud Run** (for web interface)

```bash
# Build and deploy
gcloud run deploy school-scraper \
  --source . \
  --platform managed \
  --region us-central1 \
  --set-env-vars GOOGLE_PLACES_API_KEY=YOUR_KEY,OPENAI_API_KEY=YOUR_KEY
```

### Vercel Setup (Frontend Dashboard)

1. **Install Vercel CLI**

```bash
npm i -g vercel
```

2. **Deploy**

```bash
cd frontend  # If you have a frontend folder
vercel
```

3. **Environment Variables** (set in Vercel dashboard)
   - `NEXT_PUBLIC_API_URL` - Your Google Cloud function/run URL

## 🔧 Configuration

### Adjusting Scope

**For more contacts:**
```bash
# Step 2: Increase pages per school
--top-pages-limit 10  # Instead of 5

# Step 1: Process more schools
--max-counties 5  # Instead of 1
--max-api-calls 50  # Instead of 10
```

**For cost control:**
```bash
# Step 2: Reduce pages
--top-pages-limit 3  # Instead of 5

# Step 1: Limit API calls
--max-api-calls 5  # Very restrictive
```

### Customizing Target Titles

Edit `step5.py` - adjust the prompt or post-processing rules inside `LLMParser`:

```python
self.target_titles = [
    'principal', 'superintendent', 'it director', ...
]
```

## 🐛 Troubleshooting

### Selenium Issues
- Ensure Chrome/Chromium is installed
- ChromeDriver is auto-managed by Selenium 4+
- For headless mode, add to `step4.py`: `options.add_argument('--headless')`

### JSON Parse Errors
- Step 5 now includes automatic recovery for truncated responses
- Large pages (>20 emails) automatically get higher token limits

### API Rate Limits
- Google Places API: 100 requests/second (usually not an issue)
- OpenAI API: Rate limits depend on your tier
- Both steps include retry logic with exponential backoff

## 📈 Performance Metrics

Based on 5-school test:
- **Success Rate**: 60% of schools yield contacts
- **Pages per School**: 4.6 pages (with top-5 filter)
- **Email Collection**: 56.5% of pages have emails
- **Contact Extraction**: 84.6% of pages with emails yield contacts
- **Average Contacts per School**: 2.0 (3.3 for schools with contacts)

## 📝 License

[Add your license here]

## 🤝 Contributing

[Add contribution guidelines if needed]

## 📧 Support

For issues or questions, please open a GitHub issue.

