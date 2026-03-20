# Email (Resend HTTP) — test without running a state

Production uses **`external_services/notify.py`** → **Resend** `POST https://api.resend.com/emails`.

**Required:** `RESEND_API_KEY`, `NOTIFY_EMAIL`  
**Optional:** `NOTIFY_FROM` (default `onboarding@resend.dev`), `NOTIFY_ON_RUN_COMPLETE=false` to disable all sends

## 1. Authenticated API (deployed backend)

```bash
curl -sS -X POST "$API_BASE/notify/test-email" \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json"
```

Subject includes `[TEST] Church Scraper` or `[TEST] School Scraper` per service.

## 2. Local script (real Resend send)

Same HTTP API as `notify.py`:

```bash
export RESEND_API_KEY="re_..." NOTIFY_EMAIL="you@example.com"
# optional: NOTIFY_FROM="onboarding@resend.dev"
python3 npsa-scraper/scripts/test_resend.py
```

## 3. Unit tests (mocked, no network)

```bash
cd "npsa-scraper/Church Contact Scraper" && python3 -m unittest tests.test_notify_resend -v
cd "npsa-scraper/School Contact Scraper" && python3 -m unittest tests.test_notify_resend -v
```
