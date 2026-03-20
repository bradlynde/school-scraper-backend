# Email (SMTP) — test without running a state

Production uses **`external_services/notify.py`** with:

- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `NOTIFY_EMAIL`
- Optional: `NOTIFY_ON_RUN_COMPLETE=false` to turn off all sends (including test)

## 1. Authenticated API (deployed backend)

After deploy, `POST` with your JWT:

```bash
curl -sS -X POST "$API_BASE/notify/test-email" \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json"
```

- Church and school services each have their own route; subject line includes `[TEST] Church Scraper` or `[TEST] School Scraper`.

## 2. Local CLI (real SMTP send)

From repo root, with env vars set:

```bash
python3 npsa-scraper/scripts/test_smtp_notify.py --church
# or
python3 npsa-scraper/scripts/test_smtp_notify.py --school
```

## 3. Unit tests (mocked, no network)

```bash
cd "npsa-scraper/Church Contact Scraper" && python3 -m unittest tests.test_notify_smtp -v
cd "npsa-scraper/School Contact Scraper" && python3 -m unittest tests.test_notify_smtp -v
```
