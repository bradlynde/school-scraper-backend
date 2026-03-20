#!/bin/bash
# Pull Delaware church run from Church API storage
# Usage: AUTH_TOKEN="your-token" ./scripts/pull-delaware-church-run.sh
# Or with run_id: AUTH_TOKEN="..." RUN_ID="a72c9203-..." ./scripts/pull-delaware-church-run.sh
# Get token: In browser DevTools (F12) -> Application -> Local Storage -> auth_token

CHURCH_API="${NEXT_PUBLIC_CHURCH_API_URL:-https://church-scraper-backend-production.up.railway.app}"
CHURCH_API="${CHURCH_API%/}"

if [ -z "$AUTH_TOKEN" ]; then
  echo "Set AUTH_TOKEN (from browser localStorage after logging in to npsa-tools)"
  echo "Example: AUTH_TOKEN=eyJ... ./scripts/pull-delaware-church-run.sh"
  exit 1
fi

if [ -n "$RUN_ID" ]; then
  echo "Using provided RUN_ID: $RUN_ID"
else
  echo "Fetching runs from Church API..."
  RUNS=$(curl -s -H "Authorization: Bearer $AUTH_TOKEN" "$CHURCH_API/runs")
  if echo "$RUNS" | grep -q '"error"'; then
    echo "Error: $RUNS"
    exit 1
  fi

  # Find Delaware run (most recent completed)
  RUN_ID=$(echo "$RUNS" | python3 -c "
import json, sys
data = json.load(sys.stdin)
runs = data.get('runs', [])
delaware = [r for r in runs if r.get('state','').lower() == 'delaware' and r.get('status') in ('completed','error')]
if not delaware:
    print('', end='')
else:
    delaware.sort(key=lambda x: x.get('created_at',''), reverse=True)
    print(delaware[0]['run_id'])
" 2>/dev/null)
fi

if [ -z "$RUN_ID" ]; then
  echo "No Delaware run found (or RUN_ID not set). Recent runs:"
  if [ -n "$RUNS" ]; then
    echo "$RUNS" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for r in data.get('runs', [])[:10]:
    print(f\"  {r.get('run_id')} | {r.get('state')} | {r.get('status')} | {r.get('created_at','')}\")
" 2>/dev/null
  fi
  exit 1
fi

echo "Found Delaware run: $RUN_ID"
OUTPUT="Delaware_church_run_${RUN_ID}.csv"
echo "Downloading to $OUTPUT..."
curl -s -H "Authorization: Bearer $AUTH_TOKEN" "$CHURCH_API/runs/$RUN_ID/download" -o "$OUTPUT"

if [ -f "$OUTPUT" ] && [ -s "$OUTPUT" ]; then
  echo "Saved to $OUTPUT ($(wc -l < "$OUTPUT") lines)"
else
  echo "Download failed or empty. Check run exists and you have access."
  rm -f "$OUTPUT"
  exit 1
fi
