#!/bin/bash
# Ready-to-Run Test: Texas Counties
# =========================================
# Legacy Places API | Streaming Pipeline | Hard Cap: 100 API Calls

# API keys should be set as environment variables before running this script
# export GOOGLE_PLACES_API_KEY="your-key-here"
# export OPENAI_API_KEY="your-key-here"

if [ -z "$GOOGLE_PLACES_API_KEY" ] || [ -z "$OPENAI_API_KEY" ]; then
    echo "ERROR: GOOGLE_PLACES_API_KEY and OPENAI_API_KEY must be set as environment variables"
    echo "Set them before running this script:"
    echo "  export GOOGLE_PLACES_API_KEY='your-key'"
    echo "  export OPENAI_API_KEY='your-key'"
    exit 1
fi

echo "=========================================="
echo "STREAMING PIPELINE - SCALED TEST"
echo "=========================================="
echo "Configuration:"
echo "  - Legacy Places API: Enabled"
echo "  - API Calls: 100 (hard cap)"
echo "  - Search: Texas counties (randomized)"
echo "  - Max search terms per county: 2"
echo "  - Max pages per school: 3 (top 3 with score >= 20)"
echo "  - Selenium retries: 5"
echo "  - Output: Final CSV with extracted contacts"
echo ""
echo "Starting pipeline..."
echo ""

cd "$(dirname "$0")"

python3 Pipeline.py \
  --google-api-key "$GOOGLE_PLACES_API_KEY" \
  --openai-api-key "$OPENAI_API_KEY" \
  --state texas \
  --global-max-api-calls 100 \
  --max-pages-per-school 3 \
  --batch-size 0 \
  --output test_final_contacts.csv \
  --output-no-emails test_final_contacts_no_emails.csv

echo ""
echo "=========================================="
echo "Pipeline complete!"
echo "=========================================="
echo "Output files:"
echo "  - test_final_contacts.csv"
echo "  - test_final_contacts_no_emails.csv"
echo "=========================================="

