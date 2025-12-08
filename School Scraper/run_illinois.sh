#!/bin/bash
# Run all Illinois counties in single instance
# 5 search terms per county (optimized from 10)
# API call limit set to capture all data with pagination

cd "$(dirname "$0")"

echo "=========================================="
echo "RUN: All Illinois Counties"
echo "Search Terms: 5 per county"
echo "No School Limit"
echo "Max API Calls: 1500 (covers 101 counties × 5 terms + pagination)"
echo "=========================================="
echo ""

python3 Pipeline.py \
    --google-api-key "${GOOGLE_PLACES_API_KEY}" \
    --openai-api-key "${OPENAI_API_KEY}" \
    --state illinois \
    --global-max-api-calls 1500 \
    --max-pages-per-school 3
