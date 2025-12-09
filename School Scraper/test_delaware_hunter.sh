#!/bin/bash

# Delaware Test Run with Hunter.io
# This script runs the pipeline for Delaware to test Hunter.io email enrichment

echo "=========================================="
echo "DELAWARE TEST RUN - HUNTER.IO TESTING"
echo "=========================================="
echo ""
echo "This will:"
echo "  1. Discover schools in Delaware"
echo "  2. Extract contacts"
echo "  3. Split contacts (Step 11)"
echo "  4. Enrich contacts without emails via Hunter.io (Step 12)"
echo "  5. Compile final CSV (Step 13)"
echo ""
echo "Make sure HUNTER_IO_API_KEY is set in your environment!"
echo ""

# Check for required API keys
if [ -z "$GOOGLE_PLACES_API_KEY" ]; then
    echo "ERROR: GOOGLE_PLACES_API_KEY not set"
    echo "Export it with: export GOOGLE_PLACES_API_KEY='your-key'"
    exit 1
fi

if [ -z "$OPENAI_API_KEY" ]; then
    echo "ERROR: OPENAI_API_KEY not set"
    echo "Export it with: export OPENAI_API_KEY='your-key'"
    exit 1
fi

if [ -z "$HUNTER_IO_API_KEY" ]; then
    echo "WARNING: HUNTER_IO_API_KEY not set"
    echo "Hunter.io enrichment will be skipped"
    echo "Export it with: export HUNTER_IO_API_KEY='your-key'"
    echo ""
    read -p "Continue without Hunter.io? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "Starting Delaware pipeline..."
echo ""

# Run the pipeline
cd "$(dirname "$0")"
python3 Pipeline.py \
    --google-api-key "$GOOGLE_PLACES_API_KEY" \
    --openai-api-key "$OPENAI_API_KEY" \
    --state delaware \
    --global-max-api-calls 500 \
    --max-pages-per-school 3

echo ""
echo "=========================================="
echo "TEST RUN COMPLETE"
echo "=========================================="
echo ""
echo "Check the output CSV file: Delaware leads.csv"
echo ""

