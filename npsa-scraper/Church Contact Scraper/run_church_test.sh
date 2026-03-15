#!/bin/bash
#
# Church Scraper - STEP 1 ONLY Test
# ==================================
# Runs only step 1 (Google Places API church discovery).
# - 3 counties: Austin, Bell, Bastrop (Texas)
# - 6 search terms: churches, Christian, Baptist, Methodist, Church of Christ, Catholic
# - Best demographic for nonprofitsecurityadvisors.com
#
# Prerequisites:
#   - GOOGLE_PLACES_API_KEY in environment (or .env)
#   - Python 3.11+ with dependencies (pip install -r ../requirements.txt)
#
# Usage:
#   ./run_church_test.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if present
if [ -f .env ]; then
  set -a
  source .env
  set +a
elif [ -f ../.env ]; then
  set -a
  source ../.env
  set +a
fi

# Validate required env
if [ -z "$GOOGLE_PLACES_API_KEY" ] || [ ${#GOOGLE_PLACES_API_KEY} -lt 10 ]; then
  echo "ERROR: GOOGLE_PLACES_API_KEY not set or invalid. Set it in .env or export it."
  exit 1
fi

PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" &>/dev/null; then
  PYTHON=python
fi

"$PYTHON" run_step1_test.py
