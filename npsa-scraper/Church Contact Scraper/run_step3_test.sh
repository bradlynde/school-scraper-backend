#!/bin/bash
#
# Church Scraper - STEP 3 ONLY Test
# ==================================
# Runs only step 3 (page discovery) on step 2 output.
# Crawls church websites to find staff/team/ministry pages.
#
# Prerequisites:
#   - Step 2 output CSV (step2_test_*.csv)
#   - Network access (HTTP requests to church sites)
#
# Usage:
#   ./run_step3_test.sh
#   ./run_step3_test.sh -i step2_test_20260314_153113.csv
#   ./run_step3_test.sh --limit 5
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" &>/dev/null; then
  PYTHON=python
fi

"$PYTHON" run_step3_test.py "$@"
