#!/bin/bash
#
# Church Scraper - STEP 2 ONLY Test
# ==================================
# Runs only step 2 (church filtering) on step 1 output.
# Uses latest step1_test_*.csv by default.
#
# Prerequisites:
#   - Step 1 output CSV (step1_test_*.csv)
#   - OPENAI_API_KEY for LLM filter (optional: use --no-llm for pre-filters only)
#
# Usage:
#   ./run_step2_test.sh
#   ./run_step2_test.sh -i step1_test_20260314_152539.csv
#   ./run_step2_test.sh --no-llm
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then
  set -a
  source .env
  set +a
elif [ -f ../.env ]; then
  set -a
  source ../.env
  set +a
fi

PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" &>/dev/null; then
  PYTHON=python
fi

"$PYTHON" run_step2_test.py "$@"
