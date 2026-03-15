#!/usr/bin/env python3
"""
STEP 1 ONLY - Church Discovery Test
===================================
Runs only step 1 (Google Places API search) for validation.
- 3 counties
- 6 search terms (most common denominations for NPSA client demographic)
- Outputs discovered churches to CSV for review

Search terms used (best demographic for nonprofitsecurityadvisors.com):
  1. churches in {county} County
  2. Christian churches
  3. Baptist
  4. Methodist
  5. Church of Christ
  6. Catholic
"""

import os
import sys
import csv
from pathlib import Path
from datetime import datetime

# Add parent for imports
_script_dir = Path(__file__).parent
sys.path.insert(0, str(_script_dir))

# Env vars loaded by run_church_test.sh via source .env
import importlib.util
step1_spec = importlib.util.spec_from_file_location(
    "step1_search",
    _script_dir / "steps" / "step1-search.py"
)
step1_module = importlib.util.module_from_spec(step1_spec)
step1_spec.loader.exec_module(step1_module)
ChurchSearcher = step1_module.ChurchSearcher


def main():
    api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
    if not api_key or len(api_key) < 10:
        print("ERROR: GOOGLE_PLACES_API_KEY not set or invalid. Set it in .env")
        sys.exit(1)

    state = "texas"
    counties = ["Austin", "Bell", "Bastrop"]  # 3 counties
    max_search_terms = 6
    global_max_api_calls = 500  # Reasonable cap for 3 counties × 6 terms

    print("=" * 60)
    print("STEP 1 ONLY - Church Discovery Test")
    print("=" * 60)
    print(f"  State:           {state}")
    print(f"  Counties:        {', '.join(counties)}")
    print(f"  Search terms:    {max_search_terms} (Baptist, Methodist, Catholic, etc.)")
    print(f"  API call cap:    {global_max_api_calls}")
    print("=" * 60)
    print()

    searcher = ChurchSearcher(
        api_key=api_key,
        global_max_api_calls=global_max_api_calls,
        max_churches=None,
        target_state=state
    )

    churches = []
    for church in searcher.discover_churches(
        counties=counties,
        state=state,
        batch_size=0,
        max_search_terms=max_search_terms
    ):
        churches.append(church)

    # Write step 1 output CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = _script_dir / f"step1_test_{timestamp}.csv"
    fieldnames = ["name", "address", "website", "phone", "county", "found_via"]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in churches:
            writer.writerow({
                "name": c.name,
                "address": c.address or "",
                "website": c.website or "",
                "phone": c.phone or "",
                "county": c.county or "",
                "found_via": c.found_via or "",
            })

    print()
    print("=" * 60)
    print("STEP 1 COMPLETE")
    print("=" * 60)
    print(f"  Churches found:  {len(churches)}")
    print(f"  API calls:       {searcher.stats['total_api_calls']}")
    print(f"  Output:          {output_csv.name}")
    print("=" * 60)


if __name__ == "__main__":
    main()
