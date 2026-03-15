#!/usr/bin/env python3
"""
STEP 2 ONLY - Church Filter Test
================================
Runs only step 2 (church filtering) on step 1 output CSV.
- Loads churches from step1_test_*.csv
- Applies pre-filters + optional LLM filter
- Outputs passed churches to step2_test_*.csv
"""

import os
import sys
import csv
import re
import argparse
from pathlib import Path
from datetime import datetime

_script_dir = Path(__file__).parent
sys.path.insert(0, str(_script_dir))

import importlib.util
step2_spec = importlib.util.spec_from_file_location(
    "step2_filter",
    _script_dir / "steps" / "step2-church_filter.py"
)
step2_module = importlib.util.module_from_spec(step2_spec)
step2_spec.loader.exec_module(step2_module)
filter_church = step2_module.filter_church
LLMChurchFilter = step2_module.LLMChurchFilter

from assets.shared.models import Church


def extract_state_from_address(address: str) -> str:
    """Extract state abbreviation from address (e.g. ', TX 78704' -> TX)."""
    if not address:
        return ""
    match = re.search(r',\s*([A-Z]{2})\s+\d{5}', address)
    return match.group(1) if match else ""


def load_churches_from_step1_csv(csv_path: Path, state: str = "Texas") -> list:
    """Load Church objects from step 1 output CSV."""
    churches = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            address = row.get("address", "")
            detected_state = extract_state_from_address(address)
            church = Church(
                place_id=row.get("place_id") or f"step1_{i}_{abs(hash((row.get('name',''), address))) % 10**8}",
                name=row.get("name", ""),
                address=address,
                website=row.get("website") or None,
                phone=row.get("phone") or None,
                county=row.get("county", ""),
                state=state,
                detected_state=detected_state or None,
                found_via=row.get("found_via") or None,
            )
            churches.append(church)
    return churches


def main():
    parser = argparse.ArgumentParser(description="Step 2 only - filter churches from step 1 CSV")
    parser.add_argument("--input", "-i", help="Step 1 output CSV (default: latest step1_test_*.csv)")
    parser.add_argument("--state", default="texas", help="Target state (default: texas)")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM filter (pre-filters only)")
    args = parser.parse_args()

    # Find input CSV
    if args.input:
        input_path = Path(args.input)
        if not input_path.is_absolute():
            input_path = _script_dir / input_path
    else:
        step1_files = sorted(_script_dir.glob("step1_test_*.csv"), reverse=True)
        if not step1_files:
            print("ERROR: No step1_test_*.csv found. Run step 1 first.")
            sys.exit(1)
        input_path = step1_files[0]

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        sys.exit(1)

    openai_key = os.getenv("OPENAI_API_KEY", "") if not args.no_llm else ""
    use_llm = not args.no_llm and openai_key and len(openai_key) >= 10

    if not args.no_llm and not use_llm:
        print("WARNING: OPENAI_API_KEY not set. Running pre-filters only (--no-llm).")

    print("=" * 60)
    print("STEP 2 ONLY - Church Filter Test")
    print("=" * 60)
    print(f"  Input:     {input_path.name}")
    print(f"  State:     {args.state}")
    print(f"  LLM:       {'Yes (gpt-4o-mini)' if use_llm else 'No (pre-filters only)'}")
    print("=" * 60)
    print()

    churches = load_churches_from_step1_csv(input_path, state=args.state)
    print(f"Loaded {len(churches)} churches from step 1")

    llm_filter = None
    if use_llm:
        llm_filter = LLMChurchFilter(
            api_key=openai_key,
            target_state=args.state,
            model="gpt-4o-mini",
            batch_size=20
        )

    passed = []
    rejected = []
    for church in churches:
        filtered_church, reason = filter_church(
            church,
            target_state=args.state,
            llm_filter=llm_filter
        )
        if filtered_church:
            passed.append(filtered_church)
        else:
            rejected.append((church, reason))

    if llm_filter:
        llm_filter.flush()

    # Write passed churches
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = _script_dir / f"step2_test_{timestamp}.csv"
    fieldnames = ["name", "address", "website", "phone", "county", "found_via"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in passed:
            writer.writerow({
                "name": c.name,
                "address": c.address or "",
                "website": c.website or "",
                "phone": c.phone or "",
                "county": c.county or "",
                "found_via": c.found_via or "",
            })

    # Summary by reject reason
    reason_counts = {}
    for _, r in rejected:
        reason_counts[r] = reason_counts.get(r, 0) + 1

    print()
    print("=" * 60)
    print("STEP 2 COMPLETE")
    print("=" * 60)
    print(f"  Passed:    {len(passed)}")
    print(f"  Rejected:  {len(rejected)}")
    if reason_counts:
        print("  Reject reasons:")
        for r, n in sorted(reason_counts.items(), key=lambda x: -x[1]):
            print(f"    - {r}: {n}")
    print(f"  Output:    {output_csv.name}")
    print("=" * 60)


if __name__ == "__main__":
    main()
