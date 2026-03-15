#!/usr/bin/env python3
"""
STEP 3 ONLY - Page Discovery Test
=================================
Runs only step 3 (discover staff/ministry pages on church websites) on step 2 output.
- Loads churches from step2_test_*.csv
- Crawls each website to find staff, team, leadership, ministry pages
- Outputs discovered pages to step3_test_*.csv
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

_script_dir = Path(__file__).parent
sys.path.insert(0, str(_script_dir))

import importlib.util
step3_spec = importlib.util.spec_from_file_location(
    "step3_discovery",
    _script_dir / "steps" / "step3-discovery.py"
)
step3_module = importlib.util.module_from_spec(step3_spec)
step3_spec.loader.exec_module(step3_module)
PageDiscoverer = step3_module.PageDiscoverer


def main():
    parser = argparse.ArgumentParser(description="Step 3 only - discover pages from step 2 CSV")
    parser.add_argument("--input", "-i", help="Step 2 output CSV (default: latest step2_test_*.csv)")
    parser.add_argument("--output", "-o", help="Output CSV (default: step3_test_<timestamp>.csv)")
    parser.add_argument("--max-depth", type=int, default=3, help="Max crawl depth (default: 3)")
    parser.add_argument("--max-pages-per-church", type=int, default=5, help="Max pages per church (default: 5)")
    parser.add_argument("--top-pages-limit", type=int, default=3, help="Top N pages to keep per church (default: 3)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of churches to process (for quick test)")
    args = parser.parse_args()

    # Find input CSV
    if args.input:
        input_path = Path(args.input)
        if not input_path.is_absolute():
            input_path = _script_dir / input_path
    else:
        step2_files = sorted(_script_dir.glob("step2_test_*.csv"), reverse=True)
        if not step2_files:
            print("ERROR: No step2_test_*.csv found. Run step 2 first.")
            sys.exit(1)
        input_path = step2_files[0]

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else _script_dir / f"step3_test_{timestamp}.csv"
    if not output_path.is_absolute():
        output_path = _script_dir / output_path

    print("=" * 60)
    print("STEP 3 ONLY - Page Discovery Test")
    print("=" * 60)
    print(f"  Input:              {input_path.name}")
    print(f"  Output:             {output_path.name}")
    print(f"  Max depth:          {args.max_depth}")
    print(f"  Max pages/church:   {args.max_pages_per_church}")
    print(f"  Top pages limit:    {args.top_pages_limit}")
    if args.limit:
        print(f"  Church limit:       {args.limit} (quick test)")
    print("=" * 60)
    print()

    import pandas as pd
    df = pd.read_csv(input_path)
    df_with_urls = df[df['website'].notna() & (df['website'] != '')]

    if args.limit:
        df_with_urls = df_with_urls.head(args.limit)

    print(f"Processing {len(df_with_urls)} churches with websites")
    print()

    discoverer = PageDiscoverer(timeout=10, max_retries=3)
    all_pages = []

    for idx, row in df_with_urls.iterrows():
        church_name = row['name']
        base_url = row['website']

        print(f"\n[{len(all_pages) // args.top_pages_limit + 1}/{len(df_with_urls)}] {church_name}")

        try:
            pages = discoverer.discover_pages(
                church_name=church_name,
                base_url=base_url,
                max_depth=args.max_depth,
                max_pages_per_church=args.max_pages_per_church,
                top_pages_limit=args.top_pages_limit
            )
            all_pages.extend(pages)
            # Save progress after each church
            if all_pages:
                out_df = pd.DataFrame(all_pages)
                out_df.to_csv(output_path, index=False)
        except Exception as e:
            print(f"    ERROR: {e}")
            continue

    # Final save
    if all_pages:
        out_df = pd.DataFrame(all_pages)
        out_df.to_csv(output_path, index=False)

    print()
    print("=" * 60)
    print("STEP 3 COMPLETE")
    print("=" * 60)
    print(f"  Churches processed:  {len(df_with_urls)}")
    print(f"  Pages discovered:    {len(all_pages)}")
    print(f"  Output:              {output_path.name}")
    print("=" * 60)


if __name__ == "__main__":
    main()
