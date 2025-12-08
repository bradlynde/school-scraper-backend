#!/usr/bin/env python3
"""
Filter contacts CSV using Step 2 filtering logic
Removes contacts from public schools, non-Christian schools, colleges, seminaries, preschools, etc.
All filtering logic is centralized in step2.py
"""

import pandas as pd
import sys
from pathlib import Path
from assets.shared.models import School
import step2


def create_minimal_school(school_name: str) -> School:
    """Create a minimal School object from just the school name"""
    return School(
        place_id='',  # Not needed for filtering
        name=school_name,
        address='',  # Not needed for filtering
        website='',
        phone='',
        state='Texas',  # Assume Texas since CSV is "Texas School Leads"
        county='',
        detected_state='TX',
        detected_county='',
        found_via=''
    )


def filter_contacts_csv(input_csv: str, output_csv: str, target_state: str = 'texas', 
                       openai_api_key: str = None):
    """
    Filter contacts CSV by school name using Step 2 filtering logic.
    All filtering logic is centralized in step2.py.
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file
        target_state: Target state for filtering (default: 'texas')
        openai_api_key: Optional OpenAI API key for LLM filtering
    """
    print("=" * 70)
    print("FILTERING CONTACTS BY SCHOOL NAME")
    print("=" * 70)
    print(f"Input: {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Target State: {target_state}")
    if openai_api_key:
        print("LLM Filtering: ENABLED")
    else:
        print("LLM Filtering: DISABLED (using pre-filters only)")
    print("=" * 70)
    print()
    
    # Load CSV
    print("Loading CSV...")
    df = pd.read_csv(input_csv)
    print(f"Loaded {len(df)} total contacts")
    
    # Get unique school names
    unique_schools = df['school_name'].unique()
    print(f"Found {len(unique_schools)} unique schools")
    print()
    
    # Initialize LLM filter if API key provided
    llm_filter = None
    if openai_api_key:
        try:
            llm_filter = step2.LLMSchoolFilter(
                api_key=openai_api_key,
                target_state=target_state,  # Pass target state to LLM filter
                model="gpt-4o-mini",
                batch_size=20
            )
            print("Initialized LLM school filter")
        except Exception as e:
            print(f"WARNING: Could not initialize LLM filter: {e}")
            print("Continuing with pre-filters only")
    
    # Filter schools using step2.filter_school()
    print("Filtering schools...")
    valid_schools = set()
    invalid_schools = []
    
    for school_name in unique_schools:
        if pd.isna(school_name) or not school_name.strip():
            invalid_schools.append((school_name, "Empty school name"))
            continue
        
        # Create minimal School object
        school = create_minimal_school(school_name.strip())
        
        # Use step2.filter_school() - all filtering logic is centralized here
        result = step2.filter_school(school, target_state=target_state, llm_filter=llm_filter)
        if isinstance(result, tuple):
            filtered_school, _ = result
        else:
            filtered_school = result
        
        if filtered_school:
            valid_schools.add(school_name.strip())
        else:
            invalid_schools.append((school_name, "Filtered by step2"))
    
    # Flush any pending LLM batches
    if llm_filter:
        llm_filter.flush()
    
    print(f"Valid schools: {len(valid_schools)}")
    print(f"Invalid schools: {len(invalid_schools)}")
    print()
    
    # Filter contacts
    print("Filtering contacts...")
    initial_count = len(df)
    
    # Keep only contacts from valid schools
    df_filtered = df[df['school_name'].str.strip().isin(valid_schools)].copy()
    
    final_count = len(df_filtered)
    removed_count = initial_count - final_count
    
    print(f"Contacts before filtering: {initial_count}")
    print(f"Contacts after filtering: {final_count}")
    print(f"Contacts removed: {removed_count}")
    print()
    
    # Save filtered CSV
    print(f"Saving filtered contacts to {output_csv}...")
    df_filtered.to_csv(output_csv, index=False)
    print("Done!")
    print()
    
    # Print summary
    print("=" * 70)
    print("FILTERING SUMMARY")
    print("=" * 70)
    print(f"Total contacts: {initial_count}")
    print(f"Valid contacts: {final_count}")
    print(f"Removed contacts: {removed_count}")
    print(f"Removal rate: {removed_count/initial_count*100:.1f}%")
    print()
    
    if invalid_schools:
        print(f"Filtered out {len(invalid_schools)} schools:")
        print("-" * 70)
        for school_name, reason in sorted(invalid_schools, key=lambda x: x[1]):
            print(f"  [{reason:25s}] {school_name}")
    print("=" * 70)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 filter_contacts.py <input_csv> [output_csv] [target_state] [--openai-key KEY]")
        print()
        print("Example:")
        print("  python3 filter_contacts.py /path/to/contacts.csv filtered_contacts.csv texas")
        print("  python3 filter_contacts.py /path/to/contacts.csv filtered_contacts.csv texas --openai-key sk-...")
        sys.exit(1)
    
    input_csv = sys.argv[1]
    output_csv = sys.argv[2] if len(sys.argv) > 2 else input_csv.replace('.csv', '_filtered.csv')
    target_state = sys.argv[3] if len(sys.argv) > 3 else 'texas'
    
    # Parse optional OpenAI API key
    openai_api_key = None
    if '--openai-key' in sys.argv:
        idx = sys.argv.index('--openai-key')
        if idx + 1 < len(sys.argv):
            openai_api_key = sys.argv[idx + 1]
    
    filter_contacts_csv(input_csv, output_csv, target_state, openai_api_key)
