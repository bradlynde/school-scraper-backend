"""
STEP 10: FILTER CONTACTS BY TITLE (LLM-BASED FILTERING)
=======================================================
Filter contacts from Step 9 to keep only administrative/leadership roles.

Uses LLM to determine if a contact is administrative based on their title.

Input: CSV from Step 9 with ALL extracted contacts (no filtering)
Output: CSV with filtered contacts (only administrative/leadership roles)
"""

from openai import OpenAI
import pandas as pd
import csv
import io
import time
from typing import List, Dict, Optional
import re


# Title filtering prompt - determines if a contact is administrative
TITLE_FILTERING_PROMPT = """
You are a title classifier for school administrative contacts.

Your job is to determine if a contact's title indicates they are an ADMINISTRATIVE/LEADERSHIP role at a school.

INPUT:
- A contact with: First Name, Last Name, Title, Email (optional), Phone (optional)

OUTPUT:
Return ONLY one word: "KEEP" or "EXCLUDE"

RULES:

KEEP if the title indicates administrative/leadership responsibility. Examples:
- Superintendent, Head of School, School Head, President (of school/college/university), Chancellor, Provost
- Principal (ALL variants, including assistant/associate/vice/deputy levels):
  * Principal (standalone), Head Principal
  * Elementary, Middle, High School, Preschool, Early Childhood Principals
  * Upper School, Lower School, Secondary Principals
  * Principal of [division/subject] (e.g., "Principal of Elementary")
  * Assistant/Associate/Vice/Deputy Principals (any wording: "Assistant Principal", "AP of Instruction", "Elementary Assistant Principal", etc.)
- Division Head, Upper School Head, Middle School Head, Lower School Head
- Assistant/Associate Head of School
- Director roles tied to school operations (operations, academics, instruction, curriculum, assessment, student services, technology/IT, facilities, finance, HR, security, compliance)
- Chief roles (CEO, CFO, COO, CTO, CAO, Chief of Staff, Chief Strategy Officer)
- Dean (academic, student, operations), Business Manager, Operations Manager
- Campus/School Administrator, Executive Director, General Manager
- Coordinator if clearly administrative (testing, accreditation, compliance, operations). Still EXCLUDE purely instructional coordinators.
- Academic Dean, Dean of Academics, Dean of Students (if clearly administrative role, not just student services)
- Department Head, Division Director (if operations-focused, not purely instructional)
- Program Director (if operations/administrative focused, not instructional programs)

EXCLUDE if the title contains ANY of these (case-insensitive):
- teacher, faculty, instructor, professor, tutor, aide, para
- counselor, counselling, psychologist, therapist, chaplain, ministry, pastor
- admissions, admission, enrollment, registrar, recruiting, outreach
- marketing, communications, media, social media, pr, public relations, advancement, development, fundraising, alumni, donor
- athletic, athletics, coach, sports, pe, physical education
- fine arts, music, band, choir, theatre, performing arts
- secretary, administrative assistant, office manager, office admin, receptionist, executive assistant
- nurse, health office, health services, nutrition, cafeteria, dining, food service
- residential, dorm, housing, boarding
- early childhood, preschool, daycare, aftercare
- student life, student services, student support
- trip leader, mission trip, trip coordinator
- board, trustee, governance, regent, chairman, vice-chair, treasurer, secretary (if board-related), rector
- "president" (standalone - board role), "vice president" (standalone - board role)
- curriculum coordinator (instructional)
- assistant director, asst. director, asst director (unless explicitly operations/business focused)
- principal of accreditation, accreditation principal
- casp director

EXCEPTIONS:
- If title is "Head of School & [something]" or "Superintendent & [something]" → KEEP (admin role is primary)
- If title is "[something] & Head of School" → KEEP (has admin role)
- If title is "School President" or "President of [School Name]" → KEEP (school leadership, not board)
- If title is just "President" or "Vice President" without school context → EXCLUDE (board role)

DUAL ROLES:
- "Head of School & Math Teacher" → KEEP (admin role first)
- "Principal & History Teacher" → KEEP (principal is admin role, even if teaching is mentioned)
- "Assistant Principal & [anything]" → KEEP (assistant principal is administrative)
- "[anything] & Assistant Principal" → KEEP (assistant principal is administrative)

OUTPUT:
Return ONLY "KEEP" or "EXCLUDE" - nothing else.
"""


class TitleFilter:
    """Filter contacts by title using LLM"""
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        """
        Initialize title filter
        
        Args:
            api_key: OpenAI API key
            model: Model to use (default: gpt-4o-mini)
        """
        self.api_key = api_key
        self.model = model
        self.client = OpenAI(api_key=api_key)
    
    def filter_contact(self, contact: Dict, max_retries: int = 3) -> bool:
        """
        Determine if a contact should be kept based on their title
        
        Args:
            contact: Contact dictionary with 'first_name', 'last_name', 'title', etc.
            max_retries: Maximum retry attempts
        
        Returns:
            True if contact should be kept, False if excluded
        """
        title = contact.get('title', '').strip()
        first_name = contact.get('first_name', '').strip()
        last_name = contact.get('last_name', '').strip()
        
        # Skip if no title
        if not title:
            return False
        
        # Skip if no name
        if not first_name or not last_name:
            return False
        
        for attempt in range(max_retries):
            try:
                # Build user message with contact info
                user_message = f"""First Name: {first_name}
Last Name: {last_name}
Title: {title}"""
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": TITLE_FILTERING_PROMPT},
                        {"role": "user", "content": user_message}
                    ],
                    temperature=0.0,
                    max_tokens=10  # Just need "KEEP" or "EXCLUDE"
                )
                
                # Extract response
                response_text = response.choices[0].message.content.strip().upper()
        
                # Parse response
                if "KEEP" in response_text:
                    return True
                elif "EXCLUDE" in response_text:
                    return False
                else:
                    # Default to exclude if unclear
                    print(f"      WARNING: Unexpected LLM response for {first_name} {last_name}: {response_text}")
                    return False
                
            except Exception as e:
                error_str = str(e)
                is_rate_limit = '429' in error_str or 'rate_limit' in error_str.lower()
                
                if is_rate_limit:
                    wait_seconds = 1.0
                    import re
                    wait_match = re.search(r'Please try again in (\d+)(ms|s)', error_str, re.IGNORECASE)
                    if wait_match:
                        wait_value = int(wait_match.group(1))
                        wait_unit = wait_match.group(2).lower()
                        if wait_unit == 'ms':
                            wait_seconds = (wait_value / 1000.0) + 0.5
                        else:
                            wait_seconds = wait_value + 0.5
                    
                    if attempt < max_retries - 1:
                        print(f"      ⚠️  Rate limit hit (attempt {attempt + 1}/{max_retries}), waiting {wait_seconds:.1f}s...")
                        time.sleep(wait_seconds)
                        continue
                    else:
                        print(f"      ❌ Rate limit exceeded. Excluding contact.")
                        return False
                else:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        print(f"      WARNING: LLM error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    print(f"      ERROR: LLM error: {e}")
                    return False
        
        return False
    
    def filter_contacts(self, input_csv: str, output_csv: str, output_excluded_csv: str = None):
        """
        Filter contacts from Step 9 to keep only administrative roles
        
        Args:
            input_csv: CSV from Step 9 with all contacts
            output_csv: Output CSV with filtered contacts (administrative only)
            output_excluded_csv: Optional CSV with excluded contacts (for review)
        """
        print("\n" + "="*70)
        print("STEP 10: FILTERING CONTACTS BY TITLE")
        print("="*70)
        print(f"Model: {self.model}")
        print("Filtering contacts to keep only administrative/leadership roles")
        print("="*70 + "\n")
        
        # Read contacts from Step 9
        df = pd.read_csv(input_csv)
        
        print(f"Processing {len(df)} contacts from Step 9")
        print("="*70 + "\n")
        
        kept_contacts = []
        excluded_contacts = []
        
        for idx, row in df.iterrows():
            contact = {
                'first_name': row.get('first_name', ''),
                'last_name': row.get('last_name', ''),
                'title': row.get('title', ''),
                'email': row.get('email', ''),
                'phone': row.get('phone', ''),
                'school_name': row.get('school_name', ''),
                'source_url': row.get('source_url', '')
            }
            
            if (idx + 1) % 10 == 0:
                print(f"  Processing contact {idx + 1}/{len(df)}...")
            
            # Filter by title
            should_keep = self.filter_contact(contact, max_retries=5)
            
            if should_keep:
                kept_contacts.append(contact)
            else:
                excluded_contacts.append(contact)
            
            # Rate limiting - small delay between contacts
            if idx < len(df) - 1:
                time.sleep(0.1)
        
        # Save results
        if kept_contacts:
            df_kept = pd.DataFrame(kept_contacts)
            df_kept.to_csv(output_csv, index=False)
            print(f"\n✓ Kept {len(kept_contacts)} administrative contacts")
        else:
            # Create empty CSV with headers
            pd.DataFrame(columns=['first_name', 'last_name', 'title', 'email', 'phone', 'school_name', 'source_url']).to_csv(output_csv, index=False)
            print(f"\n⚠️  No administrative contacts found")
        
        if output_excluded_csv and excluded_contacts:
            df_excluded = pd.DataFrame(excluded_contacts)
            df_excluded.to_csv(output_excluded_csv, index=False)
            print(f"✓ Excluded {len(excluded_contacts)} non-administrative contacts")
        
        # Print summary
        print("\n" + "="*70)
        print("FILTERING COMPLETE")
        print("="*70)
        print(f"Total contacts processed: {len(df)}")
        print(f"Contacts kept (administrative): {len(kept_contacts)}")
        print(f"Contacts excluded (non-administrative): {len(excluded_contacts)}")
        print(f"Output file: {output_csv}")
        if output_excluded_csv:
            print(f"Excluded contacts file: {output_excluded_csv}")
        print("="*70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Filter contacts by title using LLM')
    parser.add_argument('--input', required=True, help='Input CSV from Step 5')
    parser.add_argument('--output', default='step10_contacts_filtered.csv', help='Output CSV filename for filtered contacts')
    parser.add_argument('--output-excluded', default=None, help='Output CSV filename for excluded contacts (optional)')
    parser.add_argument('--api-key', required=True, help='OpenAI API key')
    parser.add_argument('--model', default='gpt-4o-mini', help='Model to use (default: gpt-4o-mini)')
    args = parser.parse_args()
    
    filterer = TitleFilter(api_key=args.api_key, model=args.model)
    filterer.filter_contacts(args.input, args.output, args.output_excluded)
