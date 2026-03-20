"""
STEP 10: FILTER CONTACTS BY TITLE (LLM-BASED FILTERING)
=======================================================
Filter contacts from Step 9 to keep only church administrative/leadership roles.

Uses LLM to determine if a contact is administrative based on their title.

Input: CSV from Step 9 with ALL extracted contacts (no filtering)
Output: CSV with filtered contacts (only church administrative/leadership roles)
"""

from openai import OpenAI
import pandas as pd
import csv
import io
import time
import os
import sys
from typing import List, Dict, Optional
import re

# ANSI escape codes for bold text
BOLD = '\033[1m'
RESET = '\033[0m'

def bold(text: str) -> str:
    """Make text bold in terminal output"""
    return f"{BOLD}{text}{RESET}"


# Title filtering prompt - church administrative/leadership roles
TITLE_FILTERING_PROMPT = """
You are a title classifier for church administrative contacts.

Your job is to determine if a contact's title indicates they are an ADMINISTRATIVE/LEADERSHIP role at a church.

INPUT:
- A contact with: First Name, Last Name, Title, Email (optional), Phone (optional)

OUTPUT:
Return ONLY one word: "KEEP" or "EXCLUDE"

RULES:

KEEP if the title indicates church administrative/leadership responsibility. Examples:
- Administrative Pastor, Associate Pastor, Business Administrator, Campus Pastor
- Church Administrator, Director of Operations, Executive Pastor, Facilities Director
- Lead Pastor, Operations Director, Security Coordinator, Security Director
- Senior Pastor, IT Director, Technology Director
- Pastor (senior, lead, executive, administrative, associate, campus - all pastor roles)
- Minister (senior, associate - ministry leadership)
- Priest, Rector, Vicar (clergy leadership)
- Director roles (operations, technology, facilities, security, finance, HR)
- Chief roles (CEO, CFO, COO, CTO, Chief of Staff)
- Church Administrator, Business Manager, Operations Manager
- Elder, Deacon (if clearly administrative/leadership)

EXCLUDE if the title contains ANY of these (case-insensitive):
- teacher, faculty, instructor, professor, tutor, aide, para (instructional roles)
- counselor, counselling, psychologist, therapist (unless church staff counselor)
- admissions, admission, enrollment, registrar, recruiting, outreach
- marketing, communications, media, social media, pr, public relations
- athletic, athletics, coach, sports, pe, physical education
- secretary, administrative assistant, office manager, office admin, receptionist, executive assistant
- nurse, health office, nutrition, cafeteria, dining, food service
- youth minister (unless also has admin title), children's minister (unless also has admin title)
- worship leader, music director, choir director (unless also operations/admin)
- board, trustee, governance, chairman, vice-chair, treasurer (if board-only role)
- volunteer, intern, resident (trainee roles)

EXCEPTIONS:
- "Senior Pastor" or "Lead Pastor" or "Executive Pastor" → KEEP
- "Pastor & [something]" → KEEP (pastor is leadership)
- "Director of [Operations/Technology/Facilities]" → KEEP

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
        self.total_api_calls = 0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
    
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
                        {"role": "user", "content": user_message},
                    ],
                    temperature=0.0,
                    max_tokens=10,
                )

                self.total_api_calls += 1
                u = getattr(response, "usage", None)
                if u is not None:
                    self.total_prompt_tokens += int(getattr(u, "prompt_tokens", 0) or 0)
                    self.total_completion_tokens += int(
                        getattr(u, "completion_tokens", 0) or 0
                    )

                response_text = response.choices[0].message.content.strip().upper()
        
                # Parse response
                if "KEEP" in response_text:
                    return True
                elif "EXCLUDE" in response_text:
                    return False
                else:
                    from church_run_log import log_warn

                    log_warn(
                        f"Title filter unexpected LLM reply for {first_name} {last_name}: {response_text}"
                    )
                    return False

            except Exception as e:
                from church_run_log import log_warn

                error_str = str(e)
                is_rate_limit = "429" in error_str or "rate_limit" in error_str.lower()

                if is_rate_limit:
                    wait_seconds = 1.0
                    wait_match = re.search(
                        r"Please try again in (\d+)(ms|s)", error_str, re.IGNORECASE
                    )
                    if wait_match:
                        wait_value = int(wait_match.group(1))
                        wait_unit = wait_match.group(2).lower()
                        if wait_unit == "ms":
                            wait_seconds = (wait_value / 1000.0) + 0.5
                        else:
                            wait_seconds = wait_value + 0.5

                    if attempt < max_retries - 1:
                        log_warn(
                            f"Title filter rate limit (attempt {attempt + 1}/{max_retries}), wait {wait_seconds:.1f}s"
                        )
                        time.sleep(wait_seconds)
                        continue
                    log_warn("Title filter rate limit — excluding contact")
                    return False
                else:
                    if attempt < max_retries - 1:
                        wait_time = 2**attempt
                        log_warn(
                            f"Title filter error (attempt {attempt + 1}/{max_retries}): {e}, retry {wait_time}s"
                        )
                        time.sleep(wait_time)
                        continue
                    log_warn(f"Title filter LLM error: {e}")
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
        # Read contacts from Step 9
        df = pd.read_csv(input_csv)

        kept_contacts = []
        excluded_contacts = []

        for idx, row in df.iterrows():
            contact = {
                'first_name': row.get('first_name', ''),
                'last_name': row.get('last_name', ''),
                'title': row.get('title', ''),
                'email': row.get('email', ''),
                'phone': row.get('phone', ''),
                'church_name': row.get('church_name', ''),
                'source_url': row.get('source_url', '')
            }
            
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
        else:
            # Create empty CSV with headers
            pd.DataFrame(columns=['first_name', 'last_name', 'title', 'email', 'phone', 'church_name', 'source_url']).to_csv(output_csv, index=False)
        
        if output_excluded_csv and excluded_contacts:
            df_excluded = pd.DataFrame(excluded_contacts)
            df_excluded.to_csv(output_excluded_csv, index=False)
        
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Filter contacts by title using LLM')
    parser.add_argument('--input', required=True, help='Input CSV from Step 5')
    parser.add_argument('--output', default='step10_contacts_filtered.csv', help='Output CSV filename for filtered contacts')
    parser.add_argument('--output-excluded', default=None, help='Output CSV filename for excluded contacts (optional)')
    parser.add_argument('--model', default='gpt-4o-mini', help='Model to use (default: gpt-4o-mini)')
    args = parser.parse_args()
    
    # API key must come from OPENAI_API_KEY environment variable
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("ERROR: OPENAI_API_KEY environment variable not set")
        sys.exit(1)
    
    filterer = TitleFilter(api_key=api_key, model=args.model)
    filterer.filter_contacts(args.input, args.output, args.output_excluded)
