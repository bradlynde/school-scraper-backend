"""
STEP 8: CSV PARSING & EMAIL CLEANING
====================================
Parse LLM's CSV response and clean email addresses.

Input: CSV text from Step 7
Output: List of contact dictionaries with cleaned emails
"""

import csv
import io
import re
import pandas as pd
from typing import List, Dict


class CSVParser:
    """Parse CSV response from LLM and clean email addresses."""
    
    def clean_email(self, email: str) -> str:
        """
        Clean email address by removing special characters and validating format.
        
        Args:
            email: Raw email string (may contain special characters or invalid text)
        
        Returns:
            Cleaned email string, or empty string if invalid
        """
        if not email or pd.isna(email):
            return ''
        
        email = str(email).strip()
        
        # Handle UTF-8 encoding artifacts like "â€‹" which appears when zero-width space is mis-encoded
        # Try to decode and re-encode to fix encoding issues
        try:
            # If the string contains "â€‹" (common mis-encoding of zero-width space)
            if 'â€‹' in email:
                # Try to fix by removing the problematic sequence
                email = email.replace('â€‹', '')
            # Also try removing if it's the actual zero-width space character
            email = email.lstrip('\u200B\u200C\u200D\uFEFF')  # Zero-width space, zero-width non-joiner, etc.
        except:
            pass
        
        # Remove UTF-8 BOM and other encoding artifacts
        if email.startswith('\ufeff'):
            email = email[1:]
        
        # Remove any other common encoding artifacts at the start
        # Remove leading non-printable characters
        while email and not email[0].isprintable() and email[0] != '@':
            email = email[1:]
        
        # Remove any leading non-ASCII characters that aren't valid email characters
        # Keep only printable ASCII characters and valid email characters
        cleaned = ''
        found_at = False
        for char in email:
            # Always keep @ symbol
            if char == '@':
                found_at = True
                cleaned += char
            # Allow ASCII printable characters and common email characters
            elif ord(char) < 128 and (char.isprintable() or char in '._-+'):
                cleaned += char
            # Skip other non-ASCII characters (but only before @)
            # After @, we might have internationalized domain names, but for now we'll be strict
            elif found_at and ord(char) < 128:
                # After @, allow some non-ASCII if it's part of a valid domain
                # But for safety, we'll be conservative
                pass
        
        email = cleaned.strip()
        
        # Basic email format validation - must contain @ and have valid structure
        # This filters out things like "Bobcat Heavy Civil" or "ISAIAH'S PLACE | ASL & EQUINE ASSISTED LEARNING"
        if not email or '@' not in email:
            return ''
        
        # Split by @ to check domain
        parts = email.split('@')
        if len(parts) != 2:
            return ''
        
        local, domain = parts[0], parts[1]
        
        # Local part must not be empty and should be reasonable
        if not local or len(local) < 1:
            return ''
        
        # Domain must contain at least one dot and have valid TLD
        if '.' not in domain or len(domain.split('.')[-1]) < 2:
            return ''
        
        # Additional check: if it looks like random text (too many spaces, special chars, etc.)
        # This catches cases where non-email text got into the field
        if ' ' in email or len(email.split()) > 1:
            return ''  # Emails shouldn't have spaces
        
        # Final regex validation for proper email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            return ''
        
        return email.lower()
    
    def parse_csv_response(self, csv_text: str) -> List[Dict]:
        """
        Parse CSV response from LLM into list of contact dictionaries
        
        Args:
            csv_text: CSV text from LLM
        
        Returns:
            List of contact dictionaries
        """
        contacts = []
        
        # Clean up response (remove markdown code blocks if present)
        csv_text = re.sub(r'^```csv\s*', '', csv_text, flags=re.MULTILINE)
        csv_text = re.sub(r'^```\s*', '', csv_text, flags=re.MULTILINE)
        csv_text = re.sub(r'\s*```$', '', csv_text, flags=re.MULTILINE)
        csv_text = csv_text.strip()
        
        # Parse CSV
        try:
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                # Helper function to safely get and strip CSV values (handles None)
                def safe_get(field_name, default=''):
                    value = row.get(field_name, default)
                    return value.strip() if value is not None else default
                
                # Convert CSV row to contact dict (new format: first_name, last_name separate)
                first_name = safe_get('First Name', '')
                last_name = safe_get('Last Name', '')
                
                # Clean email to remove special characters and validate format
                raw_email = safe_get('Email', '')
                cleaned_email = self.clean_email(raw_email)
                
                contact = {
                    'first_name': first_name,
                    'last_name': last_name,
                    'title': safe_get('Title', ''),
                    'email': cleaned_email,  # Use cleaned email
                    'phone': safe_get('Phone', '')
                }
                
                # Only add if has first name, last name, and title (email is optional)
                if contact['first_name'] and contact['last_name'] and contact['title']:
                    contacts.append(contact)
        except Exception as e:
            print(f"      WARNING: CSV parse error: {e}")
            print(f"      CSV preview: {csv_text[:500]}")
        
        return contacts


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse CSV response from LLM')
    parser.add_argument('--input', required=True, help='Input CSV file')
    args = parser.parse_args()
    
    with open(args.input, 'r', encoding='utf-8') as f:
        csv_text = f.read()
    
    parser_obj = CSVParser()
    contacts = parser_obj.parse_csv_response(csv_text)
    
    print(f"Parsed {len(contacts)} contacts:")
    for contact in contacts:
        print(f"  {contact['first_name']} {contact['last_name']} - {contact['title']} - {contact['email']}")

