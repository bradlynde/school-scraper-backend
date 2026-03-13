"""
STEP 9: DEDUPLICATION
=====================
Remove duplicate contacts, keeping the most complete version.

Input: List of contact dictionaries
Output: Deduplicated list of contacts
"""

import re
from typing import List, Dict


class ContactDeduplicator:
    """Deduplicate contacts by email (or name+domain if no email)."""
    
    def __init__(self, email_cleaner=None):
        """
        Initialize deduplicator
        
        Args:
            email_cleaner: Optional function to clean emails (from step8)
        """
        self.email_cleaner = email_cleaner
    
    def clean_email(self, email: str) -> str:
        """Clean email using provided cleaner or basic validation"""
        if self.email_cleaner:
            return self.email_cleaner(email)
        return email.lower().strip() if email else ''
    
    def deduplicate_contacts(self, contacts: List[Dict]) -> List[Dict]:
        """
        Deduplicate contacts by email (or name+domain if no email)
        Keep the most complete version
        
        Args:
            contacts: List of contact dictionaries
            
        Returns:
            Deduplicated list
        """
        seen = {}
        
        for contact in contacts:
            # Use cleaned email for deduplication
            raw_email = contact.get('email', '')
            email = self.clean_email(raw_email) if raw_email else ''
            
            # Build name from first_name and last_name
            first_name = contact.get('first_name', '').strip()
            last_name = contact.get('last_name', '').strip()
            name = f"{first_name} {last_name}".strip().lower()
            
            # Primary key: email if present
            if email:
                key = email
            else:
                # Fallback: name + domain (if email domain is present in other fields)
                domain = ''
                if 'source_url' in contact:
                    # Extract domain from URL
                    url = contact['source_url']
                    match = re.search(r'https?://([^/]+)', url)
                    if match:
                        domain = match.group(1)
                key = f"{name}|{domain}"
            
            # If we've seen this key before, keep the more complete version
            if key in seen:
                existing = seen[key]
                # Count non-empty fields
                existing_fields = sum(1 for v in existing.values() if v and str(v).strip())
                new_fields = sum(1 for v in contact.values() if v and str(v).strip())
                
                # Keep the one with more fields, or longer title if equal
                if new_fields > existing_fields:
                    seen[key] = contact
                elif new_fields == existing_fields:
                    if len(contact.get('title', '')) > len(existing.get('title', '')):
                        seen[key] = contact
            else:
                seen[key] = contact
        
        return list(seen.values())


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Deduplicate contacts')
    parser.add_argument('--input', required=True, help='Input JSON file with contacts')
    parser.add_argument('--output', default='deduplicated.json', help='Output JSON file')
    args = parser.parse_args()
    
    import json
    with open(args.input, 'r', encoding='utf-8') as f:
        contacts = json.load(f)
    
    deduplicator = ContactDeduplicator()
    deduplicated = deduplicator.deduplicate_contacts(contacts)
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(deduplicated, f, indent=2)
    
    print(f"Deduplicated: {len(contacts)} → {len(deduplicated)} contacts")

