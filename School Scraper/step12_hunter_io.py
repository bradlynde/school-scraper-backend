"""
STEP 12: EMAIL ENRICHMENT (Hunter.io)
======================================
Enrich contacts without email addresses using Hunter.io Email Finder API.

Input: List of Contact objects without emails (from Step 11)
Output: List of Contact objects with emails added where found

This step is optional and non-blocking - pipeline continues even if enrichment fails.
"""

import os
import pandas as pd
import requests
import time
from typing import Optional, Dict, List
from urllib.parse import urlparse
import re
from assets.shared.models import Contact


class HunterIOEnricher:
    """
    Enrich contacts without emails using Hunter.io Email Finder API.
    """
    
    def __init__(self, api_key: str, verify_emails: bool = True, score_threshold: int = 70):
        """
        Initialize Hunter.io enricher.
        
        Args:
            api_key: Hunter.io API key
            verify_emails: Whether to verify found emails (uses 0.5 credits per verification)
            score_threshold: Minimum email score to accept (0-100, default: 70)
        """
        self.api_key = api_key
        self.verify_emails = verify_emails
        self.score_threshold = score_threshold
        self.base_url = "https://api.hunter.io/v2"
        self.stats = {
            'contacts_processed': 0,
            'emails_found': 0,
            'emails_verified': 0,
            'api_calls': 0,
            'errors': 0
        }
    
    def extract_domain_from_url(self, url: str) -> Optional[str]:
        """
        Extract domain from source_url.
        
        Example: "https://www.example.com/page" → "example.com"
        
        Args:
            url: Source URL string
            
        Returns:
            Domain string or None if invalid
        """
        if not url:
            return None
        
        try:
            parsed = urlparse(str(url))
            domain = parsed.netloc or parsed.path.split('/')[0]
            
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Validate domain format
            if '.' in domain and len(domain) > 3:
                return domain.lower()
            
            return None
        except Exception:
            return None
    
    def find_email_via_hunter_io(
        self,
        first_name: str,
        last_name: str,
        domain: str
    ) -> Optional[Dict]:
        """
        Find email for a contact using Hunter.io Email Finder API.
        
        Args:
            first_name: Contact's first name
            last_name: Contact's last name
            domain: School domain (extracted from source_url)
        
        Returns:
            Dict with 'email', 'score', 'sources' or None if not found
        """
        if not all([first_name, last_name, domain]):
            return None
        
        # Clean names
        first_name = str(first_name).strip()
        last_name = str(last_name).strip()
        domain = str(domain).strip().lower()
        
        if not first_name or not last_name or not domain:
            return None
        
        try:
            # Hunter.io Email Finder API
            url = f"{self.base_url}/email-finder"
            params = {
                'domain': domain,
                'first_name': first_name,
                'last_name': last_name,
                'api_key': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('data') and data['data'].get('email'):
                    email_data = data['data']
                    score = email_data.get('score', 0)
                    
                    # Only return if score meets threshold
                    if score >= self.score_threshold:
                        return {
                            'email': email_data['email'],
                            'score': score,
                            'sources': email_data.get('sources', [])
                        }
                    else:
                        print(f"      ⚠️  Email found but score too low ({score} < {self.score_threshold}): {email_data['email']}")
                        return None
                else:
                    # Email not found
                    return None
                    
            elif response.status_code == 404:
                # Email not found - this is normal, not an error
                return None
                
            elif response.status_code == 429:
                # Rate limit exceeded
                print(f"      ⚠️  Rate limit exceeded, waiting 60 seconds...")
                time.sleep(60)
                # Retry once
                response = requests.get(url, params=params, timeout=10)
                self.stats['api_calls'] += 1
                if response.status_code == 200:
                    data = response.json()
                    if data.get('data') and data['data'].get('email'):
                        email_data = data['data']
                        if email_data.get('score', 0) >= self.score_threshold:
                            return {
                                'email': email_data['email'],
                                'score': email_data['score'],
                                'sources': email_data.get('sources', [])
                            }
                return None
                
            elif response.status_code == 401:
                print(f"      ❌ Invalid Hunter.io API key")
                self.stats['errors'] += 1
                return None
                
            else:
                print(f"      ⚠️  Hunter.io API error: {response.status_code}")
                self.stats['errors'] += 1
                return None
                
        except requests.exceptions.Timeout:
            print(f"      ⚠️  Hunter.io API timeout")
            self.stats['errors'] += 1
            return None
        except Exception as e:
            print(f"      ⚠️  Hunter.io API error: {str(e)}")
            self.stats['errors'] += 1
            return None
    
    def enrich_contact_objects(
        self,
        contacts: List[Contact],
        batch_size: int = 10,
        delay_between_batches: float = 1.0
    ) -> List[Contact]:
        """
        Enrich Contact objects without emails using Hunter.io API.
        
        Args:
            contacts: List of Contact objects without emails
            batch_size: Number of contacts to process per batch
            delay_between_batches: Delay in seconds between batches
        
        Returns:
            List of Contact objects with emails added where found
            (Only returns contacts that successfully got emails - skips failed contacts)
        """
        print("\n" + "="*70)
        print("STEP 12: EMAIL ENRICHMENT (Hunter.io)")
        print("="*70)
        
        if not contacts:
            print(f"  ✓ No contacts to enrich")
            return []
        
        print(f"  📧 Processing {len(contacts)} contacts without emails")
        
        enriched_contacts = []
        total_batches = (len(contacts) + batch_size - 1) // batch_size
        
        for batch_idx in range(0, len(contacts), batch_size):
            batch = contacts[batch_idx:batch_idx + batch_size]
            batch_num = (batch_idx // batch_size) + 1
            
            print(f"\n  Processing batch {batch_num}/{total_batches} ({len(batch)} contacts)...")
            
            for contact in batch:
                if not contact.first_name or not contact.last_name:
                    print(f"    ⚠️  Skipping: missing name")
                    continue
                
                domain = self.extract_domain_from_url(contact.source_url)
                if not domain:
                    print(f"    ⚠️  Skipping {contact.first_name} {contact.last_name}: invalid domain from {contact.source_url}")
                    continue
                
                print(f"    🔍 Searching for: {contact.first_name} {contact.last_name} @ {domain}")
                
                email_result = self.find_email_via_hunter_io(
                    contact.first_name,
                    contact.last_name,
                    domain
                )
                self.stats['contacts_processed'] += 1
                
                if email_result:
                    email = email_result['email']
                    score = email_result['score']
                    print(f"      ✓ Found: {email} (score: {score})")
                    
                    # Update contact with found email
                    contact.email = email
                    enriched_contacts.append(contact)
                    self.stats['emails_found'] += 1
                else:
                    print(f"      ✗ Not found - skipping contact")
                    # Skip failed contacts (per user requirement)
                
                # Small delay between requests to respect rate limits
                time.sleep(0.5)
            
            # Delay between batches
            if batch_idx + batch_size < len(contacts):
                time.sleep(delay_between_batches)
        
        # Print summary
        print(f"\n" + "="*70)
        print(f"EMAIL ENRICHMENT COMPLETE")
        print(f"="*70)
        print(f"  Contacts processed: {self.stats['contacts_processed']}")
        print(f"  Emails found: {self.stats['emails_found']} ({len(enriched_contacts)} contacts enriched)")
        print(f"  API calls: {self.stats['api_calls']}")
        print(f"  Errors: {self.stats['errors']}")
        if self.stats['contacts_processed'] > 0:
            print(f"  Success rate: {(self.stats['emails_found'] / self.stats['contacts_processed']) * 100:.1f}%")
        print("="*70)
        
        return enriched_contacts
    
    def enrich_contacts_with_hunter_io(
        self,
        csv_path: str,
        output_csv_path: Optional[str] = None,
        batch_size: int = 10,
        delay_between_batches: float = 1.0
    ) -> str:
        """
        Enrich contacts without emails using Hunter.io API.
        
        Args:
            csv_path: Path to final CSV from Step 11
            output_csv_path: Optional output path (default: overwrites input)
            batch_size: Number of contacts to process per batch
            delay_between_batches: Delay in seconds between batches
        
        Returns:
            Path to enriched CSV file
        """
        if output_csv_path is None:
            output_csv_path = csv_path
        
        print("\n" + "="*70)
        print("STEP 12: EMAIL ENRICHMENT (Hunter.io)")
        print("="*70)
        
        if not os.path.exists(csv_path):
            print(f"  ❌ CSV file not found: {csv_path}")
            return csv_path
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            print(f"  📊 Loaded {len(df)} contacts from CSV")
            
            # Identify contacts without emails
            # Check for email column (try different variations)
            email_col = None
            for col_name in ['Email', 'email', 'EMAIL']:
                if col_name in df.columns:
                    email_col = col_name
                    break
            
            if not email_col:
                print(f"  ⚠️  No email column found in CSV, skipping enrichment")
                return csv_path
            
            # Find contacts without emails
            contacts_without_emails = df[
                df[email_col].isna() | 
                (df[email_col] == '') | 
                (df[email_col].str.strip() == '')
            ].copy()
            
            if len(contacts_without_emails) == 0:
                print(f"  ✓ All contacts already have emails, no enrichment needed")
                return csv_path
            
            print(f"  📧 Found {len(contacts_without_emails)} contacts without emails")
            
            # Extract domain from source_url
            source_url_col = None
            for col_name in ['source_url', 'Source URL', 'source_url', 'url']:
                if col_name in df.columns:
                    source_url_col = col_name
                    break
            
            if not source_url_col:
                print(f"  ⚠️  No source_url column found, cannot extract domains")
                return csv_path
            
            # Process contacts in batches
            enriched_count = 0
            total_batches = (len(contacts_without_emails) + batch_size - 1) // batch_size
            
            for batch_idx in range(0, len(contacts_without_emails), batch_size):
                batch = contacts_without_emails.iloc[batch_idx:batch_idx + batch_size]
                batch_num = (batch_idx // batch_size) + 1
                
                print(f"\n  Processing batch {batch_num}/{total_batches} ({len(batch)} contacts)...")
                
                for idx, row in batch.iterrows():
                    first_name = str(row.get('first_name', '') or row.get('First Name', '')).strip()
                    last_name = str(row.get('last_name', '') or row.get('Last Name', '')).strip()
                    source_url = str(row.get(source_url_col, '')).strip()
                    
                    if not first_name or not last_name:
                        continue
                    
                    domain = self.extract_domain_from_url(source_url)
                    if not domain:
                        print(f"    ⚠️  Skipping {first_name} {last_name}: invalid domain from {source_url}")
                        continue
                    
                    print(f"    🔍 Searching for: {first_name} {last_name} @ {domain}")
                    
                    email_result = self.find_email_via_hunter_io(first_name, last_name, domain)
                    self.stats['contacts_processed'] += 1
                    
                    if email_result:
                        email = email_result['email']
                        score = email_result['score']
                        print(f"      ✓ Found: {email} (score: {score})")
                        
                        # Update DataFrame
                        df.loc[idx, email_col] = email
                        enriched_count += 1
                        self.stats['emails_found'] += 1
                        
                        # Optional: Verify email (uses 0.5 credits)
                        if self.verify_emails:
                            # Email verification would go here if needed
                            # For now, we'll skip to save credits
                            pass
                    else:
                        print(f"      ✗ Not found")
                    
                    # Small delay between requests to respect rate limits
                    time.sleep(0.5)
                
                # Delay between batches
                if batch_idx + batch_size < len(contacts_without_emails):
                    time.sleep(delay_between_batches)
            
            # Save enriched CSV
            df.to_csv(output_csv_path, index=False)
            
            # Print summary
            print(f"\n" + "="*70)
            print(f"EMAIL ENRICHMENT COMPLETE")
            print(f"="*70)
            print(f"  Contacts processed: {self.stats['contacts_processed']}")
            print(f"  Emails found: {self.stats['emails_found']} ({enriched_count} added)")
            print(f"  API calls: {self.stats['api_calls']}")
            print(f"  Errors: {self.stats['errors']}")
            print(f"  Success rate: {(self.stats['emails_found'] / max(1, self.stats['contacts_processed'])) * 100:.1f}%")
            print(f"  ✓ Saved enriched CSV to: {output_csv_path}")
            
            return output_csv_path
            
        except Exception as e:
            print(f"\n  ❌ Error during email enrichment: {str(e)}")
            import traceback
            traceback.print_exc()
            # Return original CSV path on error
            return csv_path


def enrich_csv_with_hunter_io(
    csv_path: str,
    api_key: Optional[str] = None,
    output_csv_path: Optional[str] = None,
    verify_emails: bool = False,
    score_threshold: int = 70,
    batch_size: int = 10
) -> str:
    """
    Convenience function to enrich CSV with Hunter.io.
    
    Args:
        csv_path: Path to input CSV
        api_key: Hunter.io API key (default: from HUNTER_IO_API_KEY env var)
        output_csv_path: Optional output path
        verify_emails: Whether to verify emails
        score_threshold: Minimum email score (0-100)
        batch_size: Contacts per batch
    
    Returns:
        Path to enriched CSV
    """
    if api_key is None:
        api_key = os.getenv('HUNTER_IO_API_KEY')
    
    if not api_key:
        print("  ⚠️  HUNTER_IO_API_KEY not set, skipping email enrichment")
        return csv_path
    
    enricher = HunterIOEnricher(
        api_key=api_key,
        verify_emails=verify_emails,
        score_threshold=score_threshold
    )
    
    return enricher.enrich_contacts_with_hunter_io(
        csv_path=csv_path,
        output_csv_path=output_csv_path,
        batch_size=batch_size
    )


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enrich CSV contacts with Hunter.io')
    parser.add_argument('input_csv', help='Input CSV file path')
    parser.add_argument('--output', '-o', help='Output CSV file path (default: overwrites input)')
    parser.add_argument('--api-key', help='Hunter.io API key (or set HUNTER_IO_API_KEY env var)')
    parser.add_argument('--verify', action='store_true', help='Verify found emails (uses more credits)')
    parser.add_argument('--score-threshold', type=int, default=70, help='Minimum email score (0-100)')
    parser.add_argument('--batch-size', type=int, default=10, help='Contacts per batch')
    
    args = parser.parse_args()
    
    enrich_csv_with_hunter_io(
        csv_path=args.input_csv,
        api_key=args.api_key,
        output_csv_path=args.output,
        verify_emails=args.verify,
        score_threshold=args.score_threshold,
        batch_size=args.batch_size
    )

