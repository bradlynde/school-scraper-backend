"""
STREAMING PIPELINE ORCHESTRATOR
================================
Process one lead through all steps, then write to final CSV only.
No intermediate CSV files - all in memory streaming.

Usage:
    python Pipeline.py --api-key KEY --openai-key OPENAI_KEY
"""

import os
import sys
import csv
import argparse
import time
from typing import List, Iterator, Optional, Dict
from datetime import datetime
from pathlib import Path
import traceback
import importlib.util

# Import shared models
from assets.shared.models import School, Page, PageContent, Contact

# Import streaming steps
# Handle hyphens in filenames using importlib.util
_script_dir = Path(__file__).parent
_steps_dir = _script_dir / "steps"
step1_spec = importlib.util.spec_from_file_location("step1_search", _steps_dir / "step1-search.py")
step1_module = importlib.util.module_from_spec(step1_spec)
step1_spec.loader.exec_module(step1_module)
SchoolSearcher = step1_module.SchoolSearcher

step2_spec = importlib.util.spec_from_file_location("step2_school_filter", _steps_dir / "step2-school_filter.py")
step2_module = importlib.util.module_from_spec(step2_spec)
step2_spec.loader.exec_module(step2_module)
filter_school = step2_module.filter_school
LLMSchoolFilter = step2_module.LLMSchoolFilter


def load_counties_from_state(state: str) -> List[str]:
    """
    Load counties for a given state from assets/data/state_counties/{state}.txt
    
    Args:
        state: State name (e.g., 'texas', 'Texas', 'TEXAS')
        
    Returns:
        List of county names
        
    Raises:
        FileNotFoundError: If state file doesn't exist
    """
    # Normalize state name: lowercase, replace spaces with underscores
    state_normalized = state.lower().replace(' ', '_')
    
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    state_file = script_dir / 'assets' / 'data' / 'state_counties' / f'{state_normalized}.txt'
    
    if not state_file.exists():
        raise FileNotFoundError(
            f"State file not found: {state_file}\n"
            f"Please create assets/data/state_counties/{state_normalized}.txt with one county per line"
        )
    
    counties = []
    with open(state_file, 'r', encoding='utf-8') as f:
        for line in f:
            county = line.strip()
            # Skip empty lines and comments
            if county and not county.startswith('#'):
                counties.append(county)
    
    if not counties:
        raise ValueError(f"No counties found in {state_file}")
    
    return counties

# Import step classes (will need to refactor these to support streaming)
# For now, we'll import the existing classes and wrap them
# Handle hyphens in filenames using importlib.util
def load_module_with_hyphen(filename, module_name):
    """Load a Python module from a file with hyphens in the filename"""
    spec = importlib.util.spec_from_file_location(module_name, _steps_dir / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

step3 = load_module_with_hyphen('step3-discovery.py', 'step3_discovery')   # Step 3 – page discovery
step4 = load_module_with_hyphen('step4-crawler.py', 'step4_crawler')   # Step 4 – content collection
step5 = load_module_with_hyphen('step5-html_reduction.py', 'step5_html_reduction')   # Step 5 – HTML reduction
step6 = load_module_with_hyphen('step6-html_chunking.py', 'step6_html_chunking')   # Step 6 – HTML chunking
step7 = load_module_with_hyphen('step7-llm_parser.py', 'step7_llm_parser')   # Step 7 – LLM parsing
step8 = load_module_with_hyphen('step8-lead_cleaner.py', 'step8_lead_cleaner')   # Step 8 – CSV/email cleaning
step9 = load_module_with_hyphen('step9-lead_dedupe.py', 'step9_lead_dedupe')   # Step 9 – deduplication
step10 = load_module_with_hyphen('step10-lead_filter.py', 'step10_lead_filter')  # Step 10 – title filtering
step11_contact_splitter = load_module_with_hyphen('step11-contact_splitter.py', 'step11_contact_splitter')  # Step 11 – split contacts with/without emails
step12_hunter_io = load_module_with_hyphen('step12-enrichment.py', 'step12_enrichment')  # Step 12 – email enrichment (Hunter.io)
step13_final_compiler = load_module_with_hyphen('step13-compiler.py', 'step13_compiler')  # Step 13 – final CSV compilation


class StreamingPipeline:
    """Process leads through entire pipeline one at a time"""
    
    def __init__(
        self,
        google_api_key: str,
        openai_api_key: str,
        global_max_api_calls: int = 25,
        max_pages_per_school: int = 3,
        state: str = 'Texas',
        max_schools: int = None
    ):
        self.google_api_key = google_api_key
        self.openai_api_key = openai_api_key
        self.global_max_api_calls = global_max_api_calls
        self.max_pages_per_school = max_pages_per_school
        self._state = state
        self.max_schools = max_schools
        
        # Initialize step processors
        # Debug: Verify API key is being passed
        if not google_api_key or len(google_api_key) < 10:
            print(f"WARNING: Google API key appears invalid in Pipeline (length: {len(google_api_key) if google_api_key else 0})")
        self.school_searcher = SchoolSearcher(google_api_key, global_max_api_calls, max_schools=max_schools, target_state=state)
        
        # Initialize LLM school filter if OpenAI key provided
        if openai_api_key:
            try:
                self.llm_school_filter = LLMSchoolFilter(
                    api_key=openai_api_key,
                    target_state=state,  # Pass state to LLM filter
                    model="gpt-4o-mini",
                    batch_size=20
                )
            except Exception as e:
                print(f"WARNING: Could not initialize LLM school filter: {e}")
                self.llm_school_filter = None
        else:
            self.llm_school_filter = None
        
        self.page_discoverer = step3.PageDiscoverer(timeout=10, max_retries=1)
        self.content_collector = step4.ContentCollector(timeout=10, max_retries=1, use_selenium=True)
        self.html_reducer = step5.HTMLReducer()
        self.html_chunker = step6.HTMLChunker()
        self.llm_parser = step7.LLMParser(openai_api_key, model="gpt-4o-mini")
        self.csv_parser = step8.CSVParser()
        self.deduplicator = step9.ContactDeduplicator(email_cleaner=self.csv_parser.clean_email)
        self.title_filter = step10.TitleFilter(openai_api_key, model="gpt-4o-mini")
        self.contact_splitter = step11_contact_splitter.ContactSplitter()
        self.final_compiler = step13_final_compiler.FinalCompiler()
        self.enable_hunter_io = os.getenv('HUNTER_IO_API_KEY') is not None
        
        # Results accumulator
        self.all_contacts = []
        # Track unique contacts for progress display (key: normalized contact identifier)
        self.unique_contacts_set = set()
        self.stats = {
            'schools_discovered': 0,
            'schools_filtered_out': 0,
            'schools_processed': 0,
            'pages_discovered': 0,
            'pages_collected': 0,
            'contacts_extracted': 0,
            'contacts_with_emails': 0,
        }
    
    def _get_contact_key(self, contact: Contact) -> str:
        """
        Generate a unique key for a contact for deduplication tracking.
        Uses email if available, otherwise name + school.
        
        Args:
            contact: Contact object
            
        Returns:
            Normalized string key for uniqueness tracking
        """
        # If contact has email, use that as the key (normalized)
        if contact.email:
            email_key = contact.email.lower().strip()
            if email_key:
                return f"email:{email_key}"
        
        # Otherwise, use name + school (normalized)
        first_name = (contact.first_name or '').lower().strip()
        last_name = (contact.last_name or '').lower().strip()
        school_name = (contact.school_name or '').lower().strip()
        
        # Only create key if we have at least first or last name
        if first_name or last_name:
            return f"name:{first_name}|{last_name}|{school_name}"
        
        # Fallback: use all available fields
        return f"fallback:{first_name}|{last_name}|{school_name}|{contact.title or ''}"
    
    def process_single_lead(self, school: School) -> List[Contact]:
        """
        Process one school through all steps.
        Returns list of Contact objects extracted from this school.
        """
        print(f"\nProcessing: {school.name} ({school.county})")
        
        # Step 2: Filter school
        filter_result = filter_school(school, target_state=self._state, llm_filter=self.llm_school_filter)
        if isinstance(filter_result, tuple):
            filtered_school, filter_reason = filter_result
        else:
            # Backward compatibility
            filtered_school, filter_reason = filter_result, None
        
        if not filtered_school:
            reason_msg = f" - {filter_reason}" if filter_reason else ""
            print(f"  ❌ Filtered out{reason_msg}")
            self.stats['schools_filtered_out'] += 1
            return []
        
        if not filtered_school.website:
            print("  ⚠️  No website")
            return []
        
        # Step 3: Discover pages
        try:
            pages = self._discover_pages_for_school(filtered_school)
            self.stats['pages_discovered'] += len(pages)
            
            if not pages:
                print("  ⚠️  No pages found")
                return []
        except Exception as e:
            print(f"  ❌ Error: {e}")
            traceback.print_exc()
            return []
        
        # Step 4: Collect content
        page_contents = []
        for page in pages[:self.max_pages_per_school]:  # Limit pages per school
            try:
                content = self._collect_content_for_page(page)
                if content:
                    page_contents.append(content)
                    self.stats['pages_collected'] += 1
            except Exception as e:
                continue
        
        if not page_contents:
            print("  ⚠️  No content collected")
            return []
        
        # Step 5: Parse content with LLM
        all_contacts = []
        for page_content in page_contents:
            try:
                contacts = self._parse_content_with_llm(page_content, filtered_school)
                all_contacts.extend(contacts)
            except Exception as e:
                continue
        
        if all_contacts:
            print(f"  ✓ {len(all_contacts)} contacts extracted")
        else:
            print("  ⚠️  No contacts")
        
        self.stats['schools_processed'] += 1
        return all_contacts
    
    def _discover_pages_for_school(self, school: School) -> List[Page]:
        """
        Discover pages for a single school using step3's discover_pages method.
        """
        if not school.website:
            return []
        
        pages = []
        try:
            # Use step3's discover_pages method
            discovered_pages = self.page_discoverer.discover_pages(
                school_name=school.name,
                base_url=school.website,
                max_depth=3,
                max_pages_per_school=self.max_pages_per_school,
                top_pages_limit=self.max_pages_per_school
            )
            
            # Convert dicts to Page objects
            for page_dict in discovered_pages:
                pages.append(Page(
                    url=page_dict['url'],
                    school_name=school.name,
                    school_place_id=school.place_id,
                    school_website=school.website,
                    priority_score=page_dict.get('priority_score', 0),
                    page_title=page_dict.get('title'),
                    discovered_via=page_dict.get('url')  # Could enhance this
                ))
        except Exception as e:
            print(f"    Error in page discovery: {e}")
            import traceback
            traceback.print_exc()
        
        return pages
    
    def _collect_content_for_page(self, page: Page) -> Optional[PageContent]:
        """
        Collect content for a single page using step4's collect_page_content method.
        """
        try:
            # Use step4's collect_page_content method (requires school_name and url)
            result = self.content_collector.collect_page_content(page.school_name, page.url)
            
            if not result:
                return None
            
            # Extract HTML content and metadata from result dict
            html_content = result.get('html_content', '')
            fetch_method = result.get('fetch_method', 'unknown')
            email_count = result.get('email_count', 0)
            
            return PageContent(
                url=page.url,
                school_name=page.school_name,
                html_content=html_content or '',
                email_count=email_count,
                has_emails=email_count > 0,
                collection_method=fetch_method
            )
        except Exception as e:
            print(f"    Error collecting content: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _parse_content_with_llm(self, page_content: PageContent, school: School) -> List[Contact]:
        """
        Parse content with LLM using step5's reduction and chunking logic.
        """
        if not page_content.html_content:
            return []
        
        try:
            # Step 5: Reduce HTML to contact-focused sections
            reduced_html = self.html_reducer.reduce_html(page_content.html_content)
            if not reduced_html:
                return []
            
            # Step 6: Chunk HTML if needed
            chunks = self.html_chunker.chunk_html(reduced_html, max_chunk_size=50000)  # Increased from 20k to 50k for cost optimization
            
            # Step 7-8: LLM parsing + CSV parsing
            page_contacts_dicts = []
            for chunk_idx, chunk in enumerate(chunks):
                csv_text = self.llm_parser.parse_with_llm(
                    chunk,
                    school.name,
                    page_content.url,
                    max_retries=1
                )
                
                if csv_text:
                    chunk_contacts = self.csv_parser.parse_csv_response(csv_text)
                    for contact in chunk_contacts:
                        contact['school_name'] = school.name
                        contact['source_url'] = page_content.url
                    page_contacts_dicts.extend(chunk_contacts)
                
                if chunk_idx < len(chunks) - 1:
                    time.sleep(1.0)
            
            if not page_contacts_dicts:
                return []
            
            # Step 9: Deduplicate contacts from this page
            deduped_contacts = self.deduplicator.deduplicate_contacts(page_contacts_dicts)
            
            # Step 10 (previously Step 11): Filter contacts by title
            filtered_contacts = []
            for contact_dict in deduped_contacts:
                filter_payload = {
                    'first_name': contact_dict.get('first_name', ''),
                    'last_name': contact_dict.get('last_name', ''),
                    'title': contact_dict.get('title', ''),
                    'email': contact_dict.get('email', ''),
                    'phone': contact_dict.get('phone', '')
                }
                
                should_keep = self.title_filter.filter_contact(filter_payload, max_retries=1)
                if should_keep:
                    contact = Contact(
                        first_name=contact_dict.get('first_name', '').strip(),
                        last_name=contact_dict.get('last_name', '').strip(),
                        title=contact_dict.get('title', ''),
                        email=contact_dict.get('email') or None,
                        phone=contact_dict.get('phone') or None,
                        school_name=school.name,
                        source_url=page_content.url
                    )
                    filtered_contacts.append(contact)
            
            return filtered_contacts
        except Exception as e:
            print(f"  ❌ LLM parsing error: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def run(
        self,
        counties: List[str] = None,
        batch_size: int = 0,
        output_csv: str = "final_contacts.csv"
    ):
        """
        Run the streaming pipeline.
        Processes schools one at a time through all steps.
        """
        print(f"\nPipeline started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Output: {output_csv}\n")
        
        # Step 1: Discover schools (generator - yields one at a time)
        print("Discovering schools...")
        schools_discovered = 0
        
        # Load counties from state file if not provided
        if not counties:
            if not hasattr(self, '_state') or not self._state:
                raise ValueError("--state parameter is required for county-based search")
            counties = load_counties_from_state(self._state)
        
        # Use 6 search terms per county (original 5 + Episcopal)
        max_search_terms_per_county = 6
        
        school_generator = self.school_searcher.discover_schools(
            counties=counties,
            state=self._state or 'Texas',
            batch_size=batch_size,
            max_search_terms=max_search_terms_per_county
        )
        
        for school in school_generator:
            schools_discovered += 1
            self.stats['schools_discovered'] = schools_discovered
            
            # Process this school through all steps
            contacts = self.process_single_lead(school)
            
            # Track unique contacts as we add them
            unique_before = len(self.unique_contacts_set)
            new_unique_count = 0
            for contact in contacts:
                contact_key = self._get_contact_key(contact)
                if contact_key not in self.unique_contacts_set:
                    self.unique_contacts_set.add(contact_key)
                    new_unique_count += 1
            
            # Accumulate contacts (keep full list for final deduplication)
            self.all_contacts.extend(contacts)
            
            # Print progress with unique count
            total_contacts = len(self.all_contacts)
            unique_contacts = len(self.unique_contacts_set)
            if new_unique_count > 0:
                print(f"Progress: {schools_discovered} schools | {self.stats['schools_processed']} processed | {unique_contacts} contacts (+{new_unique_count})")
            else:
                print(f"Progress: {schools_discovered} schools | {self.stats['schools_processed']} processed | {unique_contacts} contacts")
        
        # Flush any pending LLM filter batches
        if self.llm_school_filter:
            self.llm_school_filter.flush()
        
        # Generate filename for county-level output (will be aggregated later)
        if not output_csv or output_csv == "final_contacts.csv":
            output_csv = "final_contacts.csv"
        
        # Write raw contacts to CSV (Steps 11, 12, 13 will run globally after all counties complete)
        # This is per-county output that will be aggregated and enriched later
        if self.all_contacts:
            self._write_final_csv(self.all_contacts, output_csv)
        else:
            print(f"No contacts to write to {output_csv}")
        
        # Update stats
        self.stats['contacts_extracted'] = len(self.all_contacts)
        self.stats['unique_contacts'] = len(self.unique_contacts_set)
        
        # Count contacts with/without emails for stats (but don't process them yet)
        contacts_with_emails = [c for c in self.all_contacts if c.has_email()]
        contacts_without_emails = [c for c in self.all_contacts if not c.has_email()]
        self.stats['contacts_with_emails'] = len(contacts_with_emails)
        self.stats['contacts_without_emails'] = len(contacts_without_emails)
        
        # Print final summary
        self._print_summary()
    
    def cleanup(self):
        """Cleanup all pipeline resources, especially Selenium drivers"""
        try:
            # Cleanup ContentCollector's Selenium driver
            if hasattr(self, 'content_collector') and self.content_collector:
                self.content_collector.cleanup()
        except Exception as e:
            # Silently handle cleanup errors - don't crash if cleanup fails
            print(f"    Warning: Error during pipeline cleanup: {e}")
    
    def __del__(self):
        """Destructor - safety net to ensure resources are cleaned up"""
        try:
            self.cleanup()
        except:
            # Ignore errors in destructor
            pass
    
    def _write_final_csv(self, contacts: List[Contact], filename: str):
        """Write contacts to final CSV file"""
        if not contacts:
            print(f"No contacts to write to {filename}")
            return
        
        fieldnames = ['first_name', 'last_name', 'title', 'email', 'phone', 
                     'school_name', 'source_url']
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for contact in contacts:
                writer.writerow(contact.to_dict())
        
        print(f"✓ Saved {len(contacts)} contacts to {filename}")
    
    
    def _print_summary(self):
        """Print final pipeline summary"""
        print(f"\n✓ Pipeline complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Schools: {self.stats['schools_discovered']} discovered, {self.stats['schools_processed']} processed")
        if self.stats['contacts_extracted'] > 0:
            print(f"  Contacts: {self.stats['unique_contacts']} unique ({self.stats['contacts_with_emails']} with emails)")
        else:
            print("  Contacts: 0")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Streaming Pipeline - One Lead Through Entire System')
    parser.add_argument('--google-api-key', required=True, help='Google Places API key (legacy)')
    parser.add_argument('--openai-api-key', required=True, help='OpenAI API key')
    parser.add_argument('--state', required=True, help='State to search (e.g., texas, california) - loads counties from assets/data/state_counties/{state}.txt')
    parser.add_argument('--global-max-api-calls', type=int, default=3000, help='Global API call cap (default: 3000 for full Texas run, ~254 counties × 5 terms + pagination + Place Details)')
    parser.add_argument('--max-schools', type=int, default=None, help='Maximum number of schools to discover (default: unlimited)')
    parser.add_argument('--county', action='append', default=None, help='County to process (e.g., "Denton"). Can be specified multiple times for multiple counties. If not provided, processes all counties in state.')
    parser.add_argument('--batch-size', type=int, default=0, help='Number of counties to search (0 = all counties in state)')
    parser.add_argument('--max-pages-per-school', type=int, default=3, help='Max pages per school (default: 3)')
    parser.add_argument('--output', default=None, help='Output CSV. If not provided, will generate based on state name (e.g., "Texas leads.csv")')
    
    args = parser.parse_args()
    
    # Debug: Check if API keys are being parsed correctly
    if not args.google_api_key or len(args.google_api_key) < 10:
        print(f"ERROR: Google API key not provided or invalid (length: {len(args.google_api_key) if args.google_api_key else 0})")
        print(f"DEBUG: args.google_api_key value: '{args.google_api_key[:20] if args.google_api_key else 'None'}...'")
        sys.exit(1)
    
    # Create pipeline
    pipeline = StreamingPipeline(
        google_api_key=args.google_api_key,
        openai_api_key=args.openai_api_key,
        global_max_api_calls=args.global_max_api_calls,
        max_pages_per_school=args.max_pages_per_school,
        state=args.state,
        max_schools=args.max_schools
    )
    
    # Determine counties to process
    counties_to_process = None
    if args.county:
        counties_to_process = args.county  # Already a list from action='append'
        if len(counties_to_process) == 1:
            print(f"Processing single county: {counties_to_process[0]}")
        else:
            print(f"Processing {len(counties_to_process)} counties: {', '.join(counties_to_process)}")
    
    # Run pipeline
    try:
        pipeline.run(
            counties=counties_to_process,  # Will use all counties if None, or single county if specified
            batch_size=args.batch_size,
            output_csv=args.output
        )
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        print(f"Partial results: {len(pipeline.all_contacts)} contacts extracted")
        if pipeline.all_contacts:
            partial_output = args.output or f"partial_{pipeline._state or 'Texas'}_leads.csv"
            pipeline._write_final_csv(
                pipeline.all_contacts,
                partial_output
            )
    except Exception as e:
        print(f"\n\nPipeline failed: {e}")
        traceback.print_exc()
        sys.exit(1)

