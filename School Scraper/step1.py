"""
STEP 1: SCHOOL SEARCH WITH GOOGLE PLACES API (NEW) - STREAMING VERSION
==========================================================================
Search for schools using NEW Places API Essentials tier and YIELD them one at a time.

No CSV writing - yields School objects for streaming pipeline.

Uses New Places API Essentials tier (cost-effective): places.googleapis.com/v1/places:searchText

Only requests Essentials-tier fields to ensure lowest pricing.
"""

import requests
import time
from datetime import datetime
from typing import Iterator, List, Dict, Tuple, Optional
import random
import re
from assets.shared.models import School

class SchoolSearcher:
    """Search for schools using New Google Places API Essentials tier, yields School objects"""
    
    def __init__(self, api_key: str, global_max_api_calls: int = None, max_schools: int = None, target_state: str = 'texas'):
        # Debug: Verify API key is received
        if not api_key or len(api_key) < 10:
            print(f"WARNING: API key appears invalid in SchoolSearcher.__init__ (length: {len(api_key) if api_key else 0})")
        self.api_key = api_key
        self.global_max_api_calls = global_max_api_calls
        self.max_schools = max_schools
        
        # Normalize target_state: extract base state name (e.g., "texas" from "texas_ultra_test")
        normalized = target_state.lower().replace(' ', '_')
        # Extract base state name (before first underscore if it exists and is not a state name)
        # Check if the base name (before underscore) is a valid state
        base_state = normalized.split('_')[0] if '_' in normalized else normalized
        
        # State name to abbreviation mapping
        self.STATE_ABBREVIATIONS = {
            'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
            'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
            'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
            'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
            'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
            'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
            'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
            'new_hampshire': 'NH', 'new_jersey': 'NJ', 'new_mexico': 'NM', 'new_york': 'NY',
            'north_carolina': 'NC', 'north_dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
            'oregon': 'OR', 'pennsylvania': 'PA', 'rhode_island': 'RI', 'south_carolina': 'SC',
            'south_dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
            'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west_virginia': 'WV',
            'wisconsin': 'WI', 'wyoming': 'WY'
        }
        
        # Use base_state for abbreviation lookup (e.g., "texas" not "texas_ultra_test")
        # If base_state is not in the mapping, try the full normalized name
        if base_state in self.STATE_ABBREVIATIONS:
            self.target_state = base_state
        elif normalized in self.STATE_ABBREVIATIONS:
            self.target_state = normalized
        else:
            self.target_state = base_state  # Use base state even if not found (for logging)
        
        # New Places API endpoints (Essentials tier)
        self.text_search_url = "https://places.googleapis.com/v1/places:searchText"
        self.place_details_url_template = "https://places.googleapis.com/v1/places/{}"
        self.seen_place_ids = set()
        
        # Essentials-tier field mask (only request fields available in Essentials tier)
        # This ensures we're billed at Essentials pricing, not Pro/Enterprise
        # Field names must use 'places.' prefix for New Places API
        self.essentials_fields = [
            "places.id",
            "places.displayName",
            "places.formattedAddress",
            "places.location",
            "places.websiteUri",
            "places.nationalPhoneNumber",
            "places.internationalPhoneNumber",
            "places.businessStatus",
            "places.rating",
            "places.userRatingCount",
            "places.types",
            "places.primaryType"
        ]
        self.stats = {
            'counties_searched': 0,
            'total_api_calls': 0,
            'total_schools_found': 0,
            'schools_with_websites': 0,
            'non_state_skipped': 0
        }
        
        # Get state info using normalized target_state
        self.state_abbrev = self.STATE_ABBREVIATIONS.get(self.target_state, '')
        self.full_state_name = self.target_state.replace('_', ' ').title()

    def _hit_global_limit(self) -> bool:
        """Check if global API call limit or school limit has been reached"""
        api_limit_hit = (
            self.global_max_api_calls is not None and
            self.stats['total_api_calls'] >= self.global_max_api_calls
        )
        school_limit_hit = (
            self.max_schools is not None and
            self.stats['total_schools_found'] >= self.max_schools
        )
        return api_limit_hit or school_limit_hit

    def _extract_state_and_county_new(self, address: str, location: Dict = None) -> Tuple[str, str]:
        """
        Extract state and county from New API response.
        New API provides formattedAddress, we parse it or use location data.
        """
        state_value = ''
        county_value = ''
        
        # Parse formatted address (e.g., "123 Main St, Austin, TX 78701, USA")
        if address:
            # Look for state abbreviation pattern
            state_match = re.search(r',\s*([A-Z]{2})\s+\d{5}', address)
            if state_match:
                state_value = state_match.group(1)
            
            # Try to extract county from address (may not always be present)
            # Counties often appear before the city
            parts = address.split(',')
            if len(parts) >= 2:
                # County might be in the part before city
                potential_county = parts[-3].strip() if len(parts) >= 3 else ''
                if 'County' in potential_county:
                    county_value = potential_county.replace('County', '').strip()
        
        return state_value, county_value

    def _is_state_result(self, detected_state: str, formatted_address: str) -> bool:
        """Determine if the result belongs to the target state"""
        if detected_state:
            normalized_state = detected_state.strip().lower()
            # Check against abbreviation, normalized name, or full name
            if normalized_state == self.state_abbrev.lower() or normalized_state == self.target_state or normalized_state == self.full_state_name.lower():
                return True

        address_upper = (formatted_address or '').upper()
        
        # Check for state abbreviation in address
        if self.state_abbrev:
            if f', {self.state_abbrev} ' in address_upper or address_upper.endswith(f', {self.state_abbrev}'):
                return True
        
        # Check for full state name in address
        if f' {self.full_state_name.upper()}' in address_upper:
            return True

        # Last fallback: look for state abbreviation pattern
        if self.state_abbrev:
            match = re.search(r',\s*([A-Z]{2})\s+\d{5}', formatted_address or '')
            if match and match.group(1) == self.state_abbrev:
                return True

        return False

    def _parse_new_result(self, result: Dict, location: str, search_term: str) -> Optional[School]:
        """
        Parse a single result from New Places API into a School object.
        Returns None if duplicate or not Texas.
        Only uses Essentials-tier fields to ensure lowest pricing.
        """
        place_id = result.get('id', '')
        if not place_id:
            return None
        
        # Check for duplicates
        if place_id in self.seen_place_ids:
            return None
        self.seen_place_ids.add(place_id)
        
        # Extract data from New API format (Essentials-tier fields only)
        display_name = result.get('displayName', {}).get('text', '') if isinstance(result.get('displayName'), dict) else result.get('displayName', '')
        formatted_address = result.get('formattedAddress', '')
        
        # Extract state and county from address
        detected_state, detected_county = self._extract_state_and_county_new(formatted_address, result.get('location'))
        
        # Validate state-only
        if not self._is_state_result(detected_state, formatted_address):
            self.stats['non_state_skipped'] += 1
            return None
        
        # New API Text Search includes websiteUri and phone in response (Essentials tier)
        # We request these fields in the field mask, so they should be in the response
        # No need for Place Details call - saves API calls and cost
        website = result.get('websiteUri')
        phone = result.get('nationalPhoneNumber') or result.get('internationalPhoneNumber')
                
        # Note: Place Details calls removed - website/phone are included in Text Search results
        # This saves API calls and reduces costs
        
        # Extract types
        types_list = result.get('types', [])
        primary_type = result.get('primaryType', '')
        types_str = ', '.join(types_list) if types_list else primary_type
        
        # Build School object
        school = School(
            place_id=place_id,
            name=display_name,
            address=formatted_address,
            website=website,
            phone=phone,
            rating=result.get('rating'),
            user_ratings_total=result.get('userRatingCount'),
            types=types_str,
            business_status=result.get('businessStatus'),
            county=(detected_county or location).replace('County', '').strip(),
            state=self.full_state_name,
            detected_state=detected_state or '',
            detected_county=detected_county or '',
            found_via=search_term.split(' in ')[0] if ' in ' in search_term else search_term
        )
        
        # Update stats
        self.stats['total_schools_found'] += 1
        if school.website:
            self.stats['schools_with_websites'] += 1
        
        return school

    def search_county(
        self,
        county: str,
        state: str = None,
        max_search_terms: int = None
    ) -> Iterator[School]:
        """
        Search for Christian schools in a specific county.
        YIELDS School objects one at a time (generator).
        """
        # Use target_state if state not provided
        if state is None:
            state = self.full_state_name
        # Define search terms (balanced for recall; filters prune noise)
        search_terms = [
            f"Christian schools in {county} County, {state}",
            f"Catholic schools in {county} County, {state}",
            f"Episcopal schools in {county} County, {state}",
            f"Lutheran schools in {county} County, {state}",
            f"parochial schools in {county} County, {state}",
            f"church schools in {county} County, {state}",
            f"academy in {county} County, {state}",
            f"prep school in {county} County, {state}"
        ]

        if max_search_terms is not None:
            search_terms = search_terms[:max(0, max_search_terms)]

        for query in search_terms:
            # Check global limit before each API call
            if self._hit_global_limit():
                print(f"    Global API call limit reached. Stopping {county} County search.")
                break

            try:
                self.stats['total_api_calls'] += 1
                
                # NEW Places API: POST request with JSON body (Essentials tier)
                headers = {
                    'Content-Type': 'application/json',
                    'X-Goog-Api-Key': self.api_key,
                    'X-Goog-FieldMask': ','.join(self.essentials_fields)  # Only Essentials-tier fields
                }
                
                # Request body for Text Search (New Places API format)
                request_body = {
                    'textQuery': query,
                    'maxResultCount': 20,  # Max results per request
                    'languageCode': 'en'
                }
                
                # Debug: Check if API key is set
                if not self.api_key or len(self.api_key) < 10:
                    print(f"    WARNING: API key appears invalid (length: {len(self.api_key) if self.api_key else 0})")
                
                response = requests.post(self.text_search_url, headers=headers, json=request_body, timeout=60)
                
                # Debug: Log response for errors
                if response.status_code != 200:
                    try:
                        error_data = response.json() if response.content else {}
                        error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                        error_details = error_data.get('error', {}).get('details', [])
                        if response.status_code == 400:
                            print(f"    DEBUG: Error details: {error_details}")
                            print(f"    DEBUG: Full error response: {error_data}")
                    except Exception as e:
                        print(f"    DEBUG: Could not parse error response: {e}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # New API returns 'places' array directly
                    results = data.get('places', [])
                    
                    # Yield each school found (check school limit)
                    for result in results:
                        if self.max_schools is not None and self.stats['total_schools_found'] >= self.max_schools:
                            break
                        school = self._parse_new_result(result, county, query)
                        if school:
                            yield school
                        # Check again after yielding (in case limit was reached)
                        if self.max_schools is not None and self.stats['total_schools_found'] >= self.max_schools:
                            break
                    
                    # Check for next page token (pagination)
                    next_page_token = data.get('nextPageToken')
                    while next_page_token and not self._hit_global_limit():
                        # Wait 2 seconds before next page (Google requirement)
                        time.sleep(2)
                        self.stats['total_api_calls'] += 1
                        
                        # Pagination request
                        pagination_body = {
                            'pageToken': next_page_token
                        }
                        
                        response_page = requests.post(self.text_search_url, headers=headers, json=pagination_body, timeout=60)
                        if response_page.status_code == 200:
                            page_data = response_page.json()
                            page_results = page_data.get('places', [])
                            if page_results:
                                for result in page_results:
                                    if self.max_schools is not None and self.stats['total_schools_found'] >= self.max_schools:
                                        break
                                    school = self._parse_new_result(result, county, query)
                                    if school:
                                        yield school
                                    # Check again after yielding (in case limit was reached)
                                    if self.max_schools is not None and self.stats['total_schools_found'] >= self.max_schools:
                                        break
                                # Break out of pagination loop if limit reached
                                if self.max_schools is not None and self.stats['total_schools_found'] >= self.max_schools:
                                    break
                            next_page_token = page_data.get('nextPageToken')
                        else:
                            break
                elif response.status_code == 204:
                    # Success but no results
                    pass
                else:
                    # API error - get detailed error message
                    try:
                        error_data = response.json() if response.content else {}
                        error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                        if response.status_code == 403:
                            print(f"    API authentication error for query '{query}': {error_msg}")
                            print(f"    Check: 1) Places API (New) is enabled in your Google Cloud project")
                            print(f"           2) API key has no restrictions blocking this request")
                            print(f"           3) API key is valid and not expired")
                        else:
                            print(f"    API error for query '{query}': HTTP {response.status_code} - {error_msg}")
                    except:
                        print(f"    API error for query '{query}': HTTP {response.status_code} - {response.text[:200]}")
                
                # Rate limiting (New API has similar limits)
                time.sleep(0.1)  # 100ms between requests
                
            except Exception as e:
                print(f"    Error on query '{query}': {e}")
                time.sleep(2)
            
            # Check global limit again after query
            if self._hit_global_limit():
                break

    def discover_schools(
        self,
        counties: List[str],
        state: str = None,
        batch_size: int = 0,
        max_search_terms: int = None
    ) -> Iterator[School]:
        """
        Main generator function to discover schools across counties.
        YIELDS School objects one at a time for streaming pipeline.
        
        Args:
            counties: List of county names to search
            state: State to search (default: uses self.target_state)
            batch_size: Number of counties to search (0 = all)
            max_search_terms: Max search queries per county (None = all)
        """
        # Use target_state if state not provided
        if state is None:
            state = self.full_state_name
        
        print("\n" + "="*70)
        print(f"STREAMING SCHOOL DISCOVERY - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Global API call cap: {self.global_max_api_calls or 'None'}")
        
        # Shuffle counties for randomness
        shuffled_counties = counties.copy()
        random.shuffle(shuffled_counties)
        
        if batch_size and 0 < batch_size < len(counties):
            counties_to_search = shuffled_counties[:batch_size]
            print(f"Batch size: {batch_size} counties (randomized)")
        else:
            counties_to_search = shuffled_counties
            print(f"Batch size: ALL {len(counties)} counties (randomized)")
        
        print("="*70 + "\n")
        
        start_time = time.time()
        
        for i, county in enumerate(counties_to_search, 1):
            if self._hit_global_limit():
                print(f"Global API call cap reached after {i-1} counties.")
                break
            
            print(f"[{i}/{len(counties_to_search)}] Searching {county} County...")
            county_start = time.time()
            
            schools_found = 0
            # Yield schools one at a time from this county
            for school in self.search_county(county, state, max_search_terms):
                schools_found += 1
                self.stats['counties_searched'] = i
                yield school
            
            county_time = time.time() - county_start
            print(f"    Found {schools_found} schools in {county} County ({county_time:.1f}s)")
            print(f"    Total: {self.stats['total_schools_found']} schools | API calls: {self.stats['total_api_calls']}")
            
            if self._hit_global_limit():
                break
        
        elapsed = time.time() - start_time
        print("\n" + "="*70)
        print("DISCOVERY COMPLETE")
        print("="*70)
        print(f"Counties searched: {self.stats['counties_searched']}")
        print(f"Total schools found: {self.stats['total_schools_found']}")
        print(f"Schools with websites: {self.stats['schools_with_websites']}")
        print(f"Non-{state} skipped: {self.stats['non_state_skipped']}")
        print(f"Total API calls: {self.stats['total_api_calls']}")
        print(f"Time elapsed: {elapsed/60:.1f} minutes")
        print("="*70)

# County and city lists have been moved to assets/data/state_counties/{state}.txt files
# Use load_counties_from_state() helper in Pipeline.py to load them
