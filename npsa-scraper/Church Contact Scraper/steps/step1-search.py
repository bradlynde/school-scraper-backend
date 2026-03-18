"""
STEP 1: CHURCH SEARCH WITH GOOGLE PLACES API (NEW) - STREAMING VERSION
========================================================================
Search for churches using NEW Places API Essentials tier and YIELD them one at a time.

No CSV writing - yields Church objects for streaming pipeline.

Uses New Places API Essentials tier (cost-effective): places.googleapis.com/v1/places:searchText

Only requests Essentials-tier fields to ensure lowest pricing.
"""

import requests
import time
from datetime import datetime
from typing import Iterator, List, Dict, Tuple, Optional
import random
import re
from assets.shared.models import Church

# ANSI escape codes for bold text
BOLD = '\033[1m'
RESET = '\033[0m'

def bold(text: str) -> str:
    """Make text bold in terminal output"""
    return f"{BOLD}{text}{RESET}"

class ChurchSearcher:
    """Search for churches using New Google Places API Essentials tier, yields Church objects"""
    
    def __init__(self, api_key: str, global_max_api_calls: int = None, max_churches: int = None, target_state: str = 'texas'):
        # Debug: Verify API key is received
        if not api_key or len(api_key) < 10:
            print(f"WARNING: API key appears invalid in ChurchSearcher.__init__ (length: {len(api_key) if api_key else 0})")
        self.api_key = api_key
        self.global_max_api_calls = global_max_api_calls
        self.max_churches = max_churches
        
        # Normalize target_state: extract base state name (e.g., "texas" from "texas_ultra_test")
        normalized = target_state.lower().replace(' ', '_')
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
        
        if base_state in self.STATE_ABBREVIATIONS:
            self.target_state = base_state
        elif normalized in self.STATE_ABBREVIATIONS:
            self.target_state = normalized
        else:
            self.target_state = base_state
        
        # New Places API endpoints (Essentials tier)
        self.text_search_url = "https://places.googleapis.com/v1/places:searchText"
        self.place_details_url_template = "https://places.googleapis.com/v1/places/{}"
        self.seen_place_ids = set()
        
        # Field mask: name, location/address, website, phone
        # Note: websiteUri = Enterprise tier ($35/1k); phone same tier, no extra cost
        self.essentials_fields = [
            "places.id",
            "places.displayName",
            "places.formattedAddress",
            "places.location",
            "places.websiteUri",
            "places.nationalPhoneNumber",
            "places.internationalPhoneNumber",
            "places.businessStatus",
            "places.types",
            "places.primaryType"
        ]
        self.stats = {
            'counties_searched': 0,
            'total_api_calls': 0,
            'total_churches_found': 0,
            'churches_with_websites': 0,
            'non_state_skipped': 0
        }
        
        self.state_abbrev = self.STATE_ABBREVIATIONS.get(self.target_state, '')
        self.full_state_name = self.target_state.replace('_', ' ').title()

    def _hit_global_limit(self) -> bool:
        """Check if global API call limit or church limit has been reached"""
        api_limit_hit = (
            self.global_max_api_calls is not None and
            self.stats['total_api_calls'] >= self.global_max_api_calls
        )
        church_limit_hit = (
            self.max_churches is not None and
            self.stats['total_churches_found'] >= self.max_churches
        )
        return api_limit_hit or church_limit_hit

    def _extract_state_and_county_new(self, address: str, location: Dict = None) -> Tuple[str, str]:
        """Extract state and county from New API response."""
        state_value = ''
        county_value = ''
        
        if address:
            state_match = re.search(r',\s*([A-Z]{2})\s+\d{5}', address)
            if state_match:
                state_value = state_match.group(1)
            
            parts = address.split(',')
            if len(parts) >= 2:
                potential_county = parts[-3].strip() if len(parts) >= 3 else ''
                if 'County' in potential_county:
                    county_value = potential_county.replace('County', '').strip()
        
        return state_value, county_value

    def _is_state_result(self, detected_state: str, formatted_address: str) -> bool:
        """Determine if the result belongs to the target state"""
        if detected_state:
            normalized_state = detected_state.strip().lower()
            if normalized_state == self.state_abbrev.lower() or normalized_state == self.target_state or normalized_state == self.full_state_name.lower():
                return True

        address_upper = (formatted_address or '').upper()
        
        if self.state_abbrev:
            if f', {self.state_abbrev} ' in address_upper or address_upper.endswith(f', {self.state_abbrev}'):
                return True
        
        if f' {self.full_state_name.upper()}' in address_upper:
            return True

        if self.state_abbrev:
            match = re.search(r',\s*([A-Z]{2})\s+\d{5}', formatted_address or '')
            if match and match.group(1) == self.state_abbrev:
                return True

        return False

    def _parse_new_result(self, result: Dict, location: str, search_term: str) -> Optional[Church]:
        """
        Parse a single result from New Places API into a Church object.
        Returns None if duplicate or not in target state.
        """
        place_id = result.get('id', '')
        if not place_id:
            return None
        
        if place_id in self.seen_place_ids:
            return None
        self.seen_place_ids.add(place_id)
        
        display_name = result.get('displayName', {}).get('text', '') if isinstance(result.get('displayName'), dict) else result.get('displayName', '')
        formatted_address = result.get('formattedAddress', '')
        
        detected_state, detected_county = self._extract_state_and_county_new(formatted_address, result.get('location'))
        
        if not self._is_state_result(detected_state, formatted_address):
            self.stats['non_state_skipped'] += 1
            return None
        
        website = result.get('websiteUri')
        phone = result.get('nationalPhoneNumber') or result.get('internationalPhoneNumber')
        
        types_list = result.get('types', [])
        primary_type = result.get('primaryType', '')
        types_str = ', '.join(types_list) if types_list else primary_type
        
        church = Church(
            place_id=place_id,
            name=display_name,
            address=formatted_address,
            website=website,
            phone=phone,
            types=types_str,
            business_status=result.get('businessStatus'),
            county=(detected_county or location).replace('County', '').strip(),
            state=self.full_state_name,
            detected_state=detected_state or '',
            detected_county=detected_county or '',
            found_via=search_term.split(' in ')[0] if ' in ' in search_term else search_term
        )
        
        self.stats['total_churches_found'] += 1
        if church.website:
            self.stats['churches_with_websites'] += 1
        
        return church

    def search_county(
        self,
        county: str,
        state: str = None,
        max_search_terms: int = None
    ) -> Iterator[Church]:
        """
        Search for Christian/Catholic churches in a specific county.
        YIELDS Church objects one at a time (generator).
        
        Uses 20 search terms for church discovery.
        """
        if state is None:
            state = self.full_state_name
        
        # Search terms - 20 terms
        all_search_terms = [
            f"Churches in {county} County, {state}",
            f"Christian churches in {county} County, {state}",
            f"Baptist churches in {county} County, {state}",
            f"Southern Baptist church in {county} County, {state}",
            f"Non-denominational church in {county} County, {state}",
            f"Community church in {county} County, {state}",
            f"Assembly of God churches in {county} County, {state}",
            f"Pentecostal churches in {county} County, {state}",
            f"Methodist churches in {county} County, {state}",
            f"United Methodist church in {county} County, {state}",
            f"Church of Christ in {county} County, {state}",
            f"Evangelical church in {county} County, {state}",
            f"Bible church in {county} County, {state}",
            f"Catholic churches in {county} County, {state}",
            f"Lutheran churches in {county} County, {state}",
            f"Presbyterian churches in {county} County, {state}",
            f"Episcopal churches in {county} County, {state}",
            f"Anglican church in {county} County, {state}",
            f"Church of the Nazarene in {county} County, {state}",
            f"Church of God in {county} County, {state}",
        ]
        # Default to all 20 terms when not specified
        limit = max_search_terms if max_search_terms is not None else len(all_search_terms)
        search_terms = all_search_terms[:max(0, limit)]

        for query in search_terms:
            if self._hit_global_limit():
                print(f"    Global API call limit reached. Stopping {county} County search.")
                break

            try:
                self.stats['total_api_calls'] += 1
                
                headers = {
                    'Content-Type': 'application/json',
                    'X-Goog-Api-Key': self.api_key,
                    'X-Goog-FieldMask': ','.join(self.essentials_fields)
                }
                
                request_body = {
                    'textQuery': query,
                    'maxResultCount': 20,
                    'languageCode': 'en'
                }
                
                if not self.api_key or len(self.api_key) < 10:
                    print(f"    WARNING: API key appears invalid (length: {len(self.api_key) if self.api_key else 0})")
                
                response = requests.post(self.text_search_url, headers=headers, json=request_body, timeout=60)
                
                if response.status_code != 200:
                    try:
                        error_data = response.json() if response.content else {}
                        error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                        if response.status_code == 400:
                            print(f"    DEBUG: Full error response: {error_data}")
                    except Exception as e:
                        print(f"    DEBUG: Could not parse error response: {e}")
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get('places', [])
                    
                    for result in results:
                        if self.max_churches is not None and self.stats['total_churches_found'] >= self.max_churches:
                            break
                        church = self._parse_new_result(result, county, query)
                        if church:
                            yield church
                        if self.max_churches is not None and self.stats['total_churches_found'] >= self.max_churches:
                            break
                    
                    next_page_token = data.get('nextPageToken')
                    while next_page_token and not self._hit_global_limit():
                        time.sleep(2)
                        self.stats['total_api_calls'] += 1
                        
                        pagination_body = {'pageToken': next_page_token}
                        response_page = requests.post(self.text_search_url, headers=headers, json=pagination_body, timeout=60)
                        if response_page.status_code == 200:
                            page_data = response_page.json()
                            page_results = page_data.get('places', [])
                            if page_results:
                                for result in page_results:
                                    if self.max_churches is not None and self.stats['total_churches_found'] >= self.max_churches:
                                        break
                                    church = self._parse_new_result(result, county, query)
                                    if church:
                                        yield church
                                    if self.max_churches is not None and self.stats['total_churches_found'] >= self.max_churches:
                                        break
                                if self.max_churches is not None and self.stats['total_churches_found'] >= self.max_churches:
                                    break
                            next_page_token = page_data.get('nextPageToken')
                        else:
                            break
                elif response.status_code == 204:
                    pass
                else:
                    try:
                        error_data = response.json() if response.content else {}
                        error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                        if response.status_code == 403:
                            print(f"    API authentication error for query '{query}': {error_msg}")
                        else:
                            print(f"    API error for query '{query}': HTTP {response.status_code} - {error_msg}")
                    except:
                        print(f"    API error for query '{query}': HTTP {response.status_code} - {response.text[:200]}")
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"    Error on query '{query}': {e}")
                time.sleep(2)
            
            if self._hit_global_limit():
                break

    def discover_churches(
        self,
        counties: List[str],
        state: str = None,
        batch_size: int = 0,
        max_search_terms: int = None
    ) -> Iterator[Church]:
        """
        Main generator function to discover churches across counties.
        YIELDS Church objects one at a time for streaming pipeline.
        """
        if state is None:
            state = self.full_state_name
        
        shuffled_counties = counties.copy()
        random.shuffle(shuffled_counties)
        
        if batch_size and 0 < batch_size < len(counties):
            counties_to_search = shuffled_counties[:batch_size]
        else:
            counties_to_search = shuffled_counties
        print(f"{bold('[STEP 1]')} Starting discovery: {len(counties_to_search)} counties, API cap: {self.global_max_api_calls or 'None'}")
        
        start_time = time.time()
        
        for i, county in enumerate(counties_to_search, 1):
            if self._hit_global_limit():
                print(f"Global API call cap reached after {i-1} counties.")
                break
            
            print(f"[{i}/{len(counties_to_search)}] Searching {county} County...")
            county_start = time.time()
            
            churches_found = 0
            for church in self.search_county(county, state, max_search_terms):
                churches_found += 1
                self.stats['counties_searched'] = i
                yield church
            
            county_time = time.time() - county_start
            if churches_found > 0 or (i % 5 == 0):
                print(f"{bold('[STEP 1]')} {county}: {churches_found} churches ({county_time:.1f}s) | Total: {self.stats['total_churches_found']} churches, {self.stats['total_api_calls']} API calls")
            
            if self._hit_global_limit():
                break
        
        elapsed = time.time() - start_time
        print(f"{bold('[STEP 1]')} Complete: {self.stats['counties_searched']} counties, {self.stats['total_churches_found']} churches, {self.stats['total_api_calls']} API calls, {elapsed/60:.1f} min")
