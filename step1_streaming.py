"""
STEP 1: SCHOOL SEARCH WITH GOOGLE PLACES API (LEGACY) - STREAMING VERSION
==========================================================================
Search for schools using LEGACY Places API and YIELD them one at a time.
No CSV writing - yields School objects for streaming pipeline.

Uses Legacy Places API (cheaper): https://maps.googleapis.com/maps/api/place/textsearch/json
"""

import requests
import time
from datetime import datetime
from typing import Iterator, List, Dict, Tuple, Optional
import random
import re
from shared.models import School


class SchoolSearcher:
    """Search for schools using Legacy Google Places API, yields School objects"""
    
    def __init__(self, api_key: str, global_max_api_calls: int = None):
        self.api_key = api_key
        self.global_max_api_calls = global_max_api_calls
        # LEGACY Places API endpoint (cheaper than new API)
        self.base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        self.seen_place_ids = set()
        self.stats = {
            'counties_searched': 0,
            'total_api_calls': 0,
            'total_schools_found': 0,
            'schools_with_websites': 0,
            'non_texas_skipped': 0
        }

    def _hit_global_limit(self) -> bool:
        """Check if global API call limit has been reached"""
        return (
            self.global_max_api_calls is not None and
            self.stats['total_api_calls'] >= self.global_max_api_calls
        )

    def _extract_state_and_county_legacy(self, result: Dict) -> Tuple[str, str]:
        """
        Extract state and county from legacy API response.
        Legacy API uses address_components differently than new API.
        """
        state_value = ''
        county_value = ''
        
        # Legacy API has address_components at root level
        for component in result.get('address_components', []):
            types = component.get('types', [])
            long_name = component.get('long_name', '')
            short_name = component.get('short_name', '')
            
            # State (administrative_area_level_1)
            if 'administrative_area_level_1' in types and not state_value:
                state_value = short_name or long_name
            
            # County (administrative_area_level_2)
            if 'administrative_area_level_2' in types and not county_value:
                county_value = long_name or short_name
        
        return state_value, county_value

    def _is_texas_result(self, detected_state: str, formatted_address: str) -> bool:
        """Determine if the result belongs to Texas"""
        if detected_state:
            normalized_state = detected_state.strip().lower()
            if normalized_state in ('tx', 'texas'):
                return True

        address_upper = (formatted_address or '').upper()
        if ', TX ' in address_upper or address_upper.endswith(', TX') or ' TEXAS' in address_upper:
            return True

        # Last fallback: look for state abbreviation pattern
        match = re.search(r',\s*([A-Z]{2})\s+\d{5}', formatted_address or '')
        if match and match.group(1) == 'TX':
            return True

        return False

    def _parse_legacy_result(self, result: Dict, location: str, search_term: str) -> Optional[School]:
        """
        Parse a single result from legacy Places API into a School object.
        Returns None if duplicate or not Texas.
        """
        place_id = result.get('place_id', '')
        if not place_id:
            return None
        
        # Check for duplicates
        if place_id in self.seen_place_ids:
            return None
        self.seen_place_ids.add(place_id)
        
        # Extract state and county from legacy format
        detected_state, detected_county = self._extract_state_and_county_legacy(result)
        
        # Validate Texas-only
        formatted_address = result.get('formatted_address', '')
        if not self._is_texas_result(detected_state, formatted_address):
            self.stats['non_texas_skipped'] += 1
            return None
        
        # Extract website and phone
        # NOTE: Legacy Text Search API does NOT include website/phone in response
        # We MUST make a Place Details call to get these fields (costs 1 API call per school)
        website = None
        phone = None
        
        # Make Place Details API call to get website/phone (costs 1 API call)
        # Check limit first - if we've hit it, skip Place Details call
        if self._hit_global_limit():
            # Already at limit, skip Place Details call
            pass
        else:
            try:
                # Place Details API call (costs 1 API call)
                details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                details_params = {
                    'place_id': place_id,
                    'fields': 'website,formatted_phone_number',
                    'key': self.api_key
                }
                details_response = requests.get(details_url, params=details_params, timeout=30)
                if details_response.status_code == 200:
                    details_data = details_response.json()
                    if details_data.get('status') == 'OK':
                        details_result = details_data.get('result', {})
                        website = details_result.get('website') or details_result.get('website_uri')
                        phone = details_result.get('formatted_phone_number') or details_result.get('international_phone_number')
                
                # Count Place Details call toward global API limit
                self.stats['total_api_calls'] += 1
                time.sleep(0.1)  # Rate limiting (legacy API has 10 QPS limit)
            except Exception as e:
                # If Place Details fails, continue without website/phone
                pass
        
        # Build School object
        school = School(
            place_id=place_id,
            name=result.get('name', ''),
            address=formatted_address,
            website=website,
            phone=phone,
            rating=result.get('rating'),
            user_ratings_total=result.get('user_ratings_total'),
            types=', '.join(result.get('types', [])),
            business_status=result.get('business_status'),
            county=(detected_county or location).replace('County', '').strip(),
            state='Texas',
            detected_state=detected_state or '',
            detected_county=detected_county or '',
            found_via=search_term.split(' in ')[0] if ' in ' in search_term else search_term
        )
        
        # Update stats
        self.stats['total_schools_found'] += 1
        if school.website:
            self.stats['schools_with_websites'] += 1
        
        return school

    def search_city(
        self,
        city: str,
        state: str = 'Texas'
    ) -> Iterator[School]:
        """
        Search for Christian schools in a specific city.
        YIELDS School objects one at a time (generator).
        Uses ONE API call per city (hard cap).
        """
        # Single search term per city (1 API call max)
        query = f"Christian schools in {city}, {state}"
        
        # Check global limit before API call
        if self._hit_global_limit():
            print(f"    Global API call limit reached. Skipping {city}.")
            return
        
        try:
            self.stats['total_api_calls'] += 1
            
            # LEGACY API: GET request with query parameters
            # Note: Legacy API does NOT support 'fields' parameter - returns all available fields
            params = {
                'query': query,
                'key': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check for API errors
                if data.get('status') == 'OK':
                    results = data.get('results', [])
                    
                    # Yield each school found
                    # IMPORTANT: Place Details calls are made inside _parse_legacy_result
                    # Each Place Details call counts toward the global API cap
                    for result in results:
                        # Check if we've hit the limit before making Place Details call
                        if self._hit_global_limit():
                            print(f"    Global API call limit reached. Stopping {city} search.")
                            break
                        school = self._parse_legacy_result(result, city, query)
                        if school:
                            yield school
                    
                    # Note: We skip pagination to keep it to 1 API call per city
                    # If you need more results, you'd need additional API calls
                
                elif data.get('status') in ['ZERO_RESULTS', 'NOT_FOUND']:
                    # No results
                    pass
                else:
                    # API error
                    print(f"    API error for query '{query}': {data.get('status')} - {data.get('error_message', 'Unknown error')}")
            
            else:
                print(f"    HTTP error {response.status_code} for query '{query}'")
            
            # Rate limiting (legacy API has 10 QPS limit)
            time.sleep(0.1)  # 100ms between requests
            
        except Exception as e:
            print(f"    Error on query '{query}': {e}")
            time.sleep(2)

    def search_county(
        self,
        county: str,
        state: str = 'Texas',
        max_search_terms: int = None
    ) -> Iterator[School]:
        """
        Search for Christian schools in a specific county.
        YIELDS School objects one at a time (generator).
        """
        # Define search terms for Christian schools
        search_terms = [
            f"Christian schools in {county} County, {state}",
            f"Christian academy in {county} County, {state}",
            f"Catholic schools in {county} County, {state}",
            f"Catholic elementary school in {county} County, {state}",
            f"Baptist schools in {county} County, {state}",
            f"Methodist schools in {county} County, {state}",
            f"Lutheran schools in {county} County, {state}",
            f"Presbyterian schools in {county} County, {state}",
            f"Episcopal schools in {county} County, {state}",
            f"Pentecostal schools in {county} County, {state}",
            f"Assembly of God schools in {county} County, {state}",
            f"Church of God schools in {county} County, {state}",
            f"nondenominational Christian schools in {county} County, {state}",
            f"evangelical schools in {county} County, {state}",
            f"private religious schools in {county} County, {state}",
            f"parochial schools in {county} County, {state}"
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
                
                # LEGACY API: GET request with query parameters
                # Note: Legacy API does NOT support 'fields' parameter - returns all available fields
                params = {
                    'query': query,
                    'key': self.api_key
                }
                
                response = requests.get(self.base_url, params=params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check for API errors
                    if data.get('status') == 'OK':
                        results = data.get('results', [])
                        
                        # Yield each school found
                        for result in results:
                            school = self._parse_legacy_result(result, county, query)
                            if school:
                                yield school
                        
                        # Check for next page token (pagination)
                        next_page_token = data.get('next_page_token')
                        while next_page_token and not self._hit_global_limit():
                            # Wait 2 seconds before next page (Google requirement)
                            time.sleep(2)
                            self.stats['total_api_calls'] += 1
                            
                            params_page = {
                                'pagetoken': next_page_token,
                                'key': self.api_key
                            }
                            
                            response_page = requests.get(self.base_url, params=params_page, timeout=60)
                            if response_page.status_code == 200:
                                page_data = response_page.json()
                                if page_data.get('status') == 'OK':
                                    for result in page_data.get('results', []):
                                        school = self._parse_legacy_result(result, county, query)
                                        if school:
                                            yield school
                                    next_page_token = page_data.get('next_page_token')
                                else:
                                    break
                            else:
                                break
                    
                    elif data.get('status') in ['ZERO_RESULTS', 'NOT_FOUND']:
                        # No results, continue to next query
                        pass
                    else:
                        # API error
                        print(f"    API error for query '{query}': {data.get('status')} - {data.get('error_message', 'Unknown error')}")
                
                else:
                    print(f"    HTTP error {response.status_code} for query '{query}'")
                
                # Rate limiting (legacy API has 10 QPS limit)
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
        state: str = 'Texas',
        batch_size: int = 0,
        max_search_terms: int = None
    ) -> Iterator[School]:
        """
        Main generator function to discover schools across counties.
        YIELDS School objects one at a time for streaming pipeline.
        
        Args:
            counties: List of county names to search
            state: State to search (default: Texas)
            batch_size: Number of counties to search (0 = all)
            max_search_terms: Max search queries per county (None = all)
        """
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
        print(f"Non-Texas skipped: {self.stats['non_texas_skipped']}")
        print(f"Total API calls: {self.stats['total_api_calls']}")
        print(f"Time elapsed: {elapsed/60:.1f} minutes")
        print("="*70)

    def discover_schools_cities(
        self,
        cities: List[str],
        state: str = 'Texas',
        num_cities: int = 5
    ) -> Iterator[School]:
        """
        Search 5 random cities with 1 API call each (hard cap).
        YIELDS School objects one at a time for streaming pipeline.
        
        Args:
            cities: List of city names to choose from
            state: State to search (default: Texas)
            num_cities: Number of random cities to search (default: 5)
        """
        print("\n" + "="*70)
        print(f"STREAMING SCHOOL DISCOVERY BY CITY - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Global API call cap: {self.global_max_api_calls or 'None'}")
        print(f"Searching {num_cities} random cities (1 API call per city)")
        print("="*70 + "\n")
        
        # Pick random cities
        available_cities = cities.copy()
        random.shuffle(available_cities)
        selected_cities = available_cities[:min(num_cities, len(available_cities))]
        
        print(f"Selected cities: {', '.join(selected_cities)}")
        print("="*70 + "\n")
        
        start_time = time.time()
        
        for i, city in enumerate(selected_cities, 1):
            if self._hit_global_limit():
                print(f"Global API call cap reached after {i-1} cities.")
                break
            
            print(f"[{i}/{len(selected_cities)}] Searching {city}, {state}...")
            city_start = time.time()
            
            schools_found = 0
            # Yield schools one at a time from this city (1 API call max)
            for school in self.search_city(city, state):
                schools_found += 1
                self.stats['counties_searched'] = i  # Reusing this stat for cities
                yield school
            
            city_time = time.time() - city_start
            print(f"    Found {schools_found} schools in {city} ({city_time:.1f}s)")
            print(f"    Total: {self.stats['total_schools_found']} schools | API calls: {self.stats['total_api_calls']}")
            
            if self._hit_global_limit():
                break
        
        elapsed = time.time() - start_time
        print("\n" + "="*70)
        print("DISCOVERY COMPLETE")
        print("="*70)
        print(f"Cities searched: {self.stats['counties_searched']}")
        print(f"Total schools found: {self.stats['total_schools_found']}")
        print(f"Schools with websites: {self.stats['schools_with_websites']}")
        print(f"Non-Texas skipped: {self.stats['non_texas_skipped']}")
        print(f"Total API calls: {self.stats['total_api_calls']}")
        print(f"Time elapsed: {elapsed/60:.1f} minutes")
        print("="*70)


# County and city lists have been moved to data/states/{state}.txt files
# Use load_counties_from_state() function in streaming_pipeline.py to load them


