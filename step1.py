"""
STEP 1: SCHOOL SEARCH WITH GOOGLE PLACES API
==============================================
Search for schools and return ALL results - no filtering.

Input: Search parameters (counties, states, search terms)
Output: CSV with school name, address, phone, website URL, metadata
"""

import requests
import csv
import time
from datetime import datetime
from typing import List, Dict
import argparse
import random

class SchoolSearcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://places.googleapis.com/v1/places:searchText"
        self.all_schools = []
        self.seen_place_ids = set()
        self.stats = {
            'counties_searched': 0,
            'total_api_calls': 0,
            'total_schools_found': 0,
            'schools_with_websites': 0
        }

    def search_county(
        self,
        county: str,
        state: str = 'Texas',
        max_search_terms: int = None,
        max_api_calls: int = None
    ) -> tuple:
        """Search for Christian schools in a specific county"""
        schools = []
        
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

        county_new_schools = 0

        for query in search_terms:
            if max_api_calls is not None and self.stats['total_api_calls'] >= max_api_calls:
                print("    API call limit reached for this run. Stopping further queries.")
                break

            try:
                self.stats['total_api_calls'] += 1
                
                headers = {
                    'Content-Type': 'application/json',
                    'X-Goog-Api-Key': self.api_key,
                    'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.types,places.businessStatus'
                }
                
                data = {'textQuery': query, 'pageSize': 20}
                response = requests.post(self.base_url, headers=headers, json=data, timeout=15)

                if response.status_code == 200:
                    result = response.json()
                    if 'places' in result:
                        for place in result['places']:
                            place_id = place.get('id', '')
                            types = place.get('types', [])
                            name = place.get('displayName', {}).get('text', '').lower()
                            address = place.get('formattedAddress', '').lower()

                            # === SIMPLE DEDUPLICATION ONLY ===
                            # No filtering - return all results from Google Places
                            if place_id in self.seen_place_ids:
                                continue

                            self.seen_place_ids.add(place_id)
                            county_new_schools += 1

                            # Build school record
                            school = {
                                'place_id': place_id,
                                'name': place.get('displayName', {}).get('text', ''),
                                'address': place.get('formattedAddress', ''),
                                'website': place.get('websiteUri', ''),
                                'phone': place.get('nationalPhoneNumber', ''),
                                'rating': place.get('rating', ''),
                                'user_ratings': place.get('userRatingCount', ''),
                                'types': ', '.join(types),
                                'business_status': place.get('businessStatus', ''),
                                'county': county,
                                'state': state,
                                'found_via': query.split(' in ')[0]
                            }
                            schools.append(school)
                            
                            if school['website']:
                                self.stats['schools_with_websites'] += 1

                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                print(f"    Error on query: {e}")
                time.sleep(2)

        return schools, county_new_schools

    def search_random_county_sample(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        sample_size: int = 10,
        max_search_terms: int = None,
        max_api_calls: int = None
    ):
        """Pick a random county, search it, and return a random sample of schools"""
        print("\n" + "="*70)
        print(f"RANDOM COUNTY SAMPLE MODE - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Pick a random county
        selected_county = random.choice(counties)
        print(f"Selected random county: {selected_county} County")
        print(f"Target sample size: {sample_size} schools")
        print("="*70 + "\n")
        
        start_time = time.time()
        
        # Search the selected county
        print(f"Searching {selected_county} County...")
        county_schools, new_count = self.search_county(
            selected_county,
            state,
            max_search_terms=max_search_terms,
            max_api_calls=max_api_calls
        )
        
        self.all_schools.extend(county_schools)
        self.stats['counties_searched'] = 1
        self.stats['total_schools_found'] = len(self.all_schools)
        
        print(f"\nFound {len(self.all_schools)} total schools in {selected_county} County")
        
        # Randomly sample the requested number
        if len(self.all_schools) > sample_size:
            print(f"Randomly sampling {sample_size} schools from {len(self.all_schools)} found...")
            self.all_schools = random.sample(self.all_schools, sample_size)
            self.stats['total_schools_found'] = len(self.all_schools)
        elif len(self.all_schools) < sample_size:
            print(f"Warning: Only found {len(self.all_schools)} schools (requested {sample_size})")
        
        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def search_multiple_random_counties(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        target_schools: int = 100,
        max_search_terms: int = None,
        max_api_calls: int = None
    ):
        """Search multiple random counties until we reach target number of schools"""
        print("\n" + "="*70)
        print(f"MULTIPLE RANDOM COUNTIES MODE - {state.upper()}")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Target: {target_schools} schools")
        print("="*70 + "\n")
        
        start_time = time.time()
        counties_searched = []
        max_counties_to_search = min(100, len(counties))  # Search up to 100 counties max to reach target
        
        # Shuffle counties for randomness
        shuffled_counties = counties.copy()
        random.shuffle(shuffled_counties)
        
        for county in shuffled_counties[:max_counties_to_search]:
            if len(self.all_schools) >= target_schools:
                break
                
            if max_api_calls is not None and self.stats['total_api_calls'] >= max_api_calls:
                print(f"\nAPI call limit reached. Stopping county searches.")
                break
            
            print(f"Searching {county} County...")
            county_schools, new_count = self.search_county(
                county,
                state,
                max_search_terms=max_search_terms,
                max_api_calls=max_api_calls
            )
            
            self.all_schools.extend(county_schools)
            counties_searched.append(county)
            self.stats['counties_searched'] = len(counties_searched)
            self.stats['total_schools_found'] = len(self.all_schools)
            
            print(f"    Found {new_count} new schools | Total: {self.stats['total_schools_found']}")
            
            if len(self.all_schools) >= target_schools:
                print(f"\nReached target of {target_schools} schools!")
                break
        
        # Randomly sample to exactly target_schools if we have more
        if len(self.all_schools) > target_schools:
            print(f"\nRandomly sampling {target_schools} schools from {len(self.all_schools)} found...")
            self.all_schools = random.sample(self.all_schools, target_schools)
            self.stats['total_schools_found'] = len(self.all_schools)
        elif len(self.all_schools) < target_schools:
            print(f"\nWarning: Only found {len(self.all_schools)} schools (requested {target_schools})")
        
        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def search_all_counties(
        self,
        counties: List[str],
        state: str,
        output_file: str,
        max_counties: int = None,
        max_search_terms: int = None,
        max_api_calls: int = None,
        max_schools: int = None
    ):
        """Search all counties and save results"""
        print("\n" + "="*70)
        print(f"{state.upper()} CHRISTIAN SCHOOL DISCOVERY")
        print("="*70)
        print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if max_counties is not None:
            counties = counties[:max(0, max_counties)]
        print(f"Counties: {len(counties)}")

        if max_search_terms is not None:
            print(f"Search terms per county limited to: {max_search_terms}")
        if max_api_calls is not None:
            print(f"Total API call limit: {max_api_calls}")
        if max_schools is not None:
            print(f"Maximum schools to collect: {max_schools}")

        print("="*70 + "\n")

        start_time = time.time()
        
        for i, county in enumerate(counties, 1):
            if max_api_calls is not None and self.stats['total_api_calls'] >= max_api_calls:
                print("\nAPI call limit reached. Stopping further county searches.")
                break

            print(f"[{i}/{len(counties)}] Searching {county} County...")
            county_schools, new_count = self.search_county(
                county,
                state,
                max_search_terms=max_search_terms,
                max_api_calls=max_api_calls
            )
            self.all_schools.extend(county_schools)
            self.stats['counties_searched'] += 1
            self.stats['total_schools_found'] = len(self.seen_place_ids)

            print(f"    Found {new_count} new schools | Total: {self.stats['total_schools_found']}")

            # Check if we've reached max_schools limit
            if max_schools is not None and len(self.all_schools) >= max_schools:
                print(f"\nReached maximum schools limit ({max_schools}). Stopping search.")
                # Trim to exactly max_schools
                self.all_schools = self.all_schools[:max_schools]
                self.stats['total_schools_found'] = len(self.all_schools)
                break

            # Progress update every 20 counties
            if i % 20 == 0:
                elapsed = time.time() - start_time
                print(f"    Progress: {i}/{len(counties)} ({i/len(counties)*100:.1f}%) | Elapsed: {elapsed/60:.1f} min")

        self._save_to_csv(output_file)
        self._print_summary(time.time() - start_time, output_file)

    def _save_to_csv(self, filename: str):
        """Save results to CSV"""
        fieldnames = ['place_id', 'name', 'address', 'website', 'phone', 'rating', 'user_ratings',
                      'types', 'business_status', 'county', 'state', 'found_via']
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.all_schools)

    def _print_summary(self, elapsed: float, output_file: str):
        """Print final summary"""
        print("\n" + "="*70)
        print("DISCOVERY COMPLETE!")
        print("="*70)
        print(f"Time: {elapsed/60:.1f} min")
        print(f"Schools Found: {self.stats['total_schools_found']}")
        print(f"With Website: {self.stats['schools_with_websites']} "
              f"({self.stats['schools_with_websites']/max(1, self.stats['total_schools_found'])*100:.1f}%)")
        print(f"API Calls: {self.stats['total_api_calls']}")
        print(f"File: {output_file}")
        print("="*70)


# Texas counties list (abbreviated for testing - add more as needed)
TEXAS_COUNTIES = [
    'Harris', 'Dallas', 'Tarrant', 'Bexar', 'Travis',  # Major cities first for testing
    'Anderson', 'Andrews', 'Angelina', 'Aransas', 'Archer', 'Armstrong', 'Atascosa', 'Austin', 
    'Bailey', 'Bandera', 'Bastrop', 'Baylor', 'Bee', 'Bell', 'Bexar', 'Blanco', 'Borden', 
    'Bosque', 'Bowie', 'Brazoria', 'Brazos', 'Brewster', 'Briscoe', 'Brooks', 'Brown',
    'Burleson', 'Burnet', 'Caldwell', 'Calhoun', 'Callahan', 'Cameron', 'Camp', 'Carson',
    'Cass', 'Castro', 'Chambers', 'Cherokee', 'Childress', 'Clay', 'Cochran', 'Coke',
    # Add more counties as needed for full runs (254 total)
]

# Iowa counties list (abbreviated for testing - add more as needed)
IOWA_COUNTIES = [
    'Adair', 'Adams', 'Allamakee', 'Appanoose', 'Audubon', 'Benton', 'Black Hawk', 'Boone',
    'Bremer', 'Buchanan', 'Buena Vista', 'Butler', 'Calhoun', 'Carroll', 'Cass', 'Cedar',
    'Cerro Gordo', 'Cherokee', 'Chickasaw', 'Clarke', 'Clay', 'Clayton', 'Clinton', 'Crawford',
    'Dallas', 'Davis', 'Decatur', 'Delaware', 'Des Moines', 'Dickinson', 'Dubuque', 'Emmet',
    # Add more counties as needed for full runs (99 total)
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search for Christian schools using Google Places API')
    parser.add_argument('--api-key', required=True, help='Google Places API key')
    parser.add_argument('--state', default='Texas', help='State to search (default: Texas)')
    parser.add_argument('--output', default='schools_found.csv', help='Output CSV filename')
    parser.add_argument('--max-counties', type=int, help='Maximum number of counties to search')
    parser.add_argument('--max-search-terms', type=int, help='Maximum search queries per county')
    parser.add_argument('--max-api-calls', type=int, help='Maximum total Google Places API calls for this run')
    parser.add_argument('--max-schools', type=int, help='Maximum number of schools to collect (stops early when reached)')
    parser.add_argument('--random-county-sample', type=int, metavar='N', help='Pick a random county and return N random schools (testing mode)')
    parser.add_argument('--multiple-random-counties', type=int, metavar='N', help='Search multiple random counties until N schools are found')
    
    args = parser.parse_args()
    
    # Select county list based on state
    counties = TEXAS_COUNTIES if args.state.lower() == 'texas' else IOWA_COUNTIES
    
    searcher = SchoolSearcher(api_key=args.api_key)
    
    try:
        # If multiple-random-counties is specified, use that mode
        if args.multiple_random_counties is not None:
            searcher.search_multiple_random_counties(
                counties,
                args.state,
                args.output,
                target_schools=args.multiple_random_counties,
                max_search_terms=args.max_search_terms,
                max_api_calls=args.max_api_calls
            )
        # If random-county-sample is specified, use that mode
        elif args.random_county_sample is not None:
            searcher.search_random_county_sample(
                counties,
                args.state,
                args.output,
                sample_size=args.random_county_sample,
                max_search_terms=args.max_search_terms,
                max_api_calls=args.max_api_calls
            )
        else:
            # Otherwise use the normal search mode
            print(f"\nStarting Christian School Discovery for {args.state}...")
            time.sleep(2)
            searcher.search_all_counties(
                counties,
                args.state,
                args.output,
                max_counties=args.max_counties,
                max_search_terms=args.max_search_terms,
                max_api_calls=args.max_api_calls,
                max_schools=args.max_schools
            )
    except KeyboardInterrupt:
        searcher._save_to_csv(args.output)
        print(f"\nInterrupted. Saved partial results to: {args.output}")
    except Exception as e:
        searcher._save_to_csv(args.output)
        print(f"\nError: {e} | Saved partial results to: {args.output}")
