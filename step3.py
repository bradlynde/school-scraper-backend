"""
STEP 3: DISCOVER SITE PAGES
===========================
Crawl each school's website to discover priority internal pages.

Input: CSV from Step 2 with filtered schools + websites
Output: CSV with top-priority staff/admin pages per school
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import time
from typing import List, Dict, Set
import re
import pandas as pd


class PageDiscoverer:
    def __init__(self, timeout: int = 120, max_retries: int = 5):
        self.timeout = timeout  # Increased to 120 seconds
        self.max_retries = max_retries  # Increased retries
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        # Configurable thresholds
        self.min_priority_threshold = 20  # Minimum score to be included (pages below 20 are excluded)
        
        # Keywords to prioritize pages likely to have decision-maker info
        self.high_value_keywords = [
            'staff', 'faculty', 'directory', 'administration', 'admin', 'team',
            'leadership', 'our-team', 'who-we-are', 'meet-our', 'personnel',
            'board', 'principal', 'superintendent'
        ]
        # Removed 'mission', 'vision', 'history' from support - they're now zero priority
        self.support_value_keywords = ['about']
        self.low_value_keywords = ['info', 'location']  # Removed 'contact' - it's now zero priority
        
        # Keywords that immediately set score to 0 (ZERO priority - checked first)
        self.zero_priority_keywords = [
            # Contact pages
            'contact', 'contact-us', 'contactus', 'contact_us',
            # Admissions pages
            'admission', 'admissions', 'apply', 'enrollment', 'enroll',
            # Home and general pages
            'home', 'index',
            # Mission/vision/history pages (not useful for contacts)
            'mission', 'vision', 'history',
            # Calendar and events
            'calendar', 'event', 'events',
            # Sports and athletics
            'athletic', 'athletics', 'sports',
            # News and blog
            'news', 'blog',
            # Food service
            'lunch', 'menu', 'cafeteria', 'dining',
            # Forms and downloads
            'forms', 'download', 'downloads',
            # Employment
            'employment', 'jobs', 'careers', 'hiring',
            # Social media domains (in URL path)
            'linktr.ee', 'facebook.com', 'instagram.com', 'twitter.com',
            'youtube.com', 'vimeo.com', 'docs.google.com', 'drive.google.com'
        ]
        
        # These are now in zero_priority_keywords, keeping for backward compatibility if needed
        self.bad_url_keywords = [
            'calendar', 'athletic', 'sports', 'admission', 'apply', 'enroll',
            'event', 'news', 'blog', 'lunch', 'menu', 'forms', 'download',
            'linktr.ee', 'facebook.com', 'instagram.com', 'twitter.com',
            'youtube.com', 'vimeo.com', 'docs.google.com', 'drive.google.com'
        ]
        self.bad_domains = [
            'linktr.ee', 'facebook.com', 'instagram.com', 'twitter.com',
            'youtube.com', 'vimeo.com', 'docs.google.com', 'drive.google.com'
        ]
    
    def safe_get(self, url: str) -> requests.Response:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                print(f"      Timeout on attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
            except requests.exceptions.RequestException as e:
                print(f"      Request error: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        return None
    
    def extract_links(self, base_url: str, html: str) -> Set[str]:
        """Extract all internal links from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        links = set()
        
        # Get domain for filtering internal links only
        base_domain = urlparse(base_url).netloc
        
        # Keywords that indicate important hash fragments to preserve
        important_fragment_keywords = ['team', 'staff', 'faculty', 'leadership', 'directory', 
                                       'contact', 'about', 'administrat', 'office']
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)
            
            # Parse URL
            parsed = urlparse(full_url)
            
            # Only include internal links from same domain
            if parsed.netloc == base_domain:
                # Check if fragment contains important keywords
                fragment = parsed.fragment.lower() if parsed.fragment else ''
                has_important_fragment = any(keyword in fragment for keyword in important_fragment_keywords)
                
                # Preserve fragment if it contains important keywords, otherwise remove it
                if has_important_fragment and parsed.fragment:
                    # Keep the fragment for important sections
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}#{parsed.fragment}"
                else:
                    # Remove fragments and query params for cleaner URLs (standard behavior)
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                
                # Skip common non-content URLs
                skip_patterns = [
                    r'\.pdf$', r'\.jpg$', r'\.png$', r'\.gif$', r'\.jpeg$',
                    r'\.doc$', r'\.docx$', r'\.zip$', r'\.mp4$', r'\.mp3$',
                    r'/wp-admin/', r'/wp-login', r'/wp-content/uploads/',
                    r'javascript:', r'mailto:', r'tel:'
                ]
                
                # Skip patterns check (removed '#' from patterns since we're preserving some)
                if not any(re.search(pattern, clean_url, re.IGNORECASE) for pattern in skip_patterns):
                    links.add(clean_url)
        
        return links

    def score_page_priority(self, url: str) -> int:
        """Score URL based on likelihood of containing contact info"""
        url_lower = url.lower()
        parsed = urlparse(url_lower)
        netloc = parsed.netloc
        score = 0
        
        # ZERO PRIORITY: Contact and admissions pages (emails never there)
        for keyword in self.zero_priority_keywords:
            if keyword in url_lower:
                return 0  # Immediately return 0 - these pages are worthless
        
        for keyword in self.high_value_keywords:
            if keyword in url_lower:
                score += 25
        for keyword in self.support_value_keywords:
            if keyword in url_lower:
                score += 10
        for keyword in self.low_value_keywords:
            if keyword in url_lower:
                score += 5
        
        # Penalize low-value keywords / hosts
        for keyword in self.bad_url_keywords:
            if keyword in url_lower:
                score -= 25
        if any(bad_domain in netloc for bad_domain in self.bad_domains):
            score -= 40
        
        # EXTRA BOOST for hash fragments indicating team/staff pages
        if '#' in url_lower:
            hash_part = url_lower.split('#')[1]
            hash_keywords = ['team', 'staff', 'faculty', 'leadership', 'directory', 'admin']
            if any(keyword in hash_part for keyword in hash_keywords):
                score += 20
        
        return score
    
    def score_page_content(self, soup: BeautifulSoup) -> int:
        """Boost score based on page content (names, emails, headings)"""
        content_score = 0
        text = soup.get_text(separator=' ', strip=True)
        
        # Count emails on page
        mailto_links = soup.select('a[href^="mailto:"]')
        mailto_count = len(mailto_links)
        if mailto_count >= 5:
            content_score += 40
        elif mailto_count >= 2:
            content_score += 25
        elif mailto_count == 1:
            content_score += 10
        
        # Names pattern detection (simple heuristic)
        name_matches = re.findall(r'[A-Z][a-z]+\\s[A-Z][a-z]+', text)
        if len(name_matches) >= 10:
            content_score += 30
        elif len(name_matches) >= 5:
            content_score += 15
        
        # Heading keywords
        heading_text = ' '.join([h.get_text(separator=' ', strip=True).lower() for h in soup.find_all(['h1', 'h2', 'h3'])])
        for keyword in self.high_value_keywords:
            if keyword in heading_text:
                content_score += 10
                break
        
        return content_score

    def discover_pages(self, school_name: str, base_url: str, max_depth: int = 3, max_pages_per_school: int = 3, top_pages_limit: int = 3) -> List[Dict]:
        """
        Discover all pages on a school website
        
        Args:
            school_name: Name of the school
            base_url: Homepage URL
            max_depth: Maximum crawl depth (default 2)
            max_pages_per_school: Maximum pages to discover per school (default 30)
        
        Returns:
            List of page dictionaries with URL, title, and priority score
        """
        print(f"\n  Discovering pages for: {school_name}")
        print(f"  Base URL: {base_url}")
        
        if not base_url or base_url == '':
            print("    WARNING: No website URL provided")
            return []
        
        visited = set()
        # Use priority queue: (negative_priority, depth, url) - negative for max-heap behavior
        import heapq
        to_visit = []
        heapq.heappush(to_visit, (0, 0, base_url))  # Start with homepage (priority 0)
        discovered_pages = []
        high_priority_found = 0  # Count pages with score >= 40
        
        while to_visit and len(discovered_pages) < max_pages_per_school:
            # Pop highest priority page
            neg_priority, depth, current_url = heapq.heappop(to_visit)
            priority_estimate = -neg_priority
            
            # Skip if already visited or max depth reached
            if current_url in visited or depth > max_depth:
                continue
            
            # Early stopping: if we found enough pages, stop crawling
            if len(discovered_pages) >= max_pages_per_school:
                break
            
            # Skip zero-priority pages (contact, admissions, home, mission, etc.) - but allow homepage (depth 0)
            if priority_estimate <= 0 and depth > 0:
                continue
            
            visited.add(current_url)
            
            try:
                response = self.safe_get(current_url)
                if not response:
                    continue
                
                # Extract page title
                soup = BeautifulSoup(response.text, 'html.parser')
                title = soup.find('title')
                title_text = title.get_text().strip() if title else ''
                
                # Calculate priority score
                priority = self.score_page_priority(current_url)
                priority += self.score_page_content(soup)
                
                # Track high-priority pages
                if priority >= 30:
                    high_priority_found += 1
                
                # Store page info (we'll filter by threshold later)
                page_info = {
                    'school_name': school_name,
                    'url': current_url,
                    'title': title_text,
                    'priority_score': priority,
                    'depth': depth
                }
                discovered_pages.append(page_info)
                
                # Extract links for next level crawl (if not at max depth and not at page limit)
                if depth < max_depth and len(discovered_pages) < max_pages_per_school:
                    new_links = self.extract_links(base_url, response.text)
                    
                    # Prioritize links: score them before adding to queue
                    scored_links = []
                    for link in new_links:
                        if link not in visited:
                            link_priority = self.score_page_priority(link)
                            scored_links.append((link, link_priority))
                    
                    # Sort by priority and add top links first (increased to 75 per page for comprehensive coverage)
                    scored_links.sort(key=lambda x: x[1], reverse=True)
                    for link, link_priority in scored_links[:75]:
                        if link not in visited and len(discovered_pages) < max_pages_per_school:
                            heapq.heappush(to_visit, (-link_priority, depth + 1, link))
                
                time.sleep(0.5)  # Polite crawling delay
            
            except Exception as e:
                print(f"    Error crawling {current_url}: {e}")
                continue
        
        # Sort by priority score (highest first)
        discovered_pages.sort(key=lambda x: x['priority_score'], reverse=True)
        
        # Filter out pages below threshold (score must be >= 20)
        valid_pages = [page for page in discovered_pages if page['priority_score'] >= self.min_priority_threshold]
        
        # Limit to top 3 pages per school (highest scores)
        discovered_pages = valid_pages[:3]
        
        print(f"    Discovered {len(discovered_pages)} pages (top 3 with score >= {self.min_priority_threshold})")
        if discovered_pages:
            print(f"    Pages found:")
            for page in discovered_pages:
                print(f"      - {page['title'][:50]} (score: {page['priority_score']})")
        else:
            print(f"    No pages met the minimum threshold of {self.min_priority_threshold}")
        
        return discovered_pages

    def process_schools_csv(self, input_csv: str, output_csv: str, max_depth: int = 3, max_pages_per_school: int = 3, top_pages_limit: int = 3):
        """
        Process schools from Step 1 CSV and discover all their pages
        
        Args:
            input_csv: CSV file from Step 1 with school data
            output_csv: Output CSV with discovered pages
            max_depth: Maximum crawl depth
            max_pages_per_school: Maximum pages per school
        """
        print("\n" + "="*70)
        print("STEP 3: PAGE DISCOVERY")
        print("="*70)
        
        # Read input CSV
        df = pd.read_csv(input_csv)
        
        # Filter to schools with websites (process ALL schools)
        df_with_urls = df[df['website'].notna() & (df['website'] != '')]
        
        print(f"Processing {len(df_with_urls)} schools with websites")
        print("="*70 + "\n")
        
        all_pages = []
        
        for idx, row in df_with_urls.iterrows():
            school_name = row['name']
            base_url = row['website']
            
            print(f"\n[{idx + 1}/{len(df_with_urls)}] {school_name}")
            
            try:
                pages = self.discover_pages(school_name, base_url, max_depth=max_depth, max_pages_per_school=max_pages_per_school, top_pages_limit=top_pages_limit)
                all_pages.extend(pages)
                
            except Exception as e:
                print(f"    ERROR: {e}")
                continue
            
            # Save progress every 10 schools
            if (idx + 1) % 10 == 0:
                self._save_progress(all_pages, output_csv)
                print(f"\n  Progress saved: {len(all_pages)} pages discovered")
        
        # Final save
        self._save_progress(all_pages, output_csv)
        
        print("\n" + "="*70)
        print("PAGE DISCOVERY COMPLETE")
        print("="*70)
        print(f"Total pages discovered: {len(all_pages)}")
        print(f"Output file: {output_csv}")
        print("="*70)
        
    def _save_progress(self, pages: List[Dict], filename: str):
        """Save discovered pages to CSV"""
        if not pages:
            return
        
        df = pd.DataFrame(pages)
        df.to_csv(filename, index=False)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Discover pages on school websites')
    parser.add_argument('--input', required=True, help='Input CSV from Step 2')
    parser.add_argument('--output', default='step3_pages.csv', help='Output CSV filename')
    parser.add_argument('--max-depth', type=int, default=3, help='Maximum crawl depth (default: 3)')
    parser.add_argument('--max-pages-per-school', type=int, default=1000, help='Maximum pages to discover per school (default: 1000 - no practical limit)')
    parser.add_argument('--top-pages-limit', type=int, default=1000, help='Final filter: keep only top N pages per school by priority (default: 1000 - no practical limit)')
    
    args = parser.parse_args()
    
    discoverer = PageDiscoverer()
    discoverer.process_schools_csv(args.input, args.output, max_depth=args.max_depth, max_pages_per_school=args.max_pages_per_school, top_pages_limit=args.top_pages_limit)
