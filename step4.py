"""
STEP 4: COLLECT PAGE CONTENT
============================
Collect HTML/text content from the prioritized pages discovered in Step 3.

FALLBACK APPROACH (per Giorgio's recommendation):
1. TIER 1: Beautiful Soup (simple HTML) - fast, cheap (~40% of sites)
2. TIER 2: If no emails found: Selenium (click/hover to reveal hidden emails)

This step ONLY collects content - LLM parsing happens in Step 5.

Input: CSV from Step 3 with high-value staff/admin pages
Output: CSV with page URLs and their collected HTML/text content
"""

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
import re
import csv
import time
from typing import List, Dict, Set, Optional
import pandas as pd
from collections import defaultdict


class ContentCollector:
    def __init__(self, timeout: int = 120, max_retries: int = 5, use_selenium: bool = True):
        self.timeout = timeout  # Increased to 120 seconds
        self.max_retries = max_retries  # Increased retries
        self.use_selenium = use_selenium
        self.driver = None
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        if use_selenium:
            self.driver = self._setup_selenium()
        
        # Email regex pattern (to check if we should try Selenium)
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    
    def extract_emails(self, html: str) -> Set[str]:
        """Extract all email addresses from HTML"""
        if not html:
            return set()
        
        # Parse HTML to get text content
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Find all emails
        emails = set(self.email_pattern.findall(text))
        
        # Also check href attributes for mailto links
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('mailto:'):
                email = link['href'].replace('mailto:', '').split('?')[0]
                emails.add(email)
        
        # Check data attributes that might contain emails
        for element in soup.find_all(attrs={'data-email': True}):
            emails.add(element.get('data-email', ''))
        
        for element in soup.find_all(attrs={'data-mailto': True}):
            emails.add(element.get('data-mailto', ''))
        
        return emails
    
    def _setup_selenium(self):
        """Initialize headless Chrome browser"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(180)  # Increased to 180 seconds
        return driver
    
    def _ensure_driver_healthy(self):
        """Check and restart Selenium driver if needed"""
        if not self.driver:
            return
        
        try:
            self.driver.execute_script("return document.readyState")
        except:
            print("    Selenium driver crashed. Restarting...")
            try:
                self.driver.quit()
            except:
                pass
            self.driver = self._setup_selenium()
    
    def safe_get(self, url: str) -> requests.Response:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        return None
    
    def fetch_with_selenium(self, url: str, interact: bool = True) -> Optional[str]:
        """
        Fetch page using Selenium and interact with it to reveal emails
        
        Clicks on profile photos, staff cards, and hovers over elements
        to reveal hidden emails (mailto links that appear on interaction)
        """
        try:
            self._ensure_driver_healthy()
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)  # Wait for any dynamic content
            
            if interact:
                # Look for clickable elements that might reveal emails
                # Common patterns: profile photos, staff cards, staff member containers
                clickable_selectors = [
                    "img[alt*='staff']", "img[alt*='team']", "img[alt*='faculty']",
                    "[class*='staff']", "[class*='team']", "[class*='faculty']",
                    "[class*='member']", "[class*='profile']", "[class*='card']",
                    "a[href*='staff']", "a[href*='team']", "a[href*='faculty']",
                    "[data-email]", "[data-mailto]", "[class*='email']"
                ]
                
                for selector in clickable_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        # Increased to 75 clicks per selector type to handle large staff directories
                        for element in elements[:75]:
                            try:
                                # Scroll element into view
                                self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                time.sleep(0.5)
                                
                                # Try clicking
                                element.click()
                                time.sleep(0.5)
                                
                                # Try hovering
                                ActionChains(self.driver).move_to_element(element).perform()
                                time.sleep(0.3)
                            except:
                                continue
                    except:
                        continue
                
                # Additional wait for any JavaScript-revealed emails
                time.sleep(2)
            
            return self.driver.page_source
            
        except Exception as e:
            print(f"      Selenium error: {e}")
            return None
    
    def extract_emails(self, html: str) -> Set[str]:
        """Extract all email addresses from HTML"""
        # Parse HTML to get text content
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Find all emails
        emails = set(self.email_pattern.findall(text))
        
        # Also check href attributes for mailto links
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('mailto:'):
                email = link['href'].replace('mailto:', '').split('?')[0]
                emails.add(email)
        
        # Check data attributes that might contain emails
        for element in soup.find_all(attrs={'data-email': True}):
            emails.add(element.get('data-email', ''))
        
        for element in soup.find_all(attrs={'data-mailto': True}):
            emails.add(element.get('data-mailto', ''))
        
        return emails
    
    def _setup_selenium(self):
        """Initialize headless Chrome browser"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(180)  # Increased to 180 seconds
        return driver
    
    def collect_page_content(self, school_name: str, url: str) -> Optional[Dict]:
        """
        Collect HTML content from a page using FALLBACK approach:
        
        1. TIER 1: Try Beautiful Soup (simple HTML) - fast, cheap (~40% of sites)
        2. TIER 2: If no emails found: Try Selenium (click/hover to reveal hidden emails)
        
        Returns:
            Dictionary with school_name, url, html_content, fetch_method, email_count
            Returns None if page fetch failed
        """
        try:
            html = None
            fetch_method = 'unknown'
            
            # TIER 1: Try Beautiful Soup first (simple HTML scraping)
            response = self.safe_get(url)
            if response:
                html = response.text
                fetch_method = 'requests'
                
                # Check for emails and staff indicators
                emails = self.extract_emails(html)
                soup = BeautifulSoup(html, 'html.parser')
                text_lower = soup.get_text().lower()
                
                # Check for high-value titles/names that indicate staff pages
                high_value_keywords = [
                    'principal', 'superintendent', 'head of school', 'director',
                    'administrator', 'dean', 'assistant principal', 'vice principal'
                ]
                has_high_value_titles = any(keyword in text_lower for keyword in high_value_keywords)
                
                # Check for name patterns (likely person names)
                name_pattern = re.search(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', soup.get_text())
                has_name_pattern = bool(name_pattern)
                
                # Use Selenium if:
                # 1. No emails found, OR
                # 2. Has high-value titles/names (even if emails found - might be more in Selenium)
                should_use_selenium = (not emails or len(emails) == 0) or (has_high_value_titles and has_name_pattern)
                
                if should_use_selenium:
                    if not emails or len(emails) == 0:
                        print(f"    No emails in HTML, trying Selenium (click/hover reveals)...")
                    else:
                        print(f"    Found {len(emails)} emails + high-value titles detected, using Selenium for comprehensive extraction...")
                    
                    # TIER 2: Use Selenium for better extraction
                    if self.use_selenium:
                        html_selenium = self.fetch_with_selenium(url, interact=True)
                        if html_selenium:
                            html = html_selenium
                            fetch_method = 'selenium'
                            
                            # Re-check emails after Selenium
                            emails_after = self.extract_emails(html)
                            if emails_after:
                                print(f"    Found {len(emails_after)} emails via Selenium (was {len(emails) if emails else 0})")
                                emails = emails_after
                else:
                    print(f"    Found {len(emails)} emails via simple HTML")
            else:
                # If requests failed, try Selenium directly
                if self.use_selenium:
                    print(f"    WARNING: Requests failed, trying Selenium...")
                    html_selenium = self.fetch_with_selenium(url, interact=True)
                    if html_selenium:
                        html = html_selenium
                        fetch_method = 'selenium'
            
            # If we still have no HTML at this point, page fetch failed
            if not html:
                print(f"    ERROR: Failed to fetch page content")
                return None
            
            # Count emails found
            emails = self.extract_emails(html)
            email_count = len(emails)
            
            return {
                'school_name': school_name,
                'url': url,
                'html_content': html,
                'fetch_method': fetch_method,
                'email_count': email_count,
                'has_emails': email_count > 0
            }
        
        except Exception as e:
            print(f"      Error collecting content from {url}: {e}")
            return None
    
    def collect_content_from_pages(self, input_csv: str, output_csv: str):
        """
        Collect HTML content from all pages in the input CSV
        
        Process:
        1. Read all pages from Step 3
        2. For each page: Use fallback approach (Beautiful Soup → Selenium)
        3. Save page content (HTML) to CSV for LLM parsing in Step 5
        
        Uses FALLBACK approach per Giorgio's recommendation:
        - TIER 1: Try Beautiful Soup first (fast, cheap) - handles ~40% of sites
        - TIER 2: If no emails: Try Selenium (click/hover to reveal)
        
        This step ONLY collects content - LLM parsing happens in Step 5.
        
        Args:
            input_csv: CSV from Step 3 with discovered pages
            output_csv: Output CSV with collected page content
        """
        print("\n" + "="*70)
        print("STEP 4: COLLECTING PAGE CONTENT")
        print("="*70)
        print("Using FALLBACK approach: Beautiful Soup → Selenium")
        print("="*70)
        
        # Read discovered pages
        df = pd.read_csv(input_csv)
        
        print(f"Processing {len(df)} pages from {df['school_name'].nunique()} schools")
        print("="*70 + "\n")
        
        all_content = []
        
        for idx, row in df.iterrows():
            school_name = row['school_name']
            url = row['url']
            
            print(f"\n[{idx+1}/{len(df)}] {school_name}")
            print(f"  URL: {url[:70]}...")
            
            # Collect content from this page
            content = self.collect_page_content(school_name, url)
            
            if content:
                all_content.append(content)
                print(f"  Collected content ({content['fetch_method']}) - {content['email_count']} emails found")
            else:
                print(f"  ERROR: Failed to collect content")
            
            # Save progress every 10 pages
            if (idx + 1) % 10 == 0:
                self._save_content(all_content, output_csv)
                print(f"\n  Progress saved: {len(all_content)} pages collected so far")
            
            time.sleep(0.5)  # Polite delay
        
        # Final save
        self._save_content(all_content, output_csv)
        
        # Print final summary
        self._print_summary_content(all_content, output_csv, df)
    
    def _save_content(self, content_list: List[Dict], filename: str):
        """Save page content to CSV"""
        if not content_list:
            return
        
        df = pd.DataFrame(content_list)
        
        # Ensure all required columns exist
        required_cols = ['school_name', 'url', 'html_content', 'fetch_method', 'email_count', 'has_emails']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
        
        # Reorder columns
        df = df[required_cols]
        
        df.to_csv(filename, index=False)
    
    def _print_summary_content(self, content_list: List[Dict], output_file: str, pages_df):
        """Print final summary"""
        if not content_list:
            print("\nERROR: No content collected")
            return
        
        df = pd.DataFrame(content_list)
        
        schools_processed = pages_df['school_name'].nunique()
        schools_with_content = df['school_name'].nunique()
        total_emails = df['email_count'].sum()
        pages_with_emails = df[df['has_emails'] == True]
        
        print("\n" + "="*70)
        print("CONTENT COLLECTION COMPLETE")
        print("="*70)
        print(f"Pages processed: {len(pages_df)}")
        print(f"Pages with content collected: {len(df)}")
        print(f"Schools processed: {schools_processed}")
        print(f"Schools with content: {schools_with_content}/{schools_processed} ({schools_with_content/schools_processed*100:.1f}%)")
        print(f"Pages with emails: {len(pages_with_emails)} ({len(pages_with_emails)/len(df)*100:.1f}%)")
        print(f"Total emails found: {total_emails}")
        
        # Breakdown by fetch method
        if 'fetch_method' in df.columns:
            method_counts = df['fetch_method'].value_counts()
            print(f"\nFetch method breakdown:")
            for method, count in method_counts.items():
                print(f"  {method}: {count} pages ({count/len(df)*100:.1f}%)")
        
        print(f"\nOutput file: {output_file}")
        print("="*70)
        print("📝 Next step: Run Step 5 to parse content with LLM")
        print("="*70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect HTML content from Step 3 pages')
    parser.add_argument('--input', required=True, help='Input CSV from Step 3')
    parser.add_argument('--output', default='step4_content.csv', help='Output CSV filename')
    parser.add_argument('--no-selenium', action='store_true', help='Disable Selenium (use requests only)')
    
    args = parser.parse_args()
    
    collector = ContentCollector(use_selenium=not args.no_selenium)
    
    try:
        collector.collect_content_from_pages(args.input, args.output)
    finally:
        # Cleanup Selenium driver
        if collector.driver:
            try:
                collector.driver.quit()
            except:
                pass
