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
import subprocess
import gc
import os
from typing import List, Dict, Set, Optional
import pandas as pd
from collections import defaultdict

# Try to import psutil for process tree killing (optional)
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


class ContentCollector:
    def __init__(self, timeout: int = 10, max_retries: int = 1, use_selenium: bool = True, page_timeout: int = 30):
        self.timeout = timeout  # HTTP request timeout (10 seconds)
        self.max_retries = max_retries  # 1 retry only
        self.use_selenium = use_selenium
        self.page_timeout = page_timeout  # Max time to spend on a single page (30 seconds)
        self.driver = None
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        if use_selenium:
            self.driver = self._setup_selenium()
        
        # Email regex pattern (to check if we should try Selenium)
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    
    def extract_emails_from_html_only(self, html: str) -> Set[str]:
        """Fast regex-only email extraction (no parsing) - for initial quick check"""
        if not html:
            return set()
        
        emails = set()
        
        # Find emails in raw HTML using regex
        emails.update(self.email_pattern.findall(html))
        
        # Quick mailto extraction using regex
        mailto_pattern = re.compile(r'mailto:([^\s\'"<>?]+)', re.IGNORECASE)
        mailto_matches = mailto_pattern.findall(html)
        emails.update(mailto_matches)
        
        # Quick data-email extraction using regex
        data_email_pattern = re.compile(r'data-email=["\']([^\s\'"<>?]+)["\']', re.IGNORECASE)
        data_email_matches = data_email_pattern.findall(html)
        emails.update(data_email_matches)
        
        # Quick data-mailto extraction using regex
        data_mailto_pattern = re.compile(r'data-mailto=["\']([^\s\'"<>?]+)["\']', re.IGNORECASE)
        data_mailto_matches = data_mailto_pattern.findall(html)
        emails.update(data_mailto_matches)
        
        return emails
    
    def extract_emails(self, html: str, soup: Optional[BeautifulSoup] = None) -> Set[str]:
        """Extract all email addresses from HTML (comprehensive, uses BeautifulSoup if provided)"""
        if not html:
            return set()
        
        # If soup is provided, use it (avoid re-parsing)
        if soup is None:
            soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Find all emails in text
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
    
    def _setup_selenium(self, retry_count: int = 0, max_retries: int = 3):
        """
        Initialize headless Chrome browser with resource limits to prevent crashes.
        
        Args:
            retry_count: Current retry attempt (internal use)
            max_retries: Maximum number of retry attempts with cleanup
        """
        # Pre-creation cleanup: Kill orphaned processes before attempting to create driver
        # This is critical when previous drivers failed to cleanup properly
        # Always do cleanup, but more aggressive on retries
        if retry_count > 0:
            print(f"    🔧 Pre-creation cleanup (attempt {retry_count + 1}/{max_retries + 1})...")
            cleanup_wait = 2 + retry_count  # Longer wait on later retries
            kill_rounds = 3  # More aggressive on retries
        else:
            # Light cleanup on first attempt (especially important in parallel mode)
            cleanup_wait = 2  # Increased from 1 to ensure processes terminate
            kill_rounds = 2  # More aggressive even on first attempt
        
        try:
            # Kill chromedriver and chrome processes (more aggressive on retries)
            for round_num in range(kill_rounds):
                # Kill chromedriver processes
                subprocess.run(['pkill', '-9', 'chromedriver'], capture_output=True, timeout=3)
                # Kill chrome/chromium processes (headless)
                subprocess.run(['pkill', '-9', '-f', 'chrome.*headless'], capture_output=True, timeout=3)
                # Also kill chromium processes
                subprocess.run(['pkill', '-9', '-f', 'chromium'], capture_output=True, timeout=3)
                if round_num < kill_rounds - 1:  # Don't sleep after last round
                    time.sleep(0.5)  # Brief pause between kill rounds
            
            time.sleep(cleanup_wait)  # Wait for processes to fully terminate
            gc.collect()
        except Exception:
            pass  # Ignore cleanup errors
        
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        # Resource limits to prevent crashes
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')  # Don't load images to save memory
        # Note: We keep JavaScript enabled because we need it for click/hover interactions
        chrome_options.add_argument('--disable-background-networking')
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-renderer-backgrounding')
        chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        chrome_options.add_argument('--disable-ipc-flooding-protection')
        
        # Memory and process limits
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--max_old_space_size=512')  # Limit memory to 512MB per instance
        
        # Performance optimizations
        chrome_options.add_argument('--disable-features=TranslateUI')
        chrome_options.add_argument('--disable-ipc-flooding-protection')
        chrome_options.add_argument('--disable-hang-monitor')
        chrome_options.add_argument('--disable-prompt-on-repost')
        chrome_options.add_argument('--disable-sync')
        chrome_options.add_argument('--metrics-recording-only')
        chrome_options.add_argument('--no-first-run')
        chrome_options.add_argument('--safebrowsing-disable-auto-update')
        chrome_options.add_argument('--enable-automation')
        chrome_options.add_argument('--password-store=basic')
        chrome_options.add_argument('--use-mock-keychain')
        
        # Set preferences to reduce resource usage
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2,  # Block images
                'plugins': 2,  # Block plugins
                'popups': 2,  # Block popups
            },
            'profile.managed_default_content_settings': {
                'images': 2
            }
        }
        chrome_options.add_experimental_option('prefs', prefs)
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(15)  # 15 second timeout
            driver.implicitly_wait(2)  # Reduce implicit wait time
            return driver
        except Exception as e:
            if retry_count < max_retries:
                # Retry with exponential backoff and cleanup
                wait_time = 2 ** retry_count  # 1s, 2s, 4s
                print(f"    ⚠️  Failed to create Chrome driver (attempt {retry_count + 1}/{max_retries + 1}): {e}")
                print(f"    ⏳ Retrying in {wait_time}s after cleanup...")
                time.sleep(wait_time)
                return self._setup_selenium(retry_count=retry_count + 1, max_retries=max_retries)
            else:
                # Final attempt with minimal options
                print(f"    ⚠️  All retries exhausted, trying minimal options...")
                try:
                    minimal_options = Options()
                    minimal_options.add_argument('--headless')
                    minimal_options.add_argument('--no-sandbox')
                    minimal_options.add_argument('--disable-dev-shm-usage')
                    driver = webdriver.Chrome(options=minimal_options)
                    driver.set_page_load_timeout(15)
                    return driver
                except Exception as e2:
                    print(f"    ❌ Error: Could not create Chrome driver even with minimal options: {e2}")
                    raise
    
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
    
    def hard_reset_selenium(self, is_parallel: bool = False, use_nuclear: bool = False):
        """
        HARD RESET: Aggressively kill all Chrome/ChromeDriver processes.
        
        This is a nuclear option to ensure all Selenium processes are fully terminated.
        Used after every N counties to prevent process accumulation.
        
        Args:
            is_parallel: If True, we're in parallel mode (for logging).
            use_nuclear: If True, always use nuclear option (kill all Chrome processes).
                        This should be True at checkpoints even in parallel mode.
        """
        killed_processes = []
        driver_service_pid = None
        
        # Step 1: Graceful quit (but save service PID first)
        if self.driver:
            try:
                # Save service PID before quitting
                if hasattr(self.driver, 'service'):
                    service = self.driver.service
                    if service and hasattr(service, 'process') and service.process:
                        driver_service_pid = service.process.pid
                
                self.driver.quit()
                print("    ✓ Selenium driver quit (graceful)")
            except Exception as e:
                print(f"    Warning: Graceful quit failed: {e}")
            finally:
                self.driver = None
        
        # Step 2: Kill driver service process (if we saved the PID)
        if driver_service_pid:
            try:
                if HAS_PSUTIL:
                    try:
                        parent = psutil.Process(driver_service_pid)
                        parent.kill()
                        parent.wait(timeout=5)
                        killed_processes.append(f"service:{driver_service_pid}")
                        print(f"    ✓ Killed driver service process {driver_service_pid}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                else:
                    # Fallback: try to kill by PID directly
                    try:
                        os.kill(driver_service_pid, 9)
                        killed_processes.append(f"service:{driver_service_pid}")
                        print(f"    ✓ Killed driver service process {driver_service_pid}")
                    except (ProcessLookupError, PermissionError):
                        pass
            except Exception as e:
                print(f"    Warning: Service kill failed: {e}")
        
        # Step 3: Process tree kill (if psutil available and we have a PID)
        if HAS_PSUTIL and driver_service_pid:
            try:
                parent = psutil.Process(driver_service_pid)
                for child in parent.children(recursive=True):
                    try:
                        child.kill()
                        killed_processes.append(f"child:{child.pid}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                if killed_processes:
                    child_count = len([p for p in killed_processes if 'child' in p])
                    if child_count > 0:
                        print(f"    ✓ Killed {child_count} child processes")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception as e:
                print(f"    Warning: Process tree kill failed: {e}")
        
        # Step 4: ALWAYS use nuclear option - kill ALL Chrome/ChromeDriver processes aggressively
        # Multiple rounds to ensure everything is killed
        try:
            killed_any = False
            for round_num in range(3):  # 3 rounds of aggressive killing
                # Round 1: Kill chromedriver processes
                result1 = subprocess.run(
                    ['pkill', '-9', 'chromedriver'],
                    capture_output=True,
                    timeout=5
                )
                if result1.returncode == 0:
                    killed_any = True
                
                # Round 2: Kill chrome/chromium processes (headless)
                result2 = subprocess.run(
                    ['pkill', '-9', '-f', 'chrome.*headless'],
                    capture_output=True,
                    timeout=5
                )
                if result2.returncode == 0:
                    killed_any = True
                
                # Round 3: Kill chromium processes (catch-all)
                result3 = subprocess.run(
                    ['pkill', '-9', '-f', 'chromium'],
                    capture_output=True,
                    timeout=5
                )
                if result3.returncode == 0:
                    killed_any = True
                
                # Round 4: Kill any remaining chrome processes (not just headless)
                result4 = subprocess.run(
                    ['pkill', '-9', 'chrome'],
                    capture_output=True,
                    timeout=5
                )
                if result4.returncode == 0:
                    killed_any = True
                
                if round_num < 2:  # Don't sleep after last round
                    time.sleep(0.5)  # Brief pause between rounds
            
            if killed_any:
                print("    ✓ Killed all Chrome/ChromeDriver processes (nuclear, 3 rounds)")
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            # pkill might not be available or might fail - that's okay
            print(f"    Note: Nuclear cleanup had issues: {e}")
        
        # Step 5: Wait for processes to fully terminate (longer wait for thorough cleanup)
        time.sleep(4)  # Increased from 3 to 4 seconds
        
        # Step 6: Force garbage collection (multiple times for thorough cleanup)
        gc.collect()
        time.sleep(0.5)
        gc.collect()
        
        # Step 7: Final wait before recreating
        time.sleep(2)
        
        print(f"    ✓ Hard reset complete (nuclear, killed {len(killed_processes)} tracked processes + all Chrome/ChromeDriver)")
    
    def cleanup(self):
        """Cleanup Selenium driver resources (standard cleanup)"""
        if self.driver:
            try:
                self.driver.quit()
                print("    ✓ Selenium driver quit successfully")
            except Exception as e:
                # Silently handle cleanup errors - don't crash if cleanup fails
                print(f"    Warning: Error quitting Selenium driver: {e}")
            finally:
                self.driver = None
        else:
            print("    ✓ No Selenium driver to cleanup")
    
    def __del__(self):
        """Destructor - safety net to ensure driver is cleaned up"""
        self.cleanup()
    
    def safe_get(self, url: str) -> requests.Response:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                # Silent retry - don't print errors
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None  # Return None instead of raising
            except requests.exceptions.RequestException as e:
                # Silent retry - don't print errors
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None  # Return None instead of raising
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
    
    def collect_page_content(self, school_name: str, url: str) -> Optional[Dict]:
        """
        Collect HTML content from a page using OPTIMIZED approach:
        
        1. Check page titles (H2, H3, H4) FIRST to determine if page is relevant
        2. Extract emails using regex (fast, no LLM needed)
        3. Only process full HTML if page passes initial checks
        4. Timeout after 2-3 minutes if page is unproductive
        
        Returns:
            Dictionary with school_name, url, html_content, fetch_method, email_count
            Returns None if page fetch failed or timed out
        """
        import time
        page_start_time = time.time()
        
        try:
            html = None
            fetch_method = 'unknown'
            
            # TIER 1: Try Beautiful Soup first (simple HTML scraping)
            response = self.safe_get(url)
            if not response:
                # If requests failed, try Selenium directly (if enabled)
                if self.use_selenium:
                    print(f"    WARNING: Requests failed, trying Selenium...")
                    html_selenium = self.fetch_with_selenium(url, interact=True)
                    if html_selenium:
                        html = html_selenium
                        fetch_method = 'selenium'
                else:
                    print(f"    ERROR: Failed to fetch page content")
                    return None
            else:
                html = response.text
                fetch_method = 'requests'
            
            # Check timeout - skip if we've spent too much time
            elapsed = time.time() - page_start_time
            if elapsed > self.page_timeout:
                print(f"    ⚠️  Page timeout ({elapsed:.1f}s > {self.page_timeout}s) - skipping")
                return None
            
            # OPTIMIZATION: Fast regex email extraction FIRST (no parsing)
            emails_fast = self.extract_emails_from_html_only(html)
            
            # OPTIMIZATION: Parse HTML ONCE and reuse soup object
            soup = BeautifulSoup(html, 'html.parser')
            
            # Check titles (H1, H2, H3, H4, title) for relevance
            title_tags = soup.find_all(['h1', 'h2', 'h3', 'h4', 'title'])
            title_text = ' '.join([tag.get_text(strip=True) for tag in title_tags]).lower()
            
            # High-value keywords that indicate staff/admin pages
            high_value_keywords = [
                'principal', 'superintendent', 'head of school', 'director',
                'administrator', 'dean', 'leadership', 'administration', 'staff', 'faculty'
            ]
            has_high_value_titles = any(keyword in title_text for keyword in high_value_keywords)
            
            # OPTIMIZATION: Use comprehensive email extraction with existing soup object
            emails = self.extract_emails(html, soup=soup)
            
            # Check if page is worth processing
            # Skip if no emails AND no high-value titles (likely not a staff page)
            if not emails and not has_high_value_titles:
                # Check for name patterns in titles as last resort
                name_pattern = re.search(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', title_text)
                if not name_pattern:
                    print(f"    ⚠️  Page doesn't appear relevant (no emails, no admin titles) - skipping")
                    return None
            
            # Check timeout again before heavy processing
            elapsed = time.time() - page_start_time
            if elapsed > self.page_timeout:
                print(f"    ⚠️  Page timeout ({elapsed:.1f}s > {self.page_timeout}s) - skipping")
                return None
            
            # Cache full text (expensive operation, only do once)
            full_text = soup.get_text()
            text_lower = full_text.lower()
            has_high_value_titles_full = any(keyword in text_lower for keyword in high_value_keywords)
            name_pattern = re.search(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', full_text)
            has_name_pattern = bool(name_pattern)
            
            # OPTIMIZED Selenium logic: Skip if emails AND high-value titles found
            # Use Selenium only if:
            # 1. No emails found, OR
            # 2. Has high-value titles/names BUT no emails (might be hidden)
            # Skip Selenium if: emails found AND high-value titles found (already have what we need)
            should_use_selenium = False
            if not emails or len(emails) == 0:
                # No emails found - try Selenium
                should_use_selenium = True
            elif has_high_value_titles_full and has_name_pattern and not emails:
                # Has high-value content but no emails - might be hidden, try Selenium
                should_use_selenium = True
            # If emails found AND high-value titles found, skip Selenium (already have what we need)
            
            if should_use_selenium:
                # Check timeout before Selenium (expensive operation)
                elapsed = time.time() - page_start_time
                if elapsed > self.page_timeout * 0.8:  # Use 80% of timeout for Selenium
                    print(f"    ⚠️  Skipping Selenium (approaching timeout: {elapsed:.1f}s)")
                elif self.use_selenium:
                    if not emails or len(emails) == 0:
                        print(f"    No emails in HTML, trying Selenium (click/hover reveals)...")
                    else:
                        print(f"    Found {len(emails)} emails + high-value titles detected, using Selenium for comprehensive extraction...")
                    
                    # TIER 2: Use Selenium for better extraction
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
            
            # Final timeout check
            elapsed = time.time() - page_start_time
            if elapsed > self.page_timeout:
                print(f"    ⚠️  Page timeout exceeded ({elapsed:.1f}s > {self.page_timeout}s) - returning partial results")
            
            # Count emails found
            email_count = len(emails) if emails else 0
            
            return {
                'school_name': school_name,
                'url': url,
                'html_content': html,
                'fetch_method': fetch_method,
                'email_count': email_count,
                'has_emails': email_count > 0
            }
        
        except Exception as e:
            elapsed = time.time() - page_start_time
            print(f"      Error collecting content from {url}: {e} [{elapsed:.1f}s]")
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
