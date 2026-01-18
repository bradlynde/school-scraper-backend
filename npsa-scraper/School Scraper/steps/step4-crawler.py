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
from selenium.webdriver.chrome.service import Service
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
import threading
import platform  # For OS detection
from typing import List, Dict, Set, Optional
import pandas as pd
from collections import defaultdict

# Try to import psutil for process tree killing (optional)
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# ANSI escape codes for bold text
BOLD = '\033[1m'
RESET = '\033[0m'

def bold(text: str) -> str:
    """Make text bold in terminal output"""
    return f"{BOLD}{text}{RESET}"


class ContentCollector:
    def __init__(self, timeout: int = 10, max_retries: int = 1, use_selenium: bool = True, page_timeout: int = 45):
        self.timeout = timeout  # HTTP request timeout (10 seconds)
        self.max_retries = max_retries  # 1 retry only
        self.use_selenium = use_selenium
        self.page_timeout = page_timeout  # Max time to spend on a single page (45 seconds)
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
        Initialize headless Chrome browser with resource limits.
        
        Args:
            retry_count: Current retry attempt (internal use)
            max_retries: Maximum number of retry attempts
        """
        
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
        
        driver = None
        try:
            # Use explicit ChromeDriver path to bypass Selenium Manager network issues
            service = Service(executable_path=os.getenv('CHROMEDRIVER_PATH', '/usr/bin/chromedriver'))
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(45)  # 45 second timeout
            driver.set_script_timeout(45)  # Script timeout
            # Remove implicit wait, use explicit waits instead
            # driver.implicitly_wait(2)  # Removed - use explicit waits
            return driver
        except Exception as e:
            if retry_count < max_retries:
                # Cleanup any leftover Chrome processes before retry (bottom-up)
                self._kill_all_chrome_processes()
                
                # Retry with exponential backoff
                wait_time = 2 ** retry_count  # 1s, 2s, 4s
                print(f"    {bold('[SELENIUM]')} Failed to create Chrome driver (attempt {retry_count + 1}/{max_retries + 1}): {e}")
                print(f"    {bold('[SELENIUM]')} Retrying in {wait_time}s...")
                time.sleep(wait_time)
                return self._setup_selenium(retry_count=retry_count + 1, max_retries=max_retries)
            else:
                # Final attempt with minimal options
                # Cleanup any leftover Chrome processes before final attempt (bottom-up)
                self._kill_all_chrome_processes()
                print(f"    {bold('[SELENIUM]')} All retries exhausted, trying minimal options...")
            try:
                minimal_options = Options()
                minimal_options.add_argument('--headless')
                minimal_options.add_argument('--no-sandbox')
                minimal_options.add_argument('--disable-dev-shm-usage')
                # Always use explicit ChromeDriver path to bypass Selenium Manager network lookups
                chromedriver_path = os.getenv('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
                service = Service(executable_path=chromedriver_path)
                driver = webdriver.Chrome(service=service, options=minimal_options)
                driver.set_page_load_timeout(45)
                driver.set_script_timeout(45)
                print(f"    {bold('[SELENIUM]')} Driver created with minimal options")
                return driver
            except Exception as e2:
                print(f"    {bold('[SELENIUM]')} ERROR: Could not create Chrome driver even with minimal options: {e2}")
                raise
    
    def _ensure_driver_healthy(self):
        """Check and restart Selenium driver if needed"""
        if not self.driver:
            # Driver doesn't exist, create it
            self.driver = self._setup_selenium()
            return
        
        try:
            self.driver.execute_script("return document.readyState")
        except:
            print(f"    {bold('[SELENIUM]')} Driver crashed, restarting...")
            driver = None
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass  # Don't let cleanup fail
            
            # Cleanup any leftover Chrome processes before restart (bottom-up)
            self._kill_all_chrome_processes()
            self.driver = None
            
            # Restart the driver
            self.driver = self._setup_selenium()
    
    def cleanup(self):
        """Basic cleanup: quit Selenium driver and kill all Chrome processes (bottom-up)"""
        # Cleanup Chrome processes first (bottom-up) before quitting driver
        self._kill_all_chrome_processes()
        
        driver = None
        try:
            if self.driver:
                driver = self.driver
                self.driver = None
                driver.quit()
                print(f"    {bold('[SELENIUM]')} Driver quit")
        except Exception as e:
            print(f"    {bold('[SELENIUM]')} WARNING: Error quitting driver: {e}")
        finally:
            self.driver = None
    
    def __del__(self):
        """Destructor - safety net to ensure driver is cleaned up"""
        self.cleanup()
    
    def _kill_all_chrome_processes(self):
        """
        Deep cleanup of all Chrome/Chromium/ChromeDriver child processes.
        Kills processes BOTTOM-UP (children first, then parents) to prevent zombies.
        Only targets processes in the current worker's process tree (not system-wide).
        Protects main container processes (waitress-serve, main Python process).
        
        Returns:
            int: Number of processes killed
        """
        if not HAS_PSUTIL:
            return 0
        
        killed_count = 0
        try:
            current_pid = os.getpid()
            current_process = psutil.Process(current_pid)
            
            # Identify main container processes to protect
            # These should never be killed, even if they match patterns
            protected_names = {'waitress-serve', 'dumb-init', 'python', 'python3'}
            protected_pids = set()
            
            # Get main container PID (parent or grandparent process)
            try:
                parent = current_process.parent()
                if parent:
                    protected_pids.add(parent.pid)
                    # Also protect parent's parent (grandparent, likely waitress)
                    try:
                        grandparent = parent.parent()
                        if grandparent:
                            protected_pids.add(grandparent.pid)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            
            # Collect all Chrome processes in worker's process tree
            chrome_processes = []
            try:
                for child in current_process.children(recursive=True):
                    try:
                        name = child.info.get('name', '').lower()
                        # Check if it's a Chrome process
                        if ('chrome' in name or 'chromium' in name or 'chromedriver' in name):
                            # Skip protected processes
                            if child.pid not in protected_pids:
                                if name not in protected_names:
                                    chrome_processes.append(child)
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, KeyError):
                        continue
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            
            if not chrome_processes:
                return 0
            
            # Sort processes by depth (deepest children first) - BOTTOM-UP approach
            # Build depth map: calculate depth of each process from root
            depth_map = {}
            def get_depth(proc):
                if proc.pid in depth_map:
                    return depth_map[proc.pid]
                try:
                    if proc.pid == current_pid:
                        depth = 0
                    else:
                        parent_depth = get_depth(proc.parent()) if proc.parent() else 0
                        depth = parent_depth + 1
                    depth_map[proc.pid] = depth
                    return depth
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    return 999  # Unknown depth, kill last
            
            # Calculate depths
            for proc in chrome_processes:
                get_depth(proc)
            
            # Sort by depth descending (deepest = children first, shallowest = parents last)
            chrome_processes.sort(key=lambda p: depth_map.get(p.pid, 999), reverse=True)
            
            # Kill processes BOTTOM-UP (deepest children first)
            for proc in chrome_processes:
                try:
                    if proc.is_running():
                        # Try graceful termination first
                        proc.terminate()
                        killed_count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
            
            # Wait a moment for processes to die
            if killed_count > 0:
                time.sleep(0.5)
                
                # Force kill any remaining processes
                for proc in chrome_processes:
                    try:
                        if proc.is_running():
                            proc.kill()  # Force kill if still running
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        continue
            
            if killed_count > 0:
                print(f"    {bold('[SELENIUM]')} Killed {killed_count} Chrome child processes (bottom-up)")
            
        except Exception as e:
            print(f"    {bold('[SELENIUM]')} WARNING: Error in Chrome process cleanup: {e}")
        
        return killed_count
    
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
    
    def _get_url_with_timeout(self, driver, url: str, timeout: int = 45) -> bool:
        """
        Wrapper for driver.get() with a hard 45-second timeout.
        If the page load exceeds the timeout, forcefully quits the driver.
        
        Returns:
            True if page loaded successfully, False if timeout occurred
        """
        result = [None]  # Use list to allow modification from nested function
        exception = [None]
        
        def get_url():
            try:
                driver.get(url)
                result[0] = True
            except Exception as e:
                exception[0] = e
                result[0] = False
        
        # Start driver.get() in a separate thread
        thread = threading.Thread(target=get_url, daemon=True)
        thread.start()
        thread.join(timeout=timeout)
        
        # Check if thread is still alive (timed out)
        if thread.is_alive():
            # Timeout occurred - forcefully quit driver
            print(f"      [TIMEOUT] Page load exceeded {timeout}s, forcefully terminating...")
            try:
                driver.quit()
            except:
                pass
            # No cleanup needed - subprocess will die naturally and take children with it
            # Mark driver as dead so it gets recreated
            self.driver = None
            return False
        
        # Check if an exception occurred
        if exception[0]:
            raise exception[0]
        
        return result[0] if result[0] is not None else False
    
    def fetch_with_selenium(self, url: str, interact: bool = True) -> Optional[str]:
        """
        Fetch page using Selenium and interact with it to reveal emails
        
        Clicks on profile photos, staff cards, and hovers over elements
        to reveal hidden emails (mailto links that appear on interaction)
        """
        driver = None
        try:
            self._ensure_driver_healthy()
            driver = self.driver
            
            # Use timeout wrapper for driver.get() - 45 second hard limit
            if not self._get_url_with_timeout(driver, url, timeout=45):
                print(f"      [TIMEOUT] Failed to load page within 45s timeout")
                # Driver was killed, need to recreate it
                self.driver = None
                return None
            
            # Wait for page to load using explicit wait (with shorter timeout since page should be loaded)
            try:
                wait = WebDriverWait(driver, 5)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except TimeoutException:
                # Page might be loaded but body not ready - continue anyway
                pass
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
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        # Increased to 75 clicks per selector type to handle large staff directories
                        for element in elements[:75]:
                            try:
                                # Scroll element into view
                                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                time.sleep(0.5)
                                
                                # Try clicking
                                element.click()
                                time.sleep(0.5)
                                
                                # Try hovering
                                ActionChains(driver).move_to_element(element).perform()
                                time.sleep(0.3)
                            except:
                                continue
                    except:
                        continue
                
                # Additional wait for any JavaScript-revealed emails
                time.sleep(2)
            
            return driver.page_source
            
        except Exception as e:
            print(f"      Selenium error: {e}")
            return None
        finally:
            # Cleanup handled by cleanup() method, but ensure driver reference is maintained
            pass
    
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
                print(f"    [TIMEOUT] Page timeout ({elapsed:.1f}s > {self.page_timeout}s) - skipping")
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
                    print(f"    [SKIP] Page doesn't appear relevant (no emails, no admin titles) - skipping")
                    return None
            
            # Check timeout again before heavy processing
            elapsed = time.time() - page_start_time
            if elapsed > self.page_timeout:
                print(f"    [TIMEOUT] Page timeout ({elapsed:.1f}s > {self.page_timeout}s) - skipping")
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
                    print(f"    [SKIP] Skipping Selenium (approaching timeout: {elapsed:.1f}s)")
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
                print(f"    [TIMEOUT] Page timeout exceeded ({elapsed:.1f}s > {self.page_timeout}s) - returning partial results")
            
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
        # Read discovered pages
        df = pd.read_csv(input_csv)
        print(f"{bold('[STEP 4]')} Processing {len(df)} pages from {df['school_name'].nunique()} schools")
        
        all_content = []
        
        for idx, row in df.iterrows():
            school_name = row['school_name']
            url = row['url']
            
            # Collect content from this page
            content = self.collect_page_content(school_name, url)
            
            if content:
                all_content.append(content)
                if (idx + 1) % 10 == 0:
                    print(f"{bold('[STEP 4]')} Progress: {idx+1}/{len(df)} pages, {len(all_content)} collected, {sum(c.get('email_count', 0) for c in all_content)} emails")
            else:
                if (idx + 1) % 10 == 0:
                    print(f"{bold('[STEP 4]')} Progress: {idx+1}/{len(df)} pages, {len(all_content)} collected")
            
            # Save progress every 10 pages
            if (idx + 1) % 10 == 0:
                self._save_content(all_content, output_csv)
            
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
        
        print(f"{bold('[STEP 4]')} Complete: {len(df)}/{len(pages_df)} pages collected, {schools_with_content}/{schools_processed} schools, {total_emails} emails")


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
        # Cleanup Selenium driver with nuclear option
        driver = None
        try:
            if collector.driver:
                driver = collector.driver
                collector.driver = None
                driver.quit()
        except:
            pass  # Don't let cleanup fail
        finally:
            # No cleanup needed - subprocess will die naturally and take children with it
            pass
