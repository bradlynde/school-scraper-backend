"""
STEP 7: LLM PARSING
===================
Send HTML chunks to LLM and extract contacts in CSV format.

Input: HTML chunk from Step 6
Output: CSV text with contacts
"""

from openai import OpenAI
import time
from typing import List, Dict
import re


# Step 7: Extract ALL contacts (NO filtering - that happens in Step 11)
CONTACT_EXTRACTION_PROMPT = """
You extract ALL PEOPLE from raw HTML. Do not hallucinate or invent data.

INPUT:
- Full HTML from a single web page or a contiguous chunk of HTML.
- The page may contain people in cards, tables, lists, or text blocks.
- Ignore organization names, departments, and generic contact emails.

OUTPUT:
Return ONLY CSV text (no backticks, no explanations) in this exact format:

First Name,Last Name,Title,Email,Phone

GENERAL RULES:
- Extract only real people, never departments, committees, buildings, teams, or offices.
- A valid contact MUST have at least a first and last name.
- Keep each person's name, title, email, and phone correctly matched.
- If a field is missing, leave it blank but keep the comma.
- If no valid contacts are found, return ONLY the header.

HEADER (always the first line):
First Name,Last Name,Title,Email,Phone

STEP-BY-STEP:
1. Identify all blocks that look like people entries (name + title, often near an email).
2. For each candidate person:
   - Extract full name (first + last). Remove prefixes like Dr., Mr., Mrs., Ms., Rev., Fr., Sr.
   - Extract the associated title / role (extract whatever title is shown, even if it's "Teacher", "Coach", "Board Member", etc.)
   - Extract the nearest email (usually mailto:). Skip generic inboxes (info@, office@, contact@, admissions@, marketing@, communications@).
   - Extract phone only if clearly linked to that person; otherwise leave blank.

NAME RULES:
- Valid examples:
  - "Dr. Terry Rodgers" → First Name: Terry, Last Name: Rodgers
  - "Mary Jane Watson" → First Name: Mary Jane, Last Name: Watson
  - "Kent A. Means" → First Name: Kent A., Last Name: Means
- If you cannot find a clear first + last name, skip that contact entirely.

TITLE EXTRACTION:
- Extract whatever title/role is shown for the person
- Include ALL titles: teachers, coaches, board members, administrators, etc.
- Do NOT filter by title in this step - extract everyone
- If someone has multiple titles, extract the primary one (or first one listed)

OUTPUT FORMAT (STRICT):
- First line: exactly
  First Name,Last Name,Title,Email,Phone
- Each subsequent line: one contact, 5 comma-separated fields.
- Use quotes only if a field contains a comma.
- No markdown, no commentary, no extra text.

IMPORTANT: Extract ALL people with names and titles. Do NOT filter by role - that filtering happens in a later step.
"""


class LLMParser:
    """Send HTML chunks to LLM and extract contacts."""
    
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        """
        Initialize LLM parser
        
        Args:
            api_key: OpenAI API key
            model: Model to use (default: gpt-4o-mini)
        """
        self.api_key = api_key
        self.model = model
        self.client = OpenAI(api_key=api_key, timeout=15.0)  # 15 second timeout for all requests
    
    def parse_with_llm(self, html_chunk: str, school_name: str, url: str, max_retries: int = 1) -> str:
        """
        Send HTML chunk to LLM and get CSV response
        
        Args:
            html_chunk: HTML chunk to parse
            school_name: School name (metadata)
            url: Page URL (metadata)
            max_retries: Maximum retry attempts
            
        Returns:
            CSV text from LLM (or empty string on error)
        """
        import signal
        
        # Timeout handler
        class TimeoutError(Exception):
            pass
        
        def timeout_handler(signum, frame):
            raise TimeoutError("LLM request timed out")
        
        for attempt in range(max_retries):
            try:
                # Build user message with metadata and HTML chunk only
                # The full prompt is in the system message (sent once per session, not per chunk)
                user_message = f"""SCHOOL NAME: {school_name}
PAGE URL: {url}

HTML CONTENT:
{html_chunk}"""
                
                # Estimate tokens for max_tokens calculation
                estimated_input_tokens = len(html_chunk) // 4
                
                # Safety check: if chunk is still too large, it should have been split earlier
                # But as a final safeguard, we'll note it (shouldn't happen with improved chunking)
                if len(html_chunk) > 100000:
                    print(f"      WARNING: HTML chunk still too large ({len(html_chunk):,} chars) - this should have been split earlier!")
                    # Don't truncate - this indicates a bug in chunking logic
                    # Process it anyway but log the issue
                
                # Set max_tokens based on input size
                if estimated_input_tokens > 20000:
                    max_tokens = 32000
                elif estimated_input_tokens > 10000:
                    max_tokens = 16000
                else:
                    max_tokens = 8000
                
                # Set timeout: 15 seconds per request
                # Use signal-based timeout as fallback if client timeout doesn't work
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(15)  # 15 second timeout
                
                try:
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {"role": "system", "content": CONTACT_EXTRACTION_PROMPT},
                            {"role": "user", "content": user_message}
                        ],
                        temperature=0.0,
                        max_tokens=max_tokens
                    )
                finally:
                    signal.alarm(0)  # Cancel timeout
                
                # Extract response text
                response_text = response.choices[0].message.content.strip()
                
                return response_text
                
            except TimeoutError:
                print(f"      ⚠️  Request timed out after 15s (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1.0)
                    continue
                return ""
            except Exception as e:
                error_str = str(e)
                
                # Check if it's a timeout error
                is_timeout = 'timeout' in error_str.lower() or 'timed out' in error_str.lower()
                if is_timeout:
                    print(f"      ⚠️  Request timed out (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(1.0)
                        continue
                    return ""
                
                # Check if it's a rate limit error (429)
                is_rate_limit = '429' in error_str or 'rate_limit' in error_str.lower() or 'rate limit' in error_str.lower()
                
                if is_rate_limit:
                    # Try to extract wait time from error message
                    wait_seconds = 1.0  # Default wait
                    # Look for "Please try again in Xms" or "Please try again in Xs"
                    wait_match = re.search(r'Please try again in (\d+)(ms|s)', error_str, re.IGNORECASE)
                    if wait_match:
                        wait_value = int(wait_match.group(1))
                        wait_unit = wait_match.group(2).lower()
                        if wait_unit == 'ms':
                            wait_seconds = (wait_value / 1000.0) + 0.5  # Add 0.5s buffer
                        else:
                            wait_seconds = wait_value + 0.5  # Add 0.5s buffer
                    
                    # For rate limits, wait longer and retry
                    if attempt < max_retries - 1:
                        print(f"      ⚠️  Rate limit hit (attempt {attempt + 1}/{max_retries}), waiting {wait_seconds:.1f}s...")
                        time.sleep(wait_seconds)
                        continue
                    else:
                        print(f"      ❌ Rate limit exceeded after {max_retries} attempts. Skipping this chunk.")
                        return ""
                else:
                    # For other errors, use exponential backoff
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        print(f"      WARNING: LLM error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    print(f"      ERROR: LLM error: {e}")
                    return ""
        
        return ""
