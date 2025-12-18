"""
STEP 5: HTML REDUCTION
======================
Extract relevant sections containing contact data (names, titles, emails).

Input: Raw HTML content
Output: Reduced HTML string (only contact sections)
"""

import re
from bs4 import BeautifulSoup
from typing import Optional


class HTMLReducer:
    """Extract sections containing contact data (names, titles, emails)."""
    
    def __init__(self):
        # Keywords that indicate admin / leadership titles
        self.title_keywords = [
            'principal', 'superintendent', 'director', 'administrator', 'dean',
            'head of', 'assistant', 'vice', 'associate', 'manager', 'coordinator',
            'specialist', 'counselor', 'teacher', 'coach', 'president', 'ceo',
            'cfo', 'controller', 'secretary', 'treasurer', 'chair', 'chairman'
        ]
        
        # Keywords that indicate contact/staff sections
        self.contact_section_keywords = [
            'staff', 'faculty', 'team', 'leadership', 'administration',
            'directory', 'contact', 'people', 'member', 'employee'
        ]
    
    def reduce_html(self, html: str) -> str:
        """
        Extract sections containing contact data (names, titles, emails).
        Focus on sections where names, titles, and emails appear together.
        
        Args:
            html: Raw HTML content
            
        Returns:
            Reduced HTML string containing only contact sections
        """
        if not html:
            return ""
        
        # Define patterns early
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        # Name pattern: 2-5 capitalized words (more permissive to catch all names)
        name_pattern = re.compile(r'\b([A-Z][a-z]+(?: [A-Z][a-z]+){1,4})\b')
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract emails from script tags BEFORE removing them
        emails_from_scripts = set()
        # Also extract structured data (JSON-LD, microdata) from scripts
        structured_data_contacts = []
        for script in soup.find_all('script'):
            script_text = script.string or ''
            if script_text:
                # Extract emails
                script_emails = email_pattern.findall(script_text)
                emails_from_scripts.update(script_emails)
                
                # Extract JSON-LD structured data (often contains contact info)
                if 'application/ld+json' in script.get('type', '').lower() or 'ld+json' in script.get('type', '').lower():
                    try:
                        import json
                        data = json.loads(script_text)
                        # Look for Person or Organization entities
                        if isinstance(data, dict):
                            if data.get('@type') in ['Person', 'Organization']:
                                structured_data_contacts.append(data)
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and item.get('@type') in ['Person', 'Organization']:
                                    structured_data_contacts.append(item)
                    except:
                        pass
        
        # Remove obvious token-bloat elements
        for tag in soup(['script', 'style', 'noscript', 'svg', 'iframe', 'canvas', 'link']):
            tag.decompose()
        
        # Remove comments
        from bs4 import Comment
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()
        
        # EXTRACTION APPROACH: Find sections that contain contact data
        # Look for common patterns where names, titles, and emails appear together
        
        contact_sections = []
        
        # Strategy 1: Look for list items (<li>) that contain contact info
        # Staff directories often use <li> for each person
        # Also check nested structures (divs within li)
        for li in soup.find_all('li'):
            text = li.get_text(" ", strip=True)
            if len(text) < 15:  # Lower threshold
                continue
            
            # Check if this list item has contact data
            has_email = bool(email_pattern.search(text))
            has_name = bool(name_pattern.search(text))
            has_title = any(kw in text.lower() for kw in self.title_keywords)
            
            # More permissive: if it has name AND (title OR email), it's a contact
            # Or if it has email, it's likely a contact
            if (has_name and (has_title or has_email)) or has_email:
                # Include parent if it's a small container (better context)
                parent = li.parent
                if parent and parent.name in ['ul', 'ol'] and len(parent.get_text(strip=True)) < 5000:
                    contact_sections.append(str(parent))
                else:
                    contact_sections.append(str(li))
        
        # Strategy 2: Look for table rows (<tr>) that contain contact info
        # Staff directories often use tables
        # Also check table cells (<td>) for nested contact info
        for tr in soup.find_all('tr'):
            text = tr.get_text(" ", strip=True)
            if len(text) < 15:
                continue
            
            has_email = bool(email_pattern.search(text))
            has_name = bool(name_pattern.search(text))
            has_title = any(kw in text.lower() for kw in self.title_keywords)
            
            # More permissive: name + (title OR email), or just email
            if (has_name and (has_title or has_email)) or has_email:
                # Include parent table if it's small (better context)
                parent = tr.parent
                if parent and parent.name == 'table' and len(parent.get_text(strip=True)) < 10000:
                    contact_sections.append(str(parent))
                else:
                    contact_sections.append(str(tr))
        
        # Strategy 2b: Look for table cells with contact info (nested structures)
        for td in soup.find_all('td'):
            text = td.get_text(" ", strip=True)
            if len(text) < 20 or len(text) > 500:
                continue
            
            has_email = bool(email_pattern.search(text))
            has_name = bool(name_pattern.search(text))
            has_title = any(kw in text.lower() for kw in self.title_keywords)
            
            if (has_name and (has_title or has_email)) or has_email:
                # Get parent row for context
                parent_row = td.find_parent('tr')
                if parent_row:
                    contact_sections.append(str(parent_row))
                else:
                    contact_sections.append(str(td))
        
        # Strategy 3: Look for divs/sections with contact-related class names or IDs
        # Also check for microdata (itemscope, itemtype="Person")
        contact_indicators = self.contact_section_keywords + ['person', 'card', 'profile', 'bio']
        for div in soup.find_all(['div', 'section', 'article']):
            # Check class and id attributes
            classes = ' '.join(div.get('class', [])).lower()
            div_id = div.get('id', '').lower()
            attrs_text = f"{classes} {div_id}"
            
            # Check for microdata attributes
            has_microdata = div.get('itemscope') or 'person' in div.get('itemtype', '').lower()
            
            # Check if div has contact-related class/id
            has_contact_class = any(indicator in attrs_text for indicator in contact_indicators)
            
            if has_contact_class or has_microdata:
                text = div.get_text(" ", strip=True)
                if len(text) > 50:  # Must have substantial content
                    has_email = bool(email_pattern.search(text))
                    has_name = bool(name_pattern.search(text))
                    has_title = any(kw in text.lower() for kw in self.title_keywords)
                    
                    contact_score = sum([has_email, has_name, has_title])
                    if contact_score >= 1:  # Lower threshold for divs with contact classes
                        contact_sections.append(str(div))
        
        # Strategy 4: Look for paragraphs or divs that contain name + title together
        # This catches cases where contact info is in paragraphs
        for element in soup.find_all(['p', 'div']):
            text = element.get_text(" ", strip=True)
            if len(text) < 20 or len(text) > 1000:  # More permissive size range
                continue
            
            # Check if element has both name and title (strong indicator of contact)
            has_name = bool(name_pattern.search(text))
            has_title = any(kw in text.lower() for kw in self.title_keywords)
            has_email = bool(email_pattern.search(text))
            
            # More permissive: name + (title OR email), or just email, or just name+title
            if (has_name and (has_title or has_email)) or (has_email and has_title):
                contact_sections.append(str(element))
        
        # Strategy 5: Look for mailto links and extract their parent containers
        # Emails are strong indicators of contact sections
        for link in soup.find_all('a', href=re.compile(r'mailto:', re.I)):
            # Get parent element (likely contains name and title)
            parent = link.parent
            if parent:
                # Go up to grandparent if parent is too small
                if len(parent.get_text(strip=True)) < 20:
                    parent = parent.parent
                
                if parent and len(parent.get_text(strip=True)) > 15:
                    parent_html = str(parent)
                    contact_sections.append(parent_html)
        
        # Strategy 6: Look for elements with data-email or similar attributes
        for element in soup.find_all(attrs={'data-email': True}):
            parent = element.parent
            if parent and len(parent.get_text(strip=True)) > 15:
                contact_sections.append(str(parent))
        
        # Strategy 7: Look for spans/divs that contain email addresses
        # Sometimes emails are in spans or small divs
        for element in soup.find_all(['span', 'div']):
            text = element.get_text(" ", strip=True)
            if email_pattern.search(text) and len(text) < 200:  # Short elements with emails
                # Get parent to include name/title context
                parent = element.parent
                if parent and len(parent.get_text(strip=True)) > 20:
                    contact_sections.append(str(parent))
                else:
                    contact_sections.append(str(element))
        
        # Strategy 8: Extract entire sections that contain contact indicators
        # BUT be selective - only extract if it's a reasonable size and has strong indicators
        for container in soup.find_all(['ul', 'ol', 'table', 'div', 'section', 'article']):
            text = container.get_text(" ", strip=True)
            if len(text) < 30 or len(text) > 50000:  # Skip very small or very large
                continue
            
            # Check if container has contact indicators
            has_email = bool(email_pattern.search(text))
            has_name = bool(name_pattern.search(text))
            has_title = any(kw in text.lower() for kw in self.title_keywords)
            
            # More selective: require name AND (title OR email) for larger blocks
            # OR just email for smaller blocks
            if len(text) < 1000:
                # Small blocks: extract if it has email OR (name and title)
                if has_email or (has_name and has_title):
                    container_html = str(container)
                    contact_sections.append(container_html)
            else:
                # Larger blocks: require stronger indicators (name AND email/title)
                if (has_name and has_email) or (has_name and has_title):
                    container_html = str(container)
                    contact_sections.append(container_html)
        
        # Remove duplicates and nested blocks
        # Strategy: Keep only the smallest block if one contains another
        unique_sections = []
        
        # Sort by size (smallest first) to prioritize specific blocks
        contact_sections.sort(key=len)
        
        seen_hashes = set()
        for section in contact_sections:
            section_hash = hash(section[:500])
            if section_hash in seen_hashes:
                continue
            
            # Check if this section is contained in any existing section
            is_contained = False
            for existing in unique_sections:
                if section in existing or existing in section:
                    # If existing is larger, replace it with this smaller one
                    if len(section) < len(existing):
                        unique_sections.remove(existing)
                        unique_sections.append(section)
                        seen_hashes.add(section_hash)
                    is_contained = True
                    break
            
            if not is_contained:
                unique_sections.append(section)
                seen_hashes.add(section_hash)
        
        # Limit total size - if we have too much, prioritize smaller blocks
        MAX_TOTAL_SIZE = 100000  # Max 100k chars total (reduced from 200k for cost optimization)
        if sum(len(s) for s in unique_sections) > MAX_TOTAL_SIZE:
            # Sort by size and take smallest blocks first
            unique_sections.sort(key=len)
            total_size = 0
            selected_sections = []
            for section in unique_sections:
                if total_size + len(section) <= MAX_TOTAL_SIZE:
                    selected_sections.append(section)
                    total_size += len(section)
                else:
                    break
            unique_sections = selected_sections
        
        # Combine all contact sections
        if unique_sections:
            result_html = "\n".join(unique_sections)
            # Compact whitespace
            result_html = re.sub(r'>\s+<', '><', result_html)
        else:
            # Fallback: if no specific sections found, return cleaned HTML
            # (better than losing data)
            result_html = str(soup)
            result_html = re.sub(r'>\s+<', '><', result_html)
        
        # If we extracted emails from scripts, prepend them
        if emails_from_scripts:
            emails_comment = f"<!-- EXTRACTED_EMAILS_FROM_SCRIPTS: {', '.join(sorted(emails_from_scripts))} -->\n"
            result_html = emails_comment + result_html
        
        # If we found structured data contacts, add them as HTML comments
        if structured_data_contacts:
            import json
            structured_comment = f"<!-- STRUCTURED_DATA_CONTACTS: {json.dumps(structured_data_contacts)} -->\n"
            result_html = structured_comment + result_html
        
        return result_html


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract contact sections from HTML')
    parser.add_argument('--input', required=True, help='Input HTML file')
    parser.add_argument('--output', default='reduced.html', help='Output HTML file')
    args = parser.parse_args()
    
    with open(args.input, 'r', encoding='utf-8') as f:
        html = f.read()
    
    reducer = HTMLReducer()
    reduced = reducer.reduce_html(html)
    
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(reduced)
    
    print(f"Reduced HTML: {len(html)} → {len(reduced)} chars ({len(reduced)/max(1,len(html))*100:.1f}%)")
