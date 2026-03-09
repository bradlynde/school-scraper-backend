"""
STEP 6: HTML CHUNKING
=====================
Split large HTML into manageable chunks that fit within LLM token limits.

Input: Reduced HTML from Step 5
Output: List of HTML chunks
"""

import re
from typing import List


class HTMLChunker:
    """Split HTML into chunks of approximately max_chunk_size characters."""
    
    def chunk_html(self, html: str, max_chunk_size: int = 20000) -> List[str]:
        """
        Split HTML into chunks of approximately max_chunk_size characters
        Intelligently splits on block boundaries to avoid cutting contact cards in half
        Ensures contacts (name + email/title) stay together within chunks
        
        Args:
            html: HTML string to chunk
            max_chunk_size: Maximum characters per chunk (default: 20,000)
        
        Returns:
            List of HTML chunks
        """
        if not html or len(html) <= max_chunk_size:
            return [html]
        
        chunks = []
        
        # Priority order for split points (most likely to preserve contact cards)
        # </li> is often a contact card boundary
        # </tr> is often a table row with contact info
        # </div> with class/id containing staff/contact keywords
        block_delimiters = ['</li>', '</tr>', '</div>', '</section>', '</article>', '</td>']
        
        # Find all split points with context
        split_points = []
        for delimiter in block_delimiters:
            pattern = re.compile(re.escape(delimiter), re.IGNORECASE)
            for match in pattern.finditer(html):
                # Check context around delimiter for contact indicators
                context_start = max(0, match.start() - 200)
                context_end = min(len(html), match.end() + 200)
                context = html[context_start:context_end].lower()
        
                # Prefer splitting after contact-like structures
                # Look for patterns like name + email or title
                has_contact_pattern = (
                    '@' in context or  # Email present
                    re.search(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', context) or  # Name pattern
                    any(kw in context for kw in ['principal', 'director', 'administrator', 'email', 'phone'])
                )
                
                split_points.append((match.end(), delimiter, has_contact_pattern))
        
        # Sort by position
        split_points.sort(key=lambda x: x[0])
        
        # Build chunks with smart boundary detection
        current_chunk = ""
        last_split = 0
        target_size = max_chunk_size - 5000  # Leave 5k buffer to avoid splitting mid-contact
        
        for split_pos, delimiter, is_contact_boundary in split_points:
            segment = html[last_split:split_pos]
            
            # If adding segment would exceed target, check if we should split
            if len(current_chunk) + len(segment) > target_size and current_chunk:
                # If this is a contact boundary, it's safe to split here
                if is_contact_boundary:
                    chunks.append(current_chunk)
                    current_chunk = segment
                    last_split = split_pos
                # Otherwise, try to find next contact boundary
                elif len(current_chunk) < max_chunk_size * 0.8:  # Still room to grow
                    current_chunk += segment
                else:
                    # Force split to avoid exceeding max
                    chunks.append(current_chunk)
                    current_chunk = segment
                    last_split = split_pos
            else:
                current_chunk += segment
                last_split = split_pos
        
        # Add remaining content
        if current_chunk:
            remaining = html[last_split:]
            if len(current_chunk) + len(remaining) <= max_chunk_size:
                current_chunk += remaining
                chunks.append(current_chunk)
            else:
                # Split remaining intelligently
                if current_chunk:
                    chunks.append(current_chunk)
                # Chunk remaining content
                if len(remaining) > max_chunk_size:
                    # Recursively chunk remaining
                    remaining_chunks = self.chunk_html(remaining, max_chunk_size)
                    chunks.extend(remaining_chunks)
                else:
                    chunks.append(remaining)
        
        # If no good split points found, split at safe boundaries
        if not chunks:
            # Try to split at paragraph or line breaks first
            safe_splits = re.finditer(r'</p>|</br>|</h[1-6]>', html, re.IGNORECASE)
            split_positions = [m.end() for m in safe_splits]
            
            if split_positions:
                last_pos = 0
                for pos in split_positions:
                    if pos - last_pos > max_chunk_size:
                        chunks.append(html[last_pos:pos])
                        last_pos = pos
                if last_pos < len(html):
                    chunks.append(html[last_pos:])
            else:
                # Last resort: split at max_chunk_size but try to avoid mid-word
                for i in range(0, len(html), max_chunk_size):
                    chunk = html[i:i + max_chunk_size]
                    # Try to end at a space or tag boundary
                    if i + max_chunk_size < len(html):
                        # Look for safe break point near end
                        for j in range(len(chunk) - 1, max(0, len(chunk) - 100), -1):
                            if chunk[j] in ['>', '\n', ' ']:
                                chunk = html[i:i+j+1]
                                i = i + j + 1
                                break
                    chunks.append(chunk)
        
        # Ensure no chunk exceeds max (safety check)
        final_chunks = []
        for chunk in chunks:
            if len(chunk) > max_chunk_size:
                # Split oversized chunk
                for i in range(0, len(chunk), max_chunk_size):
                    final_chunks.append(chunk[i:i + max_chunk_size])
            else:
                final_chunks.append(chunk)
        
        return final_chunks if final_chunks else [html]
