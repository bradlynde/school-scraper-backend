# Lead Generation Optimization Recommendations

## Current Performance
- **Raw contacts found**: 2,170
- **After deduplication**: 716 unique contacts
- **After enrichment**: 432 final contacts with emails
- **Retention rate**: ~20% (432/2,170)

## ✅ IMPLEMENTED OPTIMIZATIONS

### 1. Expanded School Discovery (Step 1) ✅
- **Changed**: Search terms per county: 5 → 14 (now 20 with additions)
- **Added search terms**:
  - "private Christian schools"
  - "Christian elementary school"
  - "Christian high school"
  - "Christian middle school"
  - "Christian preparatory school"
  - "Catholic high school"
- **Expected impact**: +50-100 more schools = +100-200 more contacts
- **Cost**: +$10-15 for Places API

### 2. Increased Pages Per School (Step 3) ✅
- **Changed**: `max_pages_per_school`: 3 → 5
- **Expected impact**: +150-250 more contacts
- **Cost**: +$1-2 for OpenAI

### 3. Enhanced Page Discovery (Step 3) ✅
- **Added keywords**: 'about-us', 'about us', 'administrative', 'administrators', 'executive', 'management', 'directors'
- **Added fallback discovery**: Tries common paths (/staff, /faculty, /team, /leadership, etc.) if not enough pages found
- **Expected impact**: +20-40 more contacts

### 4. Expanded Title Filtering (Step 10) ✅
- **Added to KEEP list**:
  - Academic Dean, Dean of Academics, Dean of Students (if administrative)
  - Department Head, Division Director (if operations-focused)
  - Program Director (if operations/administrative focused)
- **Expected impact**: +50-100 more contacts
- **Cost**: Minimal (same LLM calls)

**Total Expected Impact**: 432 → **632-782 contacts** (+46-81%)

## Optimization Opportunities

### 1. Expand School Discovery (Step 1) ⭐ HIGH IMPACT

**Current State:**
- Pipeline limits to 2-5 search terms per county (when no API limit)
- 14 search terms are defined but not all used
- Estimated impact: **+30-50% more schools discovered**

**Recommendation:**
- Increase `max_search_terms_per_county` from 5 to 14 (use all available terms)
- Add additional search term variations:
  - "private Christian schools"
  - "Christian elementary schools"
  - "Christian high schools"
  - "Christian middle schools"
  - "Christian preparatory schools"

**Cost Impact:** 
- Additional ~$10-15 for Places API (9 more terms × 254 counties × ~$0.017)
- **Estimated gain: 50-100 more schools = 100-200 more contacts**

**Implementation:**
```python
# In Pipeline.py, line 386:
max_search_terms_per_county = 14  # Use all available search terms
```

---

### 2. Increase Pages Per School (Step 3) ⭐ HIGH IMPACT

**Current State:**
- Limited to 3 pages per school
- Many schools have 5-10+ staff/admin pages
- Estimated impact: **+40-60% more contacts per school**

**Recommendation:**
- Increase `max_pages_per_school` from 3 to 5-7
- This will find more staff directories, leadership pages, etc.

**Cost Impact:**
- Additional ~$1-2 for OpenAI (more pages to parse)
- **Estimated gain: 150-250 more contacts**

**Implementation:**
```python
# In Pipeline.py, line 89:
max_pages_per_school: int = 5,  # Increased from 3
```

---

### 3. Expand Title Filtering (Step 10) ⭐ MEDIUM-HIGH IMPACT

**Current State:**
- Very strict filtering - only core administrative roles
- Excludes many decision-makers like:
  - Academic Deans (some are decision-makers)
  - Department Heads
  - Program Directors
  - Accreditation/Compliance Coordinators

**Recommendation:**
- Expand "KEEP" list to include:
  - Academic Dean, Dean of Academics, Dean of Students (if clearly administrative)
  - Department Head, Division Head
  - Program Director (if operations-focused, not instructional)
  - Accreditation Coordinator, Compliance Coordinator
  - Operations Coordinator (if clearly administrative)

**Cost Impact:**
- Minimal - same LLM calls, just different filtering logic
- **Estimated gain: 50-100 more contacts**

**Implementation:**
Update `step10.py` TITLE_FILTERING_PROMPT to include these roles in KEEP list.

---

### 4. Improve Contact Extraction (Step 5) ⭐ MEDIUM IMPACT

**Current State:**
- LLM extraction may miss contacts in complex HTML structures
- Some contacts may be in tables, lists, or nested divs

**Recommendation:**
- Enhance HTML preprocessing to better extract structured data
- Add fallback extraction for common patterns (name + title + email)
- Improve chunking strategy for very large pages

**Cost Impact:**
- Minimal - same token usage, better extraction
- **Estimated gain: 30-50 more contacts**

---

### 5. Add Alternative Discovery Methods ⭐ LOW-MEDIUM IMPACT

**Current State:**
- Relies solely on Google Places API

**Recommendation:**
- Add school directory websites (e.g., Private School Review, GreatSchools)
- Add state education department listings
- Add denomination-specific school directories

**Cost Impact:**
- Free (web scraping)
- **Estimated gain: 20-40 more schools = 40-80 more contacts**

---

## Additional Optimization Opportunities

### Medium Impact (Future Enhancements)

#### 1. Improve Contact Extraction Patterns (Step 5)
**Current**: Basic HTML reduction and pattern matching
**Enhancement**: 
- Better handling of nested HTML structures (divs within divs)
- Improved regex for name/title/email patterns
- Better detection of structured data (JSON-LD, microdata)
- Handle more table/list formats

**Expected gain**: +30-50 contacts
**Effort**: Medium (requires testing extraction patterns)

#### 2. Improve Early Exit Logic (Step 3)
**Current**: Exits after 3 pages + 1 high-value page
**Enhancement**: 
- With 5 pages limit, require 4 pages before early exit
- Or require 2 high-value pages before exit

**Expected gain**: +10-20 contacts
**Effort**: Low (simple logic change)

#### 3. Enhanced Deduplication (Step 11)
**Current**: Exact matching on name + school + source_url
**Enhancement**:
- Fuzzy matching for similar names (e.g., "John Smith" vs "J. Smith")
- Handle name variations (middle initials, nicknames)
- Better handling of duplicate contacts across different source URLs

**Expected gain**: +5-15 contacts (recover false duplicates)
**Effort**: Medium (requires fuzzy matching library)

#### 4. Alternative Discovery Sources
**Current**: Only Google Places API
**Enhancement**:
- State education department listings
- Denomination-specific school directories (e.g., Catholic school directories)
- Private school review sites
- Accreditation body listings

**Expected gain**: +20-40 schools = +40-80 contacts
**Effort**: High (requires new data sources and parsing)

#### 5. Improve HTML Reduction (Step 5)
**Current**: Extracts contact sections but may miss some
**Enhancement**:
- Better detection of staff directory structures
- Handle more CMS formats (WordPress, Squarespace, etc.)
- Extract from JavaScript-rendered content more effectively

**Expected gain**: +20-30 contacts
**Effort**: Medium (requires pattern analysis)

---

## Recommended Implementation Priority

### ✅ Phase 1: COMPLETED (High Impact, Low Risk)
1. ✅ Increase pages per school: 3 → 5
2. ✅ Expand search terms: 5 → 14 (now 20)
3. ✅ Expand title filtering to include Academic Deans and Department Heads
4. ✅ Add fallback page discovery
5. ✅ Enhance page discovery keywords

**Expected Result:** +200-350 contacts (432 → 632-782)

### Phase 2: Medium Effort (Medium Impact)
4. Improve contact extraction patterns
5. Add alternative discovery sources

**Expected Result:** +50-100 more contacts

---

## Cost-Benefit Analysis

### Current Run:
- Cost: $41.91
- Output: 432 contacts
- Cost per contact: $0.097

### With Phase 1 Optimizations:
- Additional cost: ~$12-17 (Places API + OpenAI)
- **Total cost: ~$54-59**
- **Expected output: 632-782 contacts**
- **Cost per contact: ~$0.085-0.093** (better efficiency!)

---

## Quality Assurance

All optimizations maintain strict filtering:
- ✅ Still excludes public schools (ISD, charter, etc.)
- ✅ Still requires Christian keywords
- ✅ Still excludes colleges, seminaries, standalone preschools
- ✅ Still filters to decision-maker roles only

---

## Implementation Notes

1. **Test incrementally**: Start with one optimization, measure results, then add more
2. **Monitor quality**: Check that new contacts are still high-quality decision-makers
3. **Track costs**: Monitor API usage to ensure costs stay within budget
4. **Review excluded contacts**: Periodically check what's being filtered out to ensure we're not losing good leads

