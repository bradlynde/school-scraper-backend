"""
Shared Data Models for Church Scraper Streaming Pipeline
========================================================
Data classes for passing leads through the pipeline without CSV intermediate steps.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class Church:
    """Church data model - output from Step 1"""
    place_id: str
    name: str
    address: str
    website: Optional[str] = None
    phone: Optional[str] = None
    types: Optional[str] = None
    business_status: Optional[str] = None
    county: str = ""
    state: str = "Texas"
    detected_state: Optional[str] = None
    detected_county: Optional[str] = None
    found_via: Optional[str] = None  # Which search term found it
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for CSV export"""
        return {
            'place_id': self.place_id,
            'name': self.name,
            'address': self.address,
            'website': self.website or '',
            'phone': self.phone or '',
            'types': self.types or '',
            'business_status': self.business_status or '',
            'county': self.county,
            'state': self.state,
            'detected_state': self.detected_state or '',
            'detected_county': self.detected_county or '',
            'found_via': self.found_via or ''
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Church':
        """Create Church from dictionary"""
        return cls(
            place_id=data.get('place_id', ''),
            name=data.get('name', ''),
            address=data.get('address', ''),
            website=data.get('website'),
            phone=data.get('phone'),
            types=data.get('types'),
            business_status=data.get('business_status'),
            county=data.get('county', ''),
            state=data.get('state', 'Texas'),
            detected_state=data.get('detected_state'),
            detected_county=data.get('detected_county'),
            found_via=data.get('found_via')
        )


@dataclass
class Page:
    """Page data model - output from Step 3"""
    url: str
    church_name: str
    church_place_id: str
    church_website: str
    priority_score: int = 0
    page_title: Optional[str] = None
    discovered_via: Optional[str] = None  # Which URL pattern found it
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'url': self.url,
            'church_name': self.church_name,
            'church_place_id': self.church_place_id,
            'church_website': self.church_website,
            'priority_score': self.priority_score,
            'page_title': self.page_title or '',
            'discovered_via': self.discovered_via or ''
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Page':
        """Create Page from dictionary"""
        return cls(
            url=data['url'],
            church_name=data.get('church_name', ''),
            church_place_id=data.get('church_place_id', ''),
            church_website=data.get('church_website', ''),
            priority_score=int(data.get('priority_score', 0)),
            page_title=data.get('page_title'),
            discovered_via=data.get('discovered_via')
        )


@dataclass
class PageContent:
    """Page content data model - output from Step 4"""
    url: str
    church_name: str
    html_content: str
    email_count: int = 0
    has_emails: bool = False
    collection_method: str = "requests"  # "requests" or "selenium"
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'url': self.url,
            'church_name': self.church_name,
            'html_content': self.html_content,
            'email_count': self.email_count,
            'has_emails': self.has_emails,
            'collection_method': self.collection_method,
            'error': self.error or ''
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PageContent':
        """Create PageContent from dictionary"""
        return cls(
            url=data['url'],
            church_name=data.get('church_name', ''),
            html_content=data.get('html_content', ''),
            email_count=int(data.get('email_count', 0)),
            has_emails=bool(data.get('has_emails', False)),
            collection_method=data.get('collection_method', 'requests'),
            error=data.get('error')
        )


@dataclass
class Contact:
    """Contact data model - output from Step 5"""
    first_name: str = ""
    last_name: str = ""
    title: str = ""
    email: Optional[str] = None
    phone: Optional[str] = None
    church_name: str = ""
    source_url: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for CSV export"""
        return {
            'first_name': self.first_name,
            'last_name': self.last_name,
            'title': self.title,
            'email': self.email or '',
            'phone': self.phone or '',
            'church_name': self.church_name,
            'source_url': self.source_url,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Contact':
        """Create Contact from dictionary"""
        return cls(
            first_name=data.get('first_name', ''),
            last_name=data.get('last_name', ''),
            title=data.get('title', ''),
            email=data.get('email'),
            phone=data.get('phone'),
            church_name=data.get('church_name', ''),
            source_url=data.get('source_url', '')
        )
    
    def has_email(self) -> bool:
        """Check if contact has an email"""
        return bool(self.email and self.email.strip())
    
    def full_name(self) -> str:
        """Get full name"""
        parts = [self.first_name, self.last_name]
        return " ".join([p for p in parts if p.strip()]).strip()
