"""
STEP 11: CONTACT SPLITTER
==========================
Split contacts from Step 10 into two groups:
- Contacts with emails
- Contacts without emails (to be enriched by Step 12)

Input: List of Contact objects from Step 10
Output: Two lists - contacts_with_emails, contacts_without_emails
"""

from typing import List, Tuple
from assets.shared.models import Contact


class ContactSplitter:
    """
    Split contacts into those with emails and those without.
    """
    
    def __init__(self):
        self.stats = {
            'total_contacts': 0,
            'contacts_with_emails': 0,
            'contacts_without_emails': 0
        }
    
    def split_contacts(self, contacts: List[Contact]) -> Tuple[List[Contact], List[Contact]]:
        """
        Split contacts into two groups based on email presence.
        
        Args:
            contacts: List of Contact objects from Step 10
            
        Returns:
            Tuple of (contacts_with_emails, contacts_without_emails)
        """
        print("\n" + "="*70)
        print("STEP 11: SPLITTING CONTACTS")
        print("="*70)
        
        self.stats['total_contacts'] = len(contacts)
        
        contacts_with_emails = []
        contacts_without_emails = []
        
        for contact in contacts:
            if contact.has_email():
                contacts_with_emails.append(contact)
            else:
                contacts_without_emails.append(contact)
        
        self.stats['contacts_with_emails'] = len(contacts_with_emails)
        self.stats['contacts_without_emails'] = len(contacts_without_emails)
        
        print(f"  Total contacts: {self.stats['total_contacts']}")
        print(f"  Contacts with emails: {self.stats['contacts_with_emails']}")
        print(f"  Contacts without emails: {self.stats['contacts_without_emails']}")
        print("="*70)
        
        return contacts_with_emails, contacts_without_emails


if __name__ == "__main__":
    # Test the splitter
    from assets.shared.models import Contact
    
    test_contacts = [
        Contact(first_name="John", last_name="Doe", email="john@example.com", school_name="Test School"),
        Contact(first_name="Jane", last_name="Smith", email=None, school_name="Test School"),
        Contact(first_name="Bob", last_name="Jones", email="bob@example.com", school_name="Test School"),
    ]
    
    splitter = ContactSplitter()
    with_emails, without_emails = splitter.split_contacts(test_contacts)
    
    print(f"\nWith emails: {len(with_emails)}")
    print(f"Without emails: {len(without_emails)}")

