from collections import OrderedDict
from typing import List, Optional, Tuple

from .contact import Contact

K = 20 


class KBucket:

    def __init__(self, range_min: int, range_max: int, k: int = K):
        self.range_min = range_min
        self.range_max = range_max  # exclusive
        self.k = k
        self._contacts: "OrderedDict[int, Contact]" = OrderedDict()

    def __len__(self) -> int:
        return len(self._contacts)

    def __contains__(self, node_id: int) -> bool:
        return node_id in self._contacts

    def is_full(self) -> bool:
        return len(self._contacts) >= self.k

    def covers(self, node_id: int) -> bool:
        
        return self.range_min <= node_id < self.range_max

    def get_contacts(self) -> List[Contact]:

        return list(self._contacts.values())

    def get(self, node_id: int) -> Optional[Contact]:
        return self._contacts.get(node_id)

    def least_recently_seen(self) -> Optional[Contact]:
       
        if not self._contacts:
            return None
        return next(iter(self._contacts.values()))

    def add_contact(self, contact: Contact) -> bool:
        
        if contact.node_id in self._contacts:
           
            self._contacts[contact.node_id] = contact
            self._contacts.move_to_end(contact.node_id)
            return True
        if len(self._contacts) < self.k:
            self._contacts[contact.node_id] = contact
            return True
        return False

    def remove(self, node_id: int) -> bool:
        
        return self._contacts.pop(node_id, None) is not None

    def split(self) -> Tuple["KBucket", "KBucket"]:
        
        mid = (self.range_min + self.range_max) // 2
        left = KBucket(self.range_min, mid, self.k)
        right = KBucket(mid, self.range_max, self.k)
        for c in self.get_contacts():  # keeps the original order
            (left if left.covers(c.node_id) else right).add_contact(c)
        return left, right