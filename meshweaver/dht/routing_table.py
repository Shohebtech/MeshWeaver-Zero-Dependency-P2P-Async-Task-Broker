from typing import Iterable, List, Optional

from .contact import Contact
from .kbucket import KBucket, K
from .node_id import ID_BITS, distance

ID_SPACE = 2 ** ID_BITS


class RoutingTable:
    

    def __init__(self, own_id: int, k: int = K):
        self.own_id = own_id
        self.k = k
        self.buckets: List[KBucket] = [KBucket(0, ID_SPACE, k)]

    
    def _bucket_position(self, node_id: int) -> int:
        for i, bucket in enumerate(self.buckets):
            if bucket.covers(node_id):
                return i
        raise ValueError("node_id is outside the ID space")

    def bucket_for(self, node_id: int) -> KBucket:
        return self.buckets[self._bucket_position(node_id)]

    def __len__(self) -> int:
        return sum(len(b) for b in self.buckets)

    def __contains__(self, node_id: int) -> bool:
        return node_id in self.bucket_for(node_id)

    def get(self, node_id: int) -> Optional[Contact]:
        return self.bucket_for(node_id).get(node_id)

    def all_contacts(self) -> List[Contact]:
        return [c for b in self.buckets for c in b.get_contacts()]

    
    def add_contact(self, contact: Contact) -> bool:
        
        if contact.node_id == self.own_id:
            return False
        while True:
            pos = self._bucket_position(contact.node_id)
            bucket = self.buckets[pos]
            if bucket.add_contact(contact):
                return True
            can_split = bucket.covers(self.own_id) and (bucket.range_max - bucket.range_min) > 1
            if not can_split:
                return False
            left, right = bucket.split()
            self.buckets[pos:pos + 1] = [left, right]
           

    def remove_contact(self, node_id: int) -> bool:
        return self.bucket_for(node_id).remove(node_id)

    def get_eviction_candidate(self, contact: Contact) -> Optional[Contact]:
        
        bucket = self.bucket_for(contact.node_id)
        if bucket.is_full() and contact.node_id not in bucket:
            return bucket.least_recently_seen()
        return None

    
    def find_closest(self, target: int, n: int = K,
                     exclude: Optional[Iterable[int]] = None) -> List[Contact]:
        
        skip = set(exclude) if exclude else set()
        candidates = [c for c in self.all_contacts() if c.node_id not in skip]
        candidates.sort(key=lambda c: distance(c.node_id, target))
        return candidates[:n]