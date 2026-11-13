import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from .contact import Contact
from .node_id import distance

ALPHA = 3  # how many peers we ask at the same time

# One query returns None (no answer) or (value_found, value, contacts)
QueryResult = Optional[Tuple[bool, Any, List[Contact]]]
Query = Callable[[Contact], Awaitable[QueryResult]]


async def _lookup(node: Any, target: int, query: Query,
                  alpha: int, k: int) -> Tuple[bool, Any, List[Contact]]:
    
    if alpha < 1:
        raise ValueError("alpha must be at least 1")

    # The shortlist: everyone we know about for this search.
    shortlist: Dict[int, Contact] = {}
    for c in node.routing_table.find_closest(target, k):
        shortlist[c.node_id] = c
    queried: Set[int] = set()
    failed: Set[int] = set()

    def closest_alive() -> List[Contact]:
        alive = [c for c in shortlist.values() if c.node_id not in failed]
        alive.sort(key=lambda c: distance(c.node_id, target))
        return alive[:k]

    while True:
        # Ask up to alpha of the k closest peers we haven't asked yet.
        batch = [c for c in closest_alive() if c.node_id not in queried][:alpha]
        if not batch:
            break  # the k closest have all been asked: we are done
        for c in batch:
            queried.add(c.node_id)

        answers = await asyncio.gather(*(query(c) for c in batch))

        for contact, answer in zip(batch, answers):
            if answer is None:
                failed.add(contact.node_id)  # silent peer: drop it from this search
                continue
            found, value, contacts = answer
            if found:
                return True, value, closest_alive()
            for new in contacts:
                if new.node_id != node.node_id and new.node_id not in shortlist:
                    shortlist[new.node_id] = new

    return False, None, closest_alive()


async def iterative_find_node(node: Any, target: int, alpha: int = ALPHA,
                              k: Optional[int] = None) -> List[Contact]:
    

    async def query(contact: Contact) -> QueryResult:
        contacts = await node.find_node(contact, target)
        if contacts is None:
            return None
        return (False, None, contacts)

    _, _, closest = await _lookup(node, target, query, alpha,
                                  node.k if k is None else k)
    return closest


async def iterative_find_value(node: Any, key: int, alpha: int = ALPHA,
                               k: Optional[int] = None) -> Tuple[bool, Any, List[Contact]]:
    

    async def query(contact: Contact) -> QueryResult:
        return await node.find_value(contact, key)

    return await _lookup(node, key, query, alpha,
                         node.k if k is None else k)