import asyncio
from typing import Any, Optional, Tuple

from .lookup import iterative_find_node, iterative_find_value
from .node_id import distance
from .storage import MISSING


async def put_value(node: Any, key: int, value: Any,
                    ttl: Optional[float] = None) -> int:
    """
    Store a value on the k peers closest to `key` (and on ourselves if we are
    one of the closest). Returns how many nodes accepted it.
    """
    closest = await iterative_find_node(node, key)
    results = await asyncio.gather(*(node.store(c, key, value, ttl) for c in closest))
    count = sum(1 for ok in results if ok)

    # We are never in our own lookup results, so decide separately whether we
    # belong among the k closest nodes.
    keep_local = (len(closest) < node.k or
                  distance(node.node_id, key) < distance(closest[-1].node_id, key))
    if keep_local and node.storage.set(key, value, ttl):
        count += 1
    return count


async def get_value(node: Any, key: int) -> Tuple[bool, Any]:
    """Look in our own storage first, then search the mesh. Returns (found, value)."""
    local = node.storage.get(key, MISSING)
    if local is not MISSING:
        return True, local
    found, value, _ = await iterative_find_value(node, key)
    return found, value