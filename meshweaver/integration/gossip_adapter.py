import asyncio
import inspect
import json
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from meshweaver.dht.messages import MAX_MESSAGE_SIZE, id_to_hex
from meshweaver.dht.transport import Address

Neighbor = Tuple[str, str, int]                      # (gossip id, host, port)
Handler = Callable[[Dict[str, Any], Address], Any]   # may be a normal or an async function


def gossip_id(node_id: int) -> str:
    """The DHT ID as 40 hex characters. This is the ID gossip uses for the same node."""
    return id_to_hex(node_id)


class GossipTransport:
    """
    Gives gossip the two things its engine expects from a transport:
        register_handler(msg_type, handler)    handler(msg: dict, addr)
        async send(addr, msg: dict)
    but runs on a shared Dispatcher socket instead of a socket of its own.
    The dispatcher sorts incoming messages by their "type" field, so gossip only
    sees its own message types, and the DHT keeps working on the same port.
    """

    def __init__(self, dispatcher):
        self._dispatcher = dispatcher
        self._types: List[str] = []
        self._tasks: Set["asyncio.Future[Any]"] = set()

    # ---- what the gossip engine calls ----
    def register_handler(self, msg_type: str, handler: Handler) -> None:
        def route(data: bytes, addr: Address) -> None:
            try:
                msg = json.loads(data)
            except (ValueError, RecursionError):
                return
            if not isinstance(msg, dict):
                return
            try:
                result = handler(msg, addr)
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    self._tasks.add(task)
                    task.add_done_callback(self._finished)
            except Exception:
                pass  # a broken handler must never stop the socket

        self._dispatcher.add_route(msg_type, route)  # refuses the DHT's own types
        self._types.append(msg_type)

    async def send(self, addr: Address, msg: Dict[str, Any]) -> None:
        data = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        if len(data) > MAX_MESSAGE_SIZE:
            raise ValueError("gossip message too large for one datagram")
        self._dispatcher.send(data, (addr[0], addr[1]))

    # ---- our own housekeeping ----
    async def stop(self) -> None:
        """Release our routes and cancel running handlers. The shared socket stays open."""
        for msg_type in self._types:
            self._dispatcher.remove_route(msg_type)
        self._types.clear()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _finished(self, task: "asyncio.Future[Any]") -> None:
        self._tasks.discard(task)
        if not task.cancelled():
            task.exception()  # mark as seen so Python does not warn about it


def neighbor_provider(dht: Any, limit: Optional[int] = None) -> Callable[[], List[Neighbor]]:
    """
    The function to pass to the gossip engine as get_neighbors.
    `dht` is a DHTNode. Each call returns (gossip_id, host, port) for the peers
    the DHT currently knows, nearest first. Returns [] while the node is stopped.
    """
    def provide() -> List[Neighbor]:
        if not dht.is_running:
            return []
        return [(gossip_id(c.node_id), c.host, c.port) for c in dht.get_neighbors(limit)]

    return provide


def expire_entry(engine: Any, peer_id: str, now: Optional[float] = None) -> bool:
    """
    Make the heartbeat monitor treat a peer as silent for too long, so that it
    declares the peer lost on its next scan and calls the on_peer_lost callbacks.
    Returns False if gossip has no entry for that peer.
    """
    entry = engine.table.get(peer_id)
    if entry is None:
        return False
    now = time.monotonic() if now is None else now
    entry.received_at = now - engine.heartbeat.timeout - 1.0
    return True


def link_departures(dht: Any, engine: Any) -> Callable[[], None]:
    """
    When the DHT removes a dead peer, tell gossip too. Returns a function that
    switches the link off again.
    """
    def on_left(contact: Any) -> None:
        expire_entry(engine, gossip_id(contact.node_id))

    return dht.on_peer_left(on_left)