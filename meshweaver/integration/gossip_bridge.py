import inspect
import asyncio
import json
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple

from meshweaver.integration.gossip_adapter import GossipTransport

MAX_GOSSIP_BYTES = 60_000       # a UDP datagram limit, much larger than a DHT message


class BridgeTransport(GossipTransport):
    """The transport the gossip engine expects, running on the shared dispatcher."""

    mock = False
    implementation = "BridgeTransport (shares the DHT's UDP socket)"

    def __init__(self, dispatcher: Any):
        super().__init__(dispatcher)
        self.port = dispatcher.local_addr[1]      # the engine reads this after start()
        self.stats = {"sent": 0, "send_failed": 0}

    async def start(self, handler: Callable[[Dict[str, Any], Tuple[str, int]], Any]) -> None:
        """The engine gives us one handler. Gossip and heartbeat messages go to it."""
        self.register_handler("gossip", handler)
        self.register_handler("heartbeat", handler)

    async def send(self, addr: Tuple[str, int], msg: Dict[str, Any]) -> bool: #type: ignore
        out = dict(msg)
        body = out.get("body")
        # Signed messages look like {"body": ..., "sig": ...} with no "type". The
        # dispatcher sorts by "type", so copy it to the top level.
        if "type" not in out and isinstance(body, dict) and isinstance(body.get("type"), str):
            out["type"] = body["type"]
        try:
            data = json.dumps(out, separators=(",", ":")).encode("utf-8")
            if len(data) > MAX_GOSSIP_BYTES:
                raise ValueError("too large")
            self._dispatcher.send(data, (addr[0], addr[1]))
        except (TypeError, ValueError, OSError, RuntimeError):
            self.stats["send_failed"] += 1
            return False
        self.stats["sent"] += 1
        return True

    def info(self) -> Dict[str, Any]:
        host, port = self._dispatcher.local_addr
        return {"implementation": self.implementation, "mock": self.mock,
                "real_network_traffic": True, "protocol": "UDP (shared with the DHT)",
                "bind": f"{host}:{port}", "stats": dict(self.stats)}


class NeighborDirectory:
    """The peer directory the engine expects, backed by the DHT's neighbour list."""

    mock = False
    implementation = "NeighborDirectory (DHT routing table, Module 2)"

    def __init__(self, neighbors: Callable[[], List[Tuple[str, str, int]]]):
        self._neighbors = neighbors

    def get_peers(self) -> List[Tuple[str, int]]:
        try:
            return [(host, port) for _node_id, host, port in self._neighbors()]
        except Exception:
            return []

    def add_peer(self, addr: Tuple[str, int], node_id: Optional[str] = None) -> bool:
        return False        # gossip learns peers from unverified messages; the DHT decides alone

    def remove_peer(self, addr: Tuple[str, int]) -> None:
        return None

    def info(self) -> Dict[str, Any]:
        peers = [f"{h}:{p}" for h, p in self.get_peers()]
        return {"implementation": self.implementation, "mock": self.mock,
                "seeds": [], "learned": peers, "note": "Peers come from the Kademlia DHT."}


class GossipRouting:
    """
    Gives the task manager the two methods it needs, on top of the real engine:
      lowest_cpu_node(exclude, include_self)   returns an object with node_id/host/port
      on_peer_lost(callback)                   callback(node_id, row) when a node goes offline
    """

    def __init__(self, engine: Any):
        self.engine = engine
        self._lost: List[Callable] = []
        original = engine.heartbeat.sweep

        def watched_sweep() -> List[Dict[str, Any]]:
            transitions = original()
            for t in transitions:
                if t.get("new") == "offline" and t.get("old") not in (None, "offline"):
                    self._announce(t["node_id"])
            return transitions

        engine.heartbeat.sweep = watched_sweep

    def on_peer_lost(self, callback: Callable) -> None:
        self._lost.append(callback)

    def _announce(self, node_id: str) -> None:
        row = self.engine.table.get(node_id)
        for callback in list(self._lost):
            try:
                result = callback(node_id, row)
                if inspect.isawaitable(result):
                    asyncio.ensure_future(result)
            except Exception:
                pass            # one broken callback must not stop the sweep

    def lowest_cpu_node(self, exclude=(), include_self: bool = True) -> Optional[SimpleNamespace]:
        skip = set(exclude)
        own = self.engine.identity.node_id
        if not include_self:
            skip.add(own)
        while True:
            row = self.engine.get_least_loaded(skip)
            if row is None:
                return None
            if row.get("host") and row.get("port"):
                return SimpleNamespace(node_id=row["node_id"], host=row["host"],
                                       port=row["port"], cpu=row.get("cpu"), ram=row.get("ram"))
            skip.add(row["node_id"])        # no usable address yet: try the next one


def routing_for(engine: Any) -> Any:
    """Use the engine as it is if it already has lowest_cpu_node, else wrap it."""
    return engine if hasattr(engine, "lowest_cpu_node") else GossipRouting(engine)


def bridge_factory(engine_class: Callable, config: Any = None, http_port: int = 0) -> Callable:
    """The factory MeshNode expects, for the engine in modules/gossip/engine.py."""

    def factory(node_id: str, host: str, port: int, transport: Any, neighbors: Callable) -> Any:
        from modules.interfaces import NodeIdentity          # imported late on purpose
        identity = NodeIdentity(node_id=node_id, name=node_id[:8], host=host,
                                port=port, http_port=http_port)
        return engine_class(identity, transport, NeighborDirectory(neighbors), config)

    return factory