"""In-memory transport for fast, deterministic tests (NOT used at runtime).

``MemoryHub`` connects several ``MemoryTransport`` objects inside one process and
can take an address "down" to simulate a crashed node.
"""
from __future__ import annotations

import asyncio
import copy
from typing import Any, Dict, Set

from modules.interfaces import MessageHandler, PeerAddr


class MemoryHub:
    def __init__(self):
        self.endpoints: Dict[PeerAddr, "MemoryTransport"] = {}
        self.down: Set[PeerAddr] = set()

    def transport(self, addr: PeerAddr) -> "MemoryTransport":
        return MemoryTransport(self, addr)


class MemoryTransport:
    mock = True
    implementation = "MemoryTransport (test helper)"

    def __init__(self, hub: MemoryHub, addr: PeerAddr):
        self.hub, self.addr = hub, addr
        self.host, self.port = addr
        self._handler: MessageHandler | None = None
        self.stats = {"sent": 0, "received": 0}

    async def start(self, handler: MessageHandler) -> None:
        self._handler = handler
        self.hub.endpoints[self.addr] = self

    async def send(self, addr: PeerAddr, message: Dict[str, Any]) -> bool:
        target = self.hub.endpoints.get(addr)
        if target is None or addr in self.hub.down or self.addr in self.hub.down:
            return False
        self.stats["sent"] += 1
        msg = copy.deepcopy(message)
        asyncio.get_running_loop().call_soon(target._deliver, msg, self.addr)
        return True

    def _deliver(self, msg: Dict[str, Any], src: PeerAddr) -> None:
        if self._handler and self.addr not in self.hub.down:
            self.stats["received"] += 1
            self._handler(msg, src)

    async def stop(self) -> None:
        self.hub.endpoints.pop(self.addr, None)

    def info(self) -> Dict[str, Any]:
        return {"implementation": self.implementation, "mock": True,
                "real_network_traffic": False, "stats": dict(self.stats)}
