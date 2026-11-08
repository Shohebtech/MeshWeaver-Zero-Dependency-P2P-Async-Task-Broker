"""MOCK ADAPTER for MODULE 1 (Asynchronous Networking).

*** Placeholder until the teammate's real networking module is integrated. ***
It is a minimal asyncio UDP transport, so gossip between local node processes
is REAL network traffic - only the module itself is a stand-in (``mock = True``).
Replace by any class that satisfies ``modules.interfaces.Transport``.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

from modules.interfaces import MessageHandler, PeerAddr

MAX_DATAGRAM = 60_000


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, owner: "MockUdpTransport"):
        self.owner = owner

    def datagram_received(self, data: bytes, addr) -> None:
        o = self.owner
        o.stats["received"] += 1
        try:
            msg = json.loads(data.decode("utf-8"))
            if not isinstance(msg, dict):
                raise ValueError("message is not an object")
        except (ValueError, UnicodeDecodeError):
            o.stats["malformed"] += 1
            return
        if o._handler:
            try:
                o._handler(msg, (addr[0], addr[1]))
            except Exception:  # a bad message must never kill the socket
                o.stats["handler_errors"] += 1

    def error_received(self, exc) -> None:
        self.owner.stats["socket_errors"] += 1


class MockUdpTransport:
    mock = True
    implementation = "MockUdpTransport (Module 1 placeholder; real UDP sockets)"

    def __init__(self, host: str = "0.0.0.0", port: int = 0):
        self.host, self.port = host, port
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._handler: Optional[MessageHandler] = None
        self.stats = {"sent": 0, "received": 0, "malformed": 0, "send_failed": 0,
                      "socket_errors": 0, "handler_errors": 0}

    async def start(self, handler: MessageHandler) -> None:
        self._handler = handler
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self), local_addr=(self.host, self.port))
        self.port = self._transport.get_extra_info("sockname")[1]

    async def send(self, addr: PeerAddr, message: Dict[str, Any]) -> bool:
        if self._transport is None:
            return False
        data = json.dumps(message, separators=(",", ":")).encode("utf-8")
        if len(data) > MAX_DATAGRAM:
            self.stats["send_failed"] += 1
            return False
        try:
            self._transport.sendto(data, addr)
            self.stats["sent"] += 1
            return True
        except OSError:
            self.stats["send_failed"] += 1
            return False

    async def stop(self) -> None:
        if self._transport:
            self._transport.close()
            self._transport = None

    def info(self) -> Dict[str, Any]:
        return {"implementation": self.implementation, "mock": self.mock,
                "real_network_traffic": True, "protocol": "UDP",
                "bind": f"{self.host}:{self.port}", "stats": dict(self.stats)}
