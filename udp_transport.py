"""Minimal asyncio UDP transport (JSON datagrams) so Module 3 runs standalone.

When Module 1 is ready, replace this with an adapter exposing the same two
methods the engine uses:
    register_handler(msg_type: str, handler: async (msg: dict, addr) -> None)
    async send(addr: (host, port), msg: dict) -> None
"""
import asyncio
import json
from typing import Awaitable, Callable, Dict, Tuple

Handler = Callable[[dict, Tuple[str, int]], Awaitable[None]]


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, owner: "UDPTransport"):
        self.owner = owner

    def datagram_received(self, data, addr):
        try:
            msg = json.loads(data.decode())
            handler = self.owner._handlers.get(msg.get("type"))
        except (ValueError, UnicodeDecodeError, AttributeError):
            return  # malformed datagram: drop silently
        if handler:
            asyncio.ensure_future(self.owner._safe(handler, msg, addr))

    def error_received(self, exc):
        pass  # e.g. ICMP unreachable; heartbeat monitor handles dead peers


class UDPTransport:
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host, self.port = host, port
        self._handlers: Dict[str, Handler] = {}
        self._transport = None

    def register_handler(self, msg_type: str, handler: Handler) -> None:
        self._handlers[msg_type] = handler

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _Protocol(self), local_addr=(self.host, self.port))
        self.port = self._transport.get_extra_info("sockname")[1]

    async def stop(self) -> None:
        if self._transport:
            self._transport.close()
            self._transport = None

    async def send(self, addr: Tuple[str, int], msg: dict) -> None:
        if self._transport is None:
            return
        self._transport.sendto(json.dumps(msg, separators=(",", ":")).encode(), addr)

    async def _safe(self, handler, msg, addr):
        try:
            await handler(msg, addr)
        except Exception as exc:
            print(f"[udp] handler error: {exc!r}")