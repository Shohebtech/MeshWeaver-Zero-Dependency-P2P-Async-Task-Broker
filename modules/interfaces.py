"""Shared contracts between the four MeshWeaver modules.

Each module owner implements the Protocol for their module; everyone else only
depends on these signatures. Module 3 (gossip) *implements* ``LoadProvider`` and
*consumes* ``Transport`` (Module 1) and ``PeerDirectory`` (Module 2).
Module 4 consumes ``LoadProvider`` and implements ``TaskExecutor``.

Wire message format used by Module 3 (JSON object, one per UDP datagram):

  {"v": 1, "type": "gossip" | "heartbeat",
   "sender": {"node_id", "name", "host", "port", "http_port"},
   "ts": <unix seconds>,
   "entries": [ {node_id,name,host,port,http_port,cpu,ram,seq,origin_ts,links} ],  # gossip only
   "peers": [[host, port], ...]}                                                   # gossip only

When a shared secret is configured the object is wrapped as
{"body": <the object above>, "sig": <hex HMAC-SHA256 of canonical JSON body>}.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, Tuple, runtime_checkable

PeerAddr = Tuple[str, int]                    # (host, udp_port)
MessageHandler = Callable[[Dict[str, Any], PeerAddr], None]


@dataclass
class NodeIdentity:
    node_id: str
    name: str
    host: str = "127.0.0.1"      # address other nodes should use to reach us
    port: int = 0                # UDP gossip port
    http_port: int = 0           # dashboard / REST port


@runtime_checkable
class Transport(Protocol):
    """MODULE 1 contract: non-blocking datagram delivery between nodes."""
    mock: bool
    implementation: str

    async def start(self, handler: MessageHandler) -> None: ...
    async def send(self, addr: PeerAddr, message: Dict[str, Any]) -> bool: ...
    async def stop(self) -> None: ...
    def info(self) -> Dict[str, Any]: ...


@runtime_checkable
class PeerDirectory(Protocol):
    """MODULE 2 contract: who are my neighbours?"""
    mock: bool
    implementation: str

    def get_peers(self) -> List[PeerAddr]: ...
    def add_peer(self, addr: PeerAddr, node_id: Optional[str] = None) -> bool: ...
    def remove_peer(self, addr: PeerAddr) -> None: ...
    def info(self) -> Dict[str, Any]: ...


@runtime_checkable
class LoadProvider(Protocol):
    """MODULE 3 contract (implemented by GossipEngine): load and liveness data."""
    def get_table(self) -> List[Dict[str, Any]]: ...
    def get_least_loaded(self, exclude: Sequence[str] = ()) -> Optional[Dict[str, Any]]: ...
    def is_online(self, node_id: str) -> bool: ...


@runtime_checkable
class TaskExecutor(Protocol):
    """MODULE 4 contract: submit work and read task state."""
    mock: bool
    implementation: str

    def submit(self, name: str, **kwargs: Any) -> Dict[str, Any]: ...
    def list_tasks(self) -> List[Dict[str, Any]]: ...
    def summary(self) -> Dict[str, Any]: ...
