import asyncio
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

from .contact import Contact
from .lookup import iterative_find_node
from .node_id import generate_node_id
from .protocol import DHTProtocol
from .transport import Address, UDPTransport

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORTS = range(9000, 9020)   # 20 well-known ports the mesh lives on
PROBE_TIMEOUT = 0.5                 # seconds to wait for a knock to be answered
_UNKNOWN_ID = 0                     # we don't know a bootstrap peer's real ID yet


@dataclass
class JoinResult:
    joined: bool                    # False = nobody found, we are the first node
    bootstrap: Optional[Contact]    # the peer that answered first
    peers: int                      # how many peers we know after joining


def parse_addr(text: str) -> Address:
    """Turn 'host:port' into (host, port). Raises ValueError if it is malformed."""
    host, sep, port = text.rpartition(":")
    if not sep or not host or not port.isdigit() or not 0 < int(port) < 65536:
        raise ValueError(f"expected host:port, got {text!r}")
    return host, int(port)


async def open_transport(host: str = DEFAULT_HOST,
                         ports: Iterable[int] = DEFAULT_PORTS) -> UDPTransport:
    """Bind to the first free port in `ports`. Port 0 means 'any port the OS picks'."""
    for port in ports:
        transport = UDPTransport(host, port)
        try:
            await transport.start()
            return transport
        except OSError:
            continue  # port already used by another node: try the next one
    raise RuntimeError("no free port available in the given range")


async def _probe(node: DHTProtocol, addr: Address, timeout: float) -> Optional[List[Contact]]:
    """
    Knock on one address with a FIND_NODE for our own ID. We don't know the
    peer's real ID, so we use a placeholder. The reply carries the real ID and
    the protocol adds the peer to our routing table automatically.
    Returns the peer's answer, or None if nobody replied.
    """
    peer = Contact(_UNKNOWN_ID, addr[0], addr[1])
    try:
        return await asyncio.wait_for(node.find_node(peer, node.node_id), timeout)
    except asyncio.TimeoutError:
        return None


async def _first_live(node: DHTProtocol, addrs: List[Address],
                      timeout: float) -> Optional[Address]:
    """Probe every address at the same time; return the first one that answers."""
    if not addrs:
        return None
    tasks = {asyncio.ensure_future(_probe(node, a, timeout)): a for a in addrs}
    waiting = set(tasks)
    try:
        while waiting:
            done, waiting = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.result() is not None:
                    return tasks[task]
        return None
    finally:
        for task in waiting:
            task.cancel()
        if waiting:
            await asyncio.gather(*waiting, return_exceptions=True)


def _contact_at(node: DHTProtocol, addr: Address) -> Optional[Contact]:
    for contact in node.routing_table.all_contacts():
        if contact.addr == addr:
            return contact
    return None


async def join_mesh(node: DHTProtocol,
                    bootstrap: Iterable[Address] = (),
                    scan_host: str = DEFAULT_HOST,
                    scan_ports: Iterable[int] = DEFAULT_PORTS,
                    probe_timeout: float = PROBE_TIMEOUT) -> JoinResult:
    """
    1. Try the addresses we were given (manual fallback).
    2. If none answers, knock on every port in the scan range.
    3. Look up our own ID so we learn our neighbours and they learn us.
    """
    own = node.transport.local_addr
    explicit = [a for a in bootstrap if a != own]
    live = await _first_live(node, explicit, probe_timeout)
    if live is None:
        scan = [(scan_host, p) for p in scan_ports if (scan_host, p) != own]
        live = await _first_live(node, scan, probe_timeout)
    if live is None:
        return JoinResult(False, None, 0)  # nobody there: we are the first node

    await iterative_find_node(node, node.node_id)
    return JoinResult(True, _contact_at(node, live), len(node.routing_table))


async def start_node(host: str = DEFAULT_HOST,
                     ports: Iterable[int] = DEFAULT_PORTS,
                     bootstrap: Iterable[Address] = (),
                     probe_timeout: float = PROBE_TIMEOUT) -> Tuple[DHTProtocol, JoinResult]:
    """One call: open a port, create a node with a random ID, join the mesh."""
    ports = list(ports)
    transport = await open_transport(host, ports)
    node = DHTProtocol(generate_node_id(), transport)
    result = await join_mesh(node, bootstrap, host, ports, probe_timeout)
    return node, result