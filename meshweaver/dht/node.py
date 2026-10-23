import asyncio
import inspect
from typing import Any, Callable, Iterable, List, Optional, Set, Tuple, Union

from .bootstrap import DEFAULT_HOST, DEFAULT_PORTS, PROBE_TIMEOUT, JoinResult, join_mesh, open_transport
from .contact import Contact
from .kbucket import K
from .lookup import iterative_find_node
from .maintenance import Maintenance
from .network_store import get_value, put_value
from .node_id import ID_BITS, generate_node_id, id_from_key
from .protocol import DHTProtocol
from .routing_table import RoutingTable
from .rpc import DEFAULT_TIMEOUT
from .transport import Address

PeerCallback = Callable[[Contact], Any]   # may be a normal function or an async one
Target = Union[int, str, bytes]           # a 160-bit ID, or any key that gets hashed to one


class ObservedRoutingTable(RoutingTable):
    """A routing table that tells us when a peer is added for the first time or removed."""

    def __init__(self, own_id: int, k: int,
                 on_added: Callable[[Contact], None],
                 on_removed: Callable[[Contact], None]):
        super().__init__(own_id, k)
        self._on_added = on_added
        self._on_removed = on_removed

    def add_contact(self, contact: Contact) -> bool:
        known = contact.node_id in self
        added = super().add_contact(contact)
        if added and not known:
            self._on_added(contact)
        return added

    def remove_contact(self, node_id: int) -> bool:
        contact = self.get(node_id)
        removed = super().remove_contact(node_id)
        if removed and contact is not None:
            self._on_removed(contact)
        return removed


class DHTNode:
    """
    The one object other modules use. Typical use:

        node = DHTNode()
        node.on_peer_joined(my_function)
        await node.start()
        ...
        await node.stop()

    or:  async with DHTNode() as node: ...
    """

    def __init__(self, host: str = DEFAULT_HOST,
                 ports: Iterable[int] = DEFAULT_PORTS,
                 bootstrap: Iterable[Address] = (),
                 scan_ports: Optional[Iterable[int]] = None,
                 probe_timeout: float = PROBE_TIMEOUT,
                 rpc_timeout: float = DEFAULT_TIMEOUT,
                 k: int = K,
                 maintenance: bool = True,
                 node_id: Optional[int] = None,
                 **maintenance_options: Any):
        self._host = host
        self._ports = list(ports)
        self._scan_ports = self._ports if scan_ports is None else list(scan_ports)
        self._bootstrap = list(bootstrap)
        self._probe_timeout = probe_timeout
        self._rpc_timeout = rpc_timeout
        self._k = k
        self._run_maintenance = maintenance
        self._fixed_id = node_id
        self._maintenance_options = maintenance_options
        self._protocol: Optional[DHTProtocol] = None
        self._maintenance: Optional[Maintenance] = None
        self._joined_listeners: List[PeerCallback] = []
        self._left_listeners: List[PeerCallback] = []
        self._callback_tasks: Set["asyncio.Future[Any]"] = set()

    # ------------------------------------------------------------------
    # Starting and stopping
    # ------------------------------------------------------------------
    async def start(self) -> JoinResult:
        """Open a port, join the mesh (or start it, if we are first) and begin maintenance."""
        if self._protocol is not None:
            raise RuntimeError("node is already started")
        transport = await open_transport(self._host, self._ports)
        node_id = generate_node_id() if self._fixed_id is None else self._fixed_id
        protocol = DHTProtocol(node_id, transport, k=self._k, timeout=self._rpc_timeout)
        protocol.routing_table = ObservedRoutingTable(
            node_id, self._k,
            lambda c: self._notify(self._joined_listeners, c),
            lambda c: self._notify(self._left_listeners, c))
        maintenance = Maintenance(protocol, **self._maintenance_options)
        self._protocol = protocol
        self._maintenance = maintenance
        try:
            result = await join_mesh(protocol, self._bootstrap, self._host,
                                     self._scan_ports, self._probe_timeout)
        except BaseException:
            self._protocol = None
            self._maintenance = None
            protocol.close()
            raise
        if self._run_maintenance:
            maintenance.start()
        return result

    async def stop(self) -> None:
        """Stop background jobs and close the port. Safe to call twice."""
        protocol, maintenance = self._protocol, self._maintenance
        if protocol is None:
            return
        self._protocol = None
        self._maintenance = None
        if maintenance is not None:
            await maintenance.stop()
        protocol.close()
        tasks = list(self._callback_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def __aenter__(self) -> "DHTNode":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # About this node
    # ------------------------------------------------------------------
    @property
    def is_running(self) -> bool:
        return self._protocol is not None

    @property
    def node_id(self) -> int:
        return self._need().node_id

    @property
    def address(self) -> Address:
        """(host, port) this node is listening on."""
        return self._need().transport.local_addr

    @property
    def contact(self) -> Contact:
        return self._need().contact

    @property
    def maintenance(self) -> Maintenance:
        """The background-job manager (for example to run check_liveness by hand)."""
        self._need()
        assert self._maintenance is not None
        return self._maintenance

    @property
    def protocol(self) -> DHTProtocol:
        """The low-level node, for code that needs ping/find_node/store directly."""
        return self._need()

    # ------------------------------------------------------------------
    # Who is around? (answers come from the routing table, no network traffic)
    # ------------------------------------------------------------------
    def peers(self) -> List[Contact]:
        """Every peer we currently know."""
        return list(self._need().routing_table.all_contacts())

    @property
    def peer_count(self) -> int:
        return len(self._need().routing_table)

    def knows(self, node_id: int) -> bool:
        return node_id in self._need().routing_table

    def get_neighbors(self, n: Optional[int] = None) -> List[Contact]:
        """Known peers nearest to THIS node by XOR distance, nearest first (None = all)."""
        protocol = self._need()
        return protocol.routing_table.find_closest(protocol.node_id, self._limit(n))

    def closest_peers(self, target: Target, n: Optional[int] = None) -> List[Contact]:
        """Known peers nearest to a target ID or key, nearest first (None = all)."""
        table = self._need().routing_table
        return table.find_closest(self._to_id(target), self._limit(n))

    # ------------------------------------------------------------------
    # Asking the network (these send messages)
    # ------------------------------------------------------------------
    async def lookup_peers(self, target: Target, n: Optional[int] = None) -> List[Contact]:
        """Search the mesh for the live peers closest to the target."""
        return await iterative_find_node(self._need(), self._to_id(target), k=n)

    async def ping(self, contact: Contact) -> bool:
        return await self._need().ping(contact)

    async def publish(self, key: Target, value: Any, ttl: Optional[float] = None) -> int:
        """Store a small JSON-safe record on the closest nodes and keep republishing it.
        Returns how many nodes accepted it."""
        self._need()
        return await self.maintenance.publish(self._to_id(key), value, ttl)

    def unpublish(self, key: Target) -> bool:
        """Stop republishing a key. Copies on other nodes expire by themselves."""
        self._need()
        return self.maintenance.unpublish(self._to_id(key))

    async def get(self, key: Target) -> Tuple[bool, Any]:
        """Read a record from anywhere in the mesh. Returns (found, value)."""
        return await get_value(self._need(), self._to_id(key))

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def on_peer_joined(self, callback: PeerCallback) -> Callable[[], None]:
        """callback(contact) runs when a peer is learned for the first time.
        Register BEFORE start() to also see the peers found while joining.
        Returns a function that removes the callback."""
        return self._subscribe(self._joined_listeners, callback)

    def on_peer_left(self, callback: PeerCallback) -> Callable[[], None]:
        """callback(contact) runs when a dead peer is removed from the routing table."""
        return self._subscribe(self._left_listeners, callback)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _need(self) -> DHTProtocol:
        if self._protocol is None:
            raise RuntimeError("node is not started")
        return self._protocol

    @staticmethod
    def _limit(n: Optional[int]) -> int:
        if n is None:
            return 2 ** 31
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise ValueError("n must be a non-negative integer or None")
        return n

    @staticmethod
    def _to_id(target: Target) -> int:
        if isinstance(target, bool):
            raise TypeError("target must be an int, str or bytes")
        if isinstance(target, int):
            if not 0 <= target < 2 ** ID_BITS:
                raise ValueError("ID out of range")
            return target
        if isinstance(target, (str, bytes)):
            return id_from_key(target)
        raise TypeError("target must be an int, str or bytes")

    @staticmethod
    def _subscribe(listeners: List[PeerCallback], callback: PeerCallback) -> Callable[[], None]:
        listeners.append(callback)

        def unsubscribe() -> None:
            if callback in listeners:
                listeners.remove(callback)

        return unsubscribe

    def _notify(self, listeners: List[PeerCallback], contact: Contact) -> None:
        for callback in list(listeners):
            try:
                result = callback(contact)
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    self._callback_tasks.add(task)
                    task.add_done_callback(self._callback_finished)
            except Exception:
                pass  # a broken callback must never break the DHT

    def _callback_finished(self, task: "asyncio.Future[Any]") -> None:
        self._callback_tasks.discard(task)
        if not task.cancelled():
            task.exception()  # mark as seen so Python doesn't warn about it