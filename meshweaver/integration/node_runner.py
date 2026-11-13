from typing import Any, Callable, Optional

from meshweaver.dht.node import DHTNode
from meshweaver.integration.dispatch import Dispatcher
from meshweaver.integration.gossip_adapter import (
    GossipTransport, gossip_id, link_departures, neighbor_provider,
)
from meshweaver.integration.task_channel import TaskServer, task_port
from meshweaver.integration.task_manager import TaskManager, TaskRecord
from meshweaver.integration.task_receiver import TaskReceiver

# gossip_factory(node_id_hex, host, port, transport, get_neighbors) -> gossip engine.
# With the real gossip package it is:
#   lambda nid, host, port, transport, neighbors: gossip.GossipEngine(
#       nid, host, port, transport, get_neighbors=neighbors)
GossipFactory = Callable[[str, str, int, Any, Callable], Any]


class MeshNode:
    """
    One complete MeshWeaver node: DHT, shared UDP socket, gossip, task server
    (receives tasks) and task manager (sends tasks).
    `execute` is Module 4's execute_serialized_task, and it must be importable by name.
    Extra keyword arguments go to DHTNode (ports, bootstrap, maintenance, ...).
    """

    def __init__(self, secret: bytes, execute: Callable, gossip_factory: GossipFactory,
                 **dht_options: Any):
        self._secret, self._execute, self._gossip_factory = secret, execute, gossip_factory
        self._dht_options = dht_options
        self.dht: Optional[DHTNode] = None
        self.engine: Any = None
        self.manager: Optional[TaskManager] = None
        self._gossip_transport: Optional[GossipTransport] = None
        self._server: Optional[TaskServer] = None
        self._unlink: Optional[Callable[[], None]] = None

    @property
    def is_running(self) -> bool:
        return self.dht is not None and self.dht.is_running

    @property
    def task_address(self):
        if self._server is None:
            raise RuntimeError("node is not started")
        return self._server.address

    async def start(self):
        """Start everything and join the mesh. Returns the DHT's JoinResult."""
        if self.dht is not None:
            raise RuntimeError("node is already started")
        self.dht = DHTNode(transport_wrapper=Dispatcher, **self._dht_options)
        try:
            result = await self.dht.start()
            host, port = self.dht.address
            self._gossip_transport = GossipTransport(self.dht.protocol.transport)
            self.engine = self._gossip_factory(gossip_id(self.dht.node_id), host, port,
                                               self._gossip_transport,
                                               neighbor_provider(self.dht))
            await self.engine.start()
            self._unlink = link_departures(self.dht, self.engine)
            receiver = TaskReceiver(self._secret, self._execute)
            self._server = TaskServer(receiver.handle, host, task_port(port))
            await self._server.start()
            self.manager = TaskManager(self.engine, self._secret)
            return result
        except BaseException:
            await self.stop()          # undo whatever was already started
            raise

    async def stop(self) -> None:
        """Stop everything. Safe to call twice."""
        dht, engine, gossip_transport = self.dht, self.engine, self._gossip_transport
        server, unlink = self._server, self._unlink
        self.dht = self.engine = self.manager = None
        self._gossip_transport = self._server = self._unlink = None
        if unlink is not None:
            unlink()
        if engine is not None:
            await engine.stop()
        if gossip_transport is not None:
            await gossip_transport.stop()
        if server is not None:
            await server.stop()
        if dht is not None:
            await dht.stop()

    async def submit(self, task_bytes: bytes) -> TaskRecord:
        """Send serialized task bytes to the least busy node and wait for the outcome."""
        if self.manager is None:
            raise RuntimeError("node is not started")
        return await self.manager.submit(task_bytes)