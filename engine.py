"""Gossip engine: periodic push-gossip of CPU/RAM load.

Every `interval` seconds (default 5) a node
  1. samples its own load and bumps its sequence number,
  2. picks up to `fanout` random neighbours,
  3. sends them its own entry plus a random sample of the fresh entries it
     knows about (so information spreads transitively across the whole mesh).
Received entries are merged into the LoadTable; the HeartbeatMonitor watches
that table for silent peers.

Message (type "LOAD_UPDATE"):
    {"type": "LOAD_UPDATE", "sender": "<node_id>",
     "entries": [{"id","host","port","cpu","ram","seq","ts"}, ...]}
"""
import asyncio
import random
import time
from typing import Callable, Iterable, List, Optional, Tuple

from .heartbeat import HeartbeatMonitor
from .sampler import LoadSampler
from .table import LoadEntry, LoadTable

MSG_LOAD_UPDATE = "LOAD_UPDATE"

# get_neighbors() -> iterable of (node_id, host, port); supplied by the DHT (Module 2)
NeighborFn = Callable[[], Iterable[Tuple[str, str, int]]]


class GossipEngine:
    def __init__(self, node_id: str, host: str, port: int, transport,
                 get_neighbors: Optional[NeighborFn] = None,
                 interval: float = 5.0, fanout: int = 3,
                 max_entries_per_msg: int = 10, missed_threshold: int = 3,
                 sampler: Optional[LoadSampler] = None):
        self.node_id, self.host, self.port = node_id, host, port
        self.transport = transport
        self.get_neighbors = get_neighbors
        self.interval = interval
        self.fanout = fanout
        self.max_entries = max_entries_per_msg
        self.sampler = sampler or LoadSampler()
        self.table = LoadTable(node_id, stale_after=interval * missed_threshold * 2)
        self.heartbeat = HeartbeatMonitor(self.table, interval, missed_threshold)
        self._seq = int(time.time())          # restart-safe: always higher than before
        self._task: Optional[asyncio.Task] = None
        self.rounds = 0

    # ---------- lifecycle ----------
    async def start(self) -> None:
        self.transport.register_handler(MSG_LOAD_UPDATE, self.handle_message)
        self._refresh_self()
        self.heartbeat.start()
        self._task = asyncio.ensure_future(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self.heartbeat.stop()

    async def _loop(self) -> None:
        next_tick = time.monotonic()
        while True:
            try:
                await self.gossip_once()
            except Exception as exc:  # never let the loop die
                print(f"[gossip {self.node_id}] round error: {exc!r}")
            next_tick += self.interval                      # no drift
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))

    # ---------- one round ----------
    def _refresh_self(self) -> LoadEntry:
        cpu, ram = self.sampler.sample()
        self._seq += 1
        entry = LoadEntry(self.node_id, self.host, self.port, cpu, ram, self._seq, time.time())
        self.table.merge(entry)
        return entry

    def _pick_targets(self) -> List[Tuple[str, str, int]]:
        if self.get_neighbors is not None:
            pool = list(self.get_neighbors())
        else:  # standalone fallback: gossip to whoever we've heard of
            pool = [(e.node_id, e.host, e.port) for e in self.table.peers(include_stale=False)]
        pool = [p for p in pool if p[0] != self.node_id and self.heartbeat.is_alive(p[0])]
        random.shuffle(pool)
        return pool[: self.fanout]

    def _build_message(self, own: LoadEntry) -> dict:
        others = [e for e in self.table.peers(include_stale=False)]
        random.shuffle(others)
        sample = others[: max(0, self.max_entries - 1)]
        return {"type": MSG_LOAD_UPDATE, "sender": self.node_id,
                "entries": [own.to_wire()] + [e.to_wire() for e in sample]}

    async def gossip_once(self) -> None:
        own = self._refresh_self()
        targets = self._pick_targets()
        self.rounds += 1
        if not targets:
            return
        msg = self._build_message(own)
        await asyncio.gather(*(self.transport.send((h, p), msg) for _, h, p in targets),
                             return_exceptions=True)

    # ---------- receiving ----------
    async def handle_message(self, msg: dict, addr) -> None:
        for raw in msg.get("entries", []):
            try:
                entry = LoadEntry.from_wire(raw)
            except (KeyError, TypeError, ValueError):
                continue                       # skip malformed entries
            if entry.node_id == self.node_id:
                continue                       # we are the authority on ourselves
            self.table.merge(entry)

    # ---------- API for other modules ----------
    def lowest_cpu_node(self, exclude: Iterable[str] = (), include_self: bool = True) -> Optional[LoadEntry]:
        """Task routing: fresh, alive node with the lowest CPU load."""
        excl = set(exclude) | set(self.heartbeat.dead)
        return self.table.lowest_cpu(exclude=excl, include_self=include_self)

    def load_table(self, include_stale: bool = True) -> List[dict]:
        return self.table.snapshot(include_stale)

    def alive_peers(self) -> List[LoadEntry]:
        return [e for e in self.table.peers(include_stale=True) if self.heartbeat.is_alive(e.node_id)]

    def dead_peers(self) -> List[str]:
        return sorted(self.heartbeat.dead)

    def on_peer_lost(self, cb: Callable) -> None:
        """cb(node_id, entry) — Task Serializer uses this to mark tasks failed / re-route."""
        self.heartbeat.on_lost(cb)

    def on_peer_recovered(self, cb: Callable) -> None:
        self.heartbeat.on_recovered(cb)