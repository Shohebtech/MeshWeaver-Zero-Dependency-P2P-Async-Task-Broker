"""Gossip Protocol Engine (MODULE 3).

Every ``interval`` seconds (default 5) the engine:
  1. samples this node's CPU and RAM,
  2. updates its own row in the load table,
  3. pushes a slice of the table to ``fanout`` random neighbours (gossip),
  4. sends a small heartbeat to every known neighbour.
Received gossip is merged into the table, so information hops across the mesh.
A separate sweep loop turns missing heartbeats into stale/offline transitions.

The engine only talks to the outside through the interfaces in
``modules/interfaces.py`` (Transport, PeerDirectory) and *is* the LoadProvider
other modules call (get_table / get_least_loaded / is_online).
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import threading
import time
from collections import defaultdict, deque
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence

from config import GossipConfig
from modules.interfaces import NodeIdentity, PeerAddr

log = logging.getLogger("meshweaver.gossip")
PROTOCOL_VERSION = 1
# Rare, important events get their own buffer so heartbeat chatter can never push them out.
KEY_EVENT_KINDS = frozenset({"status_change", "auth_failed", "peer_discovered"})


class GossipEngine:
    def __init__(self, identity: NodeIdentity, transport, directory,
                 config: Optional[GossipConfig] = None, monitor=None, table=None,
                 heartbeat=None, clock: Callable[[], float] = time.time,
                 rng: Optional[random.Random] = None):
        # imported here so each file can be added to git on its own milestone day
        from modules.gossip.heartbeat import HeartbeatMonitor
        from modules.gossip.load_monitor import LoadMonitor
        from modules.gossip.load_table import LoadTable

        self.identity, self.transport, self.directory = identity, transport, directory
        self.cfg = config or GossipConfig()
        self._clock = clock
        self._rng = rng or random.Random()
        self.monitor = monitor or LoadMonitor()
        self.table = table or LoadTable(self.cfg, clock)
        self.heartbeat = heartbeat or HeartbeatMonitor(self.table, self.cfg, clock)
        self._events: Deque[Dict[str, Any]] = deque(maxlen=self.cfg.event_history)
        self._key_events: Deque[Dict[str, Any]] = deque(maxlen=self.cfg.event_history)
        self._elock = threading.Lock()
        self.stats: Dict[str, int] = defaultdict(int)
        self.started_at: Optional[float] = None
        self.last_tick: Optional[float] = None
        self.next_tick_at: Optional[float] = None
        self._tasks: List[asyncio.Task] = []
        self._hooks: List[Callable[["GossipEngine"], None]] = []
        self.running = False

    # ------------------------------------------------ LoadProvider contract
    def get_table(self) -> List[Dict[str, Any]]:
        return self.table.snapshot()

    def get_least_loaded(self, exclude: Sequence[str] = ()) -> Optional[Dict[str, Any]]:
        return self.table.least_loaded(exclude)

    def is_online(self, node_id: str) -> bool:
        return self.table.is_online(node_id)

    # ------------------------------------------------------------ lifecycle
    def add_tick_hook(self, fn: Callable[["GossipEngine"], None]) -> None:
        self._hooks.append(fn)

    async def start(self) -> None:
        await self.transport.start(self._on_message)
        self.identity.port = getattr(self.transport, "port", self.identity.port) or self.identity.port
        self.started_at = self._clock()
        self.running = True
        await self.tick()
        self._tasks = [asyncio.create_task(self._gossip_loop(), name="gossip-loop"),
                       asyncio.create_task(self._sweep_loop(), name="gossip-sweep")]

    async def stop(self) -> None:
        self.running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        await self.transport.stop()

    async def _gossip_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.interval)
            try:
                await self.tick()
            except Exception:  # keep gossiping whatever happens
                log.exception("gossip tick failed")
                self.stats["tick_errors"] += 1

    async def _sweep_loop(self) -> None:
        period = max(0.05, self.cfg.interval / 5.0)
        while True:
            await asyncio.sleep(period)
            self.sweep()

    def sweep(self) -> None:
        for t in self.heartbeat.sweep():
            if t["old"] is None and t["new"] != "offline":
                self._event("status_change", t["node_id"], f"{t['name']} joined ({t['new']})", t["source"])
            elif t["new"] == "offline":
                self._event("status_change", t["node_id"],
                            f"{t['name']} went OFFLINE (no heartbeat for {self.cfg.offline_after:g}s)", t["source"])
            elif t["new"] == "stale":
                self._event("status_change", t["node_id"], f"{t['name']} load data is STALE", t["source"])
            else:
                self._event("status_change", t["node_id"], f"{t['name']} is online again", t["source"])
        self.table.purge()

    # ----------------------------------------------------------------- tick
    async def tick(self) -> None:
        now = self._clock()
        sample = self.monitor.sample()
        me = self.table.update_local(self.identity, sample.cpu, sample.ram, self.heartbeat.direct_peers())
        self.last_tick = now
        self.next_tick_at = now + self.cfg.interval
        self.stats["ticks"] += 1
        self._event("tick", me.node_id, f"sampled CPU {sample.cpu:.1f}% RAM {sample.ram:.1f}% ({sample.backend})", "real")
        for hook in self._hooks:
            try:
                hook(self)
            except Exception:
                log.exception("tick hook failed")

        peers = self.directory.get_peers()
        if not peers:
            return
        targets = self._rng.sample(peers, min(self.cfg.fanout, len(peers)))
        gossip = self._pack(self._body("gossip", now))
        sent = 0
        for addr in targets:
            sent += bool(await self.transport.send(addr, gossip))
        self.stats["gossip_sent"] += sent
        self._event("gossip_sent", me.node_id, f"pushed {len(self.table.wire_entries(self.cfg.max_entries_per_message))} rows to {sent}/{len(targets)} neighbours", "real")

        beat = self._pack(self._body("heartbeat", now))
        sent = 0
        for addr in peers:
            sent += bool(await self.transport.send(addr, beat))
        self.stats["heartbeat_sent"] += sent
        self._event("heartbeat_sent", me.node_id, f"heartbeat to {sent}/{len(peers)} neighbours", "real")

    # ----------------------------------------------------------- wire format
    def _sender(self) -> Dict[str, Any]:
        i = self.identity
        return {"node_id": i.node_id, "name": i.name, "host": i.host, "port": i.port, "http_port": i.http_port}

    def _body(self, kind: str, now: float) -> Dict[str, Any]:
        body: Dict[str, Any] = {"v": PROTOCOL_VERSION, "type": kind, "sender": self._sender(), "ts": now}
        if kind == "gossip":
            body["entries"] = self.table.wire_entries(self.cfg.max_entries_per_message)
            body["peers"] = [list(a) for a in self.directory.get_peers()][:50]
        return body

    @staticmethod
    def _canonical(body: Dict[str, Any]) -> bytes:
        return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()

    def _pack(self, body: Dict[str, Any]) -> Dict[str, Any]:
        if not self.cfg.secret:
            return body
        sig = hmac.new(self.cfg.secret.encode(), self._canonical(body), hashlib.sha256).hexdigest()
        return {"body": body, "sig": sig}

    def _unpack(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.cfg.secret:
            return msg.get("body", msg) if "sig" in msg and isinstance(msg.get("body"), dict) else msg
        body, sig = msg.get("body"), msg.get("sig")
        if not isinstance(body, dict) or not isinstance(sig, str):
            return None
        expect = hmac.new(self.cfg.secret.encode(), self._canonical(body), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expect, sig):
            return None
        try:
            if abs(self._clock() - float(body.get("ts", 0))) > self.cfg.max_clock_skew:
                return None
        except (TypeError, ValueError):
            return None
        return body

    # -------------------------------------------------------------- receive
    def _on_message(self, msg: Dict[str, Any], addr: PeerAddr) -> None:
        body = self._unpack(msg)
        if body is None:
            self.stats["rejected"] += 1
            self._event("auth_failed", None, f"rejected message from {addr[0]}:{addr[1]} (bad or missing signature)", "real")
            return
        sender = body.get("sender")
        if not isinstance(sender, dict) or not isinstance(sender.get("node_id"), str):
            self.stats["malformed"] += 1
            return
        sid = sender["node_id"][:128]
        if sid == self.identity.node_id:
            return
        try:
            s_addr = (str(sender["host"])[:255], int(sender["port"]))
        except (KeyError, TypeError, ValueError):
            self.stats["malformed"] += 1
            return
        kind = body.get("type")
        name = str(sender.get("name") or sid[:8])
        self.stats["received"] += 1
        created = self.heartbeat.beat(sid, sender, direct=True)
        self._learn(s_addr, sid, name)

        if kind == "heartbeat":
            self.stats["heartbeat_recv"] += 1
            self._event("heartbeat_recv", sid, f"heartbeat from {name}", "real")
        elif kind == "gossip":
            entries = body.get("entries")
            entries = entries if isinstance(entries, list) else []
            changed = self.table.merge(e for e in entries[: self.cfg.max_entries_per_message] if isinstance(e, dict))
            for nid in changed:
                row = self.table.get(nid)
                if row and row["host"] and row["port"]:
                    self._learn((row["host"], row["port"]), nid, row["name"])
            for p in (body.get("peers") or [])[:50]:
                try:
                    self.directory.add_peer((str(p[0])[:255], int(p[1])))
                except (TypeError, ValueError, IndexError):
                    pass
            self.stats["gossip_recv"] += 1
            self._event("gossip_recv", sid, f"from {name}: {len(changed)} of {len(entries)} rows were newer", "real")
        else:
            self.stats["malformed"] += 1

    def _learn(self, addr: PeerAddr, node_id: str, name: str) -> None:
        if addr == (self.identity.host, self.identity.port):
            return
        if self.directory.add_peer(addr, node_id):
            self._event("peer_discovered", node_id, f"discovered {name} at {addr[0]}:{addr[1]}", "real")

    # ------------------------------------------------------------ REST feed
    def record_heartbeat(self, node_id: str, info: Optional[Dict[str, Any]] = None) -> bool:
        """Used by POST /api/gossip/heartbeat. Not a direct UDP contact."""
        created = self.heartbeat.beat(node_id, info, direct=False, source="reported")
        self._event("heartbeat_recv", node_id, f"heartbeat reported via REST for {(info or {}).get('name') or node_id[:8]}", "reported")
        return created

    # -------------------------------------------------------------- history
    def _event(self, kind: str, node_id: Optional[str], detail: str, source: str = "real") -> None:
        ev = {"ts": self._clock(), "kind": kind, "node_id": node_id, "detail": detail, "source": source}
        with self._elock:
            self._events.append(ev)
            if kind in KEY_EVENT_KINDS:
                self._key_events.append(ev)

    def events(self, limit: int = 100, kinds: Optional[Sequence[str]] = None,
               node_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._elock:
            only_key = bool(kinds) and set(kinds) <= KEY_EVENT_KINDS
            items = list(self._key_events if only_key else self._events)
        if kinds:
            items = [e for e in items if e["kind"] in kinds]
        if node_id:
            items = [e for e in items if e["node_id"] == node_id]
        return list(reversed(items[-max(1, limit):]))     # newest first

    def health(self) -> Dict[str, Any]:
        now = self._clock()
        return {
            "status": "running" if self.running else "stopped",
            "node": {"node_id": self.identity.node_id, "name": self.identity.name,
                     "host": self.identity.host, "port": self.identity.port,
                     "http_port": self.identity.http_port},
            "server_time": now,
            "uptime": round(now - self.started_at, 1) if self.started_at else 0,
            "gossip_interval": self.cfg.interval,
            "stale_after": self.cfg.stale_after, "offline_after": self.cfg.offline_after,
            "fanout": self.cfg.fanout, "last_tick": self.last_tick, "next_tick_at": self.next_tick_at,
            "monitor_backend": getattr(self.monitor, "backend", "custom"),
            "signed_messages": bool(self.cfg.secret),
            "stats": dict(self.stats),
            "transport": self.transport.info(), "directory": self.directory.info(),
        }
