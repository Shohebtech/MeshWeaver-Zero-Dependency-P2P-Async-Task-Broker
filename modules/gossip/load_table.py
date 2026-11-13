"""Distributed load table (Module 3, requirements 4-8, 11).

One row per known node. Rows from other nodes arrive through gossip and are
merged by a per-origin sequence number: the highest ``seq`` wins, so stale or
duplicated gossip never overwrites newer data and clock differences between
machines do not matter.

Every row keeps two *local* timestamps (taken with this node's clock):
  last_update    - when a newer load reading for the node was accepted
  last_heartbeat - when we last heard directly from the node
Status is computed on read:
  offline - nothing heard (update or heartbeat) for ``offline_after`` seconds
  stale   - load reading older than ``stale_after`` seconds (or none yet)
  online  - everything else
"""
from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Tuple

from config import GossipConfig


@dataclass
class NodeLoad:
    node_id: str
    name: str = ""
    host: str = ""
    port: int = 0
    http_port: int = 0
    cpu: Optional[float] = None
    ram: Optional[float] = None
    seq: int = 0
    origin_ts: float = 0.0
    last_update: float = 0.0
    last_heartbeat: float = 0.0
    source: str = "real"          # real (peer-to-peer) | simulated | reported (REST heartbeat)
    links: List[str] = field(default_factory=list)  # node ids this node hears directly
    is_self: bool = False

    def to_wire(self) -> Dict[str, Any]:
        return {"node_id": self.node_id, "name": self.name, "host": self.host,
                "port": self.port, "http_port": self.http_port, "cpu": self.cpu,
                "ram": self.ram, "seq": self.seq, "origin_ts": self.origin_ts,
                "links": list(self.links)}


def _pct(value: Any) -> float:
    v = float(value)
    if math.isnan(v) or math.isinf(v):
        raise ValueError("not a finite number")
    return max(0.0, min(100.0, v))


def _text(value: Any, limit: int = 128) -> str:
    return str(value)[:limit]


def _int(value: Any) -> int:
    n = int(value)
    return n if 0 <= n <= 65535 else 0


class LoadTable:
    def __init__(self, config: Optional[GossipConfig] = None,
                 clock: Callable[[], float] = time.time):
        self.cfg = config or GossipConfig()
        self._clock = clock
        self._lock = threading.RLock()
        self._nodes: Dict[str, NodeLoad] = {}
        self._series: Dict[str, Deque[Tuple[float, float, float]]] = {}
        self.self_id: Optional[str] = None

    # ------------------------------------------------------------ writers
    def _record(self, node_id: str, cpu: float, ram: float, now: float) -> None:
        dq = self._series.setdefault(node_id, deque(maxlen=self.cfg.series_points))
        dq.append((round(now, 3), round(cpu, 1), round(ram, 1)))

    def update_local(self, identity, cpu: float, ram: float,
                     links: Iterable[str] = ()) -> NodeLoad:
        """Record this node's own reading and bump its sequence number."""
        now = self._clock()
        with self._lock:
            self.self_id = identity.node_id
            n = self._nodes.get(identity.node_id) or NodeLoad(identity.node_id, is_self=True)
            n.is_self = True
            n.name, n.host, n.port, n.http_port = identity.name, identity.host, identity.port, identity.http_port
            n.cpu, n.ram = _pct(cpu), _pct(ram)
            n.seq = max(n.seq + 1, int(now * 1000))   # restart-safe: grows with time
            n.origin_ts = now
            n.last_update = n.last_heartbeat = now
            n.links = sorted(set(links))
            n.source = "real"
            self._nodes[n.node_id] = n
            self._record(n.node_id, n.cpu, n.ram, now)
            return n

    def merge(self, entries: Iterable[Dict[str, Any]]) -> List[str]:
        """Merge gossiped rows. Returns ids whose data changed. Bad rows are skipped."""
        now = self._clock()
        changed: List[str] = []
        with self._lock:
            for w in entries:
                try:
                    nid = _text(w["node_id"])
                    seq = int(w["seq"])
                    cpu, ram = _pct(w["cpu"]), _pct(w["ram"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not nid or nid == self.self_id:
                    continue                      # never accept claims about ourselves
                cur = self._nodes.get(nid)
                if cur is not None and seq <= cur.seq:
                    continue                      # not newer: ignore
                if cur is None:
                    if len(self._nodes) >= self.cfg.max_nodes:
                        continue
                    cur = self._nodes[nid] = NodeLoad(nid)
                if cur.source == "simulated":
                    continue
                cur.name = _text(w.get("name", cur.name))
                cur.host = _text(w.get("host", cur.host), 255)
                cur.port = _int(w.get("port", cur.port))
                cur.http_port = _int(w.get("http_port", cur.http_port))
                cur.cpu, cur.ram, cur.seq = cpu, ram, seq
                try:
                    cur.origin_ts = float(w.get("origin_ts", 0.0))
                except (TypeError, ValueError):
                    cur.origin_ts = 0.0
                links = w.get("links", [])
                cur.links = [_text(x) for x in links][:64] if isinstance(links, list) else []
                cur.last_update = now
                cur.source = "real"
                self._record(nid, cpu, ram, now)
                changed.append(nid)
        return changed

    def touch_heartbeat(self, node_id: str, info: Optional[Dict[str, Any]] = None,
                        source: str = "real") -> bool:
        """Note that ``node_id`` is alive. Creates a placeholder row for unknown nodes."""
        now = self._clock()
        info = info or {}
        with self._lock:
            if node_id == self.self_id:
                return False
            n = self._nodes.get(node_id)
            created = n is None
            if n is None:
                if len(self._nodes) >= self.cfg.max_nodes:
                    return False
                n = self._nodes[node_id] = NodeLoad(node_id, source=source)
            if info:
                n.name = _text(info.get("name", n.name))
                n.host = _text(info.get("host", n.host), 255)
                n.port = _int(info.get("port", n.port))
                n.http_port = _int(info.get("http_port", n.http_port))
            n.last_heartbeat = now
            return created

    def upsert_simulated(self, node_id: str, name: str, cpu: float, ram: float) -> None:
        """Insert/refresh a clearly-labelled simulated row (demo mode only)."""
        now = self._clock()
        with self._lock:
            n = self._nodes.get(node_id) or NodeLoad(node_id, name=name, host="sim", source="simulated")
            n.cpu, n.ram = _pct(cpu), _pct(ram)
            n.seq += 1
            n.last_update = n.last_heartbeat = now
            n.source = "simulated"
            self._nodes[node_id] = n
            self._record(node_id, n.cpu, n.ram, now)

    def purge(self) -> List[str]:
        now = self._clock()
        with self._lock:
            gone = [i for i, n in self._nodes.items()
                    if not n.is_self and n.source != "simulated"
                    and now - max(n.last_update, n.last_heartbeat) > self.cfg.purge_after]
            for i in gone:
                del self._nodes[i]
                self._series.pop(i, None)
            return gone

    # ------------------------------------------------------------ readers
    def _status(self, n: NodeLoad, now: float) -> str:
        if n.is_self:
            return "online"
        last = max(n.last_update, n.last_heartbeat)
        return "offline" if last == 0 or now - last > self.cfg.offline_after else "online"

    def _stale(self, n: NodeLoad, now: float) -> bool:
        if n.is_self:
            return False
        return n.last_update == 0 or now - n.last_update > self.cfg.stale_after

    def _view(self, n: NodeLoad, now: float) -> Dict[str, Any]:
        status, stale = self._status(n, now), self._stale(n, now)
        return {
            "node_id": n.node_id, "name": n.name or n.node_id[:8], "host": n.host,
            "port": n.port, "http_port": n.http_port, "cpu": n.cpu, "ram": n.ram,
            "seq": n.seq, "source": n.source, "is_self": n.is_self, "links": list(n.links),
            "status": status, "stale": stale,
            "last_update": n.last_update or None, "last_heartbeat": n.last_heartbeat or None,
            "update_age": round(now - n.last_update, 2) if n.last_update else None,
            "heartbeat_age": round(now - n.last_heartbeat, 2) if n.last_heartbeat else None,
        }

    def get(self, node_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            n = self._nodes.get(node_id)
            return self._view(n, self._clock()) if n else None

    def snapshot(self) -> List[Dict[str, Any]]:
        now = self._clock()
        with self._lock:
            rows = [self._view(n, now) for n in self._nodes.values()]
        rows.sort(key=lambda r: (not r["is_self"], r["name"], r["node_id"]))
        return rows

    def wire_entries(self, limit: int) -> List[Dict[str, Any]]:
        """Rows to gossip: ourselves first, then the most recently updated real rows."""
        with self._lock:
            mine = [n for n in self._nodes.values() if n.is_self]
            others = sorted((n for n in self._nodes.values()
                             if not n.is_self and n.source == "real" and n.cpu is not None),
                            key=lambda n: n.last_update, reverse=True)
            return [n.to_wire() for n in (mine + others)[:max(1, limit)]]

    def is_online(self, node_id: str) -> bool:
        row = self.get(node_id)
        return bool(row and row["status"] == "online")

    def least_loaded(self, exclude: Iterable[str] = ()) -> Optional[Dict[str, Any]]:
        """Lowest-CPU node that is online and has fresh data (ties: lower RAM, then id)."""
        skip = set(exclude)
        rows = [r for r in self.snapshot()
                if r["status"] == "online" and not r["stale"] and r["cpu"] is not None
                and r["node_id"] not in skip]
        if not rows:
            return None
        return min(rows, key=lambda r: (r["cpu"], r["ram"] if r["ram"] is not None else 100.0, r["node_id"]))

    def summary(self) -> Dict[str, Any]:
        rows = self.snapshot()
        online = [r for r in rows if r["status"] == "online"]
        with_data = [r for r in online if r["cpu"] is not None]
        avg = lambda key: round(sum(r[key] for r in with_data) / len(with_data), 1) if with_data else None
        updates = [r["last_update"] for r in rows if r["last_update"]]
        return {
            "total_nodes": len(rows), "online_nodes": len(online),
            "offline_nodes": len(rows) - len(online),
            "stale_nodes": sum(1 for r in online if r["stale"]),
            "avg_cpu": avg("cpu"), "avg_ram": avg("ram"),
            "least_loaded": self.least_loaded(),
            "last_update": max(updates) if updates else None,
            "real_nodes": sum(1 for r in rows if r["source"] == "real"),
            "simulated_nodes": sum(1 for r in rows if r["source"] == "simulated"),
            "gossip_interval": self.cfg.interval,
            "stale_after": self.cfg.stale_after, "offline_after": self.cfg.offline_after,
        }

    def series(self) -> Dict[str, List[Dict[str, float]]]:
        with self._lock:
            return {nid: [{"t": t, "cpu": c, "ram": r} for (t, c, r) in dq]
                    for nid, dq in self._series.items()}
