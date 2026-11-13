"""Heartbeat monitoring and offline-node detection (Module 3, requirements 9-10).

A heartbeat is any datagram received directly from a peer (explicit heartbeat
messages and gossip pushes both count) or a POST to /api/gossip/heartbeat.
``sweep()`` is called frequently by the engine; it compares each node's current
state with the previous one and reports transitions such as online -> offline.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List, Optional

from config import GossipConfig


class HeartbeatMonitor:
    def __init__(self, table, config: Optional[GossipConfig] = None,
                 clock: Callable[[], float] = time.time):
        self.table = table
        self.cfg = config or GossipConfig()
        self._clock = clock
        self._lock = threading.Lock()
        self._direct: Dict[str, float] = {}   # node_id -> last direct contact
        self._last_state: Dict[str, str] = {}

    def beat(self, node_id: str, info: Optional[Dict[str, Any]] = None,
             direct: bool = True, source: str = "real") -> bool:
        """Register a heartbeat. Returns True if the node was previously unknown."""
        created = self.table.touch_heartbeat(node_id, info, source=source)
        if direct:
            with self._lock:
                self._direct[node_id] = self._clock()
        return created

    def direct_peers(self) -> List[str]:
        """Nodes we heard from directly within the offline window (used for topology)."""
        now = self._clock()
        with self._lock:
            return sorted(n for n, t in self._direct.items() if now - t <= self.cfg.offline_after)

    @staticmethod
    def _label(row: Dict[str, Any]) -> str:
        if row["status"] == "offline":
            return "offline"
        return "stale" if row["stale"] else "online"

    def states(self) -> Dict[str, str]:
        return {r["node_id"]: self._label(r) for r in self.table.snapshot()}

    def sweep(self) -> List[Dict[str, Any]]:
        """Return state transitions since the previous sweep."""
        now = self._clock()
        out: List[Dict[str, Any]] = []
        rows = self.table.snapshot()
        with self._lock:
            for r in rows:
                new = self._label(r)
                old = self._last_state.get(r["node_id"])
                if old != new:
                    self._last_state[r["node_id"]] = new
                    out.append({"ts": now, "node_id": r["node_id"], "name": r["name"],
                                "old": old, "new": new, "source": r["source"]})
            known = {r["node_id"] for r in rows}
            for nid in [n for n in self._last_state if n not in known]:
                del self._last_state[nid]
                self._direct.pop(nid, None)
        return out
