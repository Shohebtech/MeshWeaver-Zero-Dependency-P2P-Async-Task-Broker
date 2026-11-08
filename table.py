"""Load table: the merged view of every node's CPU/RAM load.

Freshness rules
* Each node stamps its own entry with an increasing sequence number (`seq`).
  A received entry replaces ours only if its seq is higher, so old gossip can
  never overwrite newer data and clock differences between machines don't matter.
* Staleness is judged with the LOCAL monotonic clock: `received_at` is set when
  an entry with a new seq first arrives here.
"""
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional


@dataclass
class LoadEntry:
    node_id: str
    host: str
    port: int
    cpu: float
    ram: float
    seq: int
    origin_ts: float = 0.0                              # wall-clock at origin (informational)
    received_at: float = field(default=0.0, compare=False)  # local monotonic, not sent

    def to_wire(self) -> dict:
        return {"id": self.node_id, "host": self.host, "port": self.port,
                "cpu": round(self.cpu, 2), "ram": round(self.ram, 2),
                "seq": self.seq, "ts": self.origin_ts}

    @classmethod
    def from_wire(cls, d: dict) -> "LoadEntry":
        return cls(node_id=str(d["id"]), host=str(d["host"]), port=int(d["port"]),
                   cpu=float(d["cpu"]), ram=float(d["ram"]),
                   seq=int(d["seq"]), origin_ts=float(d.get("ts", 0.0)))


class LoadTable:
    def __init__(self, self_id: str, stale_after: float = 15.0):
        self.self_id = self_id
        self.stale_after = stale_after
        self._entries: Dict[str, LoadEntry] = {}

    # ---------- updates ----------
    def merge(self, entry: LoadEntry, now: Optional[float] = None) -> bool:
        """Merge one entry. Returns True if it was new information."""
        now = time.monotonic() if now is None else now
        cur = self._entries.get(entry.node_id)
        if cur is not None and entry.seq <= cur.seq:
            return False
        entry.received_at = now
        self._entries[entry.node_id] = entry
        return True

    def remove(self, node_id: str) -> None:
        self._entries.pop(node_id, None)

    # ---------- queries ----------
    def get(self, node_id: str) -> Optional[LoadEntry]:
        return self._entries.get(node_id)

    def age(self, entry: LoadEntry, now: Optional[float] = None) -> float:
        return (time.monotonic() if now is None else now) - entry.received_at

    def is_stale(self, entry: LoadEntry, now: Optional[float] = None) -> bool:
        if entry.node_id == self.self_id:
            return False
        return self.age(entry, now) > self.stale_after

    def entries(self, include_stale: bool = False, now: Optional[float] = None) -> List[LoadEntry]:
        return [e for e in self._entries.values()
                if include_stale or not self.is_stale(e, now)]

    def peers(self, include_stale: bool = False) -> List[LoadEntry]:
        return [e for e in self.entries(include_stale) if e.node_id != self.self_id]

    def lowest_cpu(self, exclude: Iterable[str] = (), include_self: bool = True,
                   now: Optional[float] = None) -> Optional[LoadEntry]:
        """Fresh node with the lowest CPU load (ties broken by RAM, then id)."""
        skip = set(exclude)
        if not include_self:
            skip.add(self.self_id)
        cands = [e for e in self.entries(False, now) if e.node_id not in skip]
        if not cands:
            return None
        return min(cands, key=lambda e: (e.cpu, e.ram, e.node_id))

    def snapshot(self, include_stale: bool = True) -> List[dict]:
        """Plain dicts for dashboards (Module: Rich CLI)."""
        now = time.monotonic()
        return [{**e.to_wire(), "age": round(self.age(e, now), 1),
                 "stale": self.is_stale(e, now)}
                for e in sorted(self.entries(include_stale), key=lambda e: e.node_id)]