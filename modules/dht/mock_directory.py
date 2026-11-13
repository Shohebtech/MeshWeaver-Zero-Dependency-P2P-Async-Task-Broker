"""MOCK ADAPTER for MODULE 2 (Kademlia DHT).

*** Placeholder until the teammate's real DHT is integrated. ***
It is NOT Kademlia: it holds static seed addresses given on the command line plus
peers learned from gossip messages. The PDF notes a real DHT needs one known peer
for first contact; this mock has the same need (one ``--peers`` seed).
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional, Set

from modules.interfaces import PeerAddr


class MockPeerDirectory:
    mock = True
    implementation = "MockPeerDirectory (Module 2 placeholder; not Kademlia)"

    def __init__(self, seeds: Optional[List[PeerAddr]] = None, self_addr: Optional[PeerAddr] = None):
        self._lock = threading.Lock()
        self._seeds: List[PeerAddr] = list(seeds or [])
        self._learned: Dict[PeerAddr, Optional[str]] = {}
        self.self_addr = self_addr
        self._removed: Set[PeerAddr] = set()

    def get_peers(self) -> List[PeerAddr]:
        with self._lock:
            peers = [a for a in dict.fromkeys(self._seeds + list(self._learned)) if a != self.self_addr]
            return peers

    def add_peer(self, addr: PeerAddr, node_id: Optional[str] = None) -> bool:
        addr = (addr[0], int(addr[1]))
        with self._lock:
            if addr == self.self_addr or addr in self._learned or addr in self._seeds:
                if addr in self._learned and node_id:
                    self._learned[addr] = node_id
                return False
            self._learned[addr] = node_id
            return True

    def remove_peer(self, addr: PeerAddr) -> None:
        with self._lock:
            self._learned.pop(addr, None)

    def info(self) -> Dict[str, Any]:
        with self._lock:
            return {"implementation": self.implementation, "mock": self.mock,
                    "seeds": [f"{h}:{p}" for h, p in self._seeds],
                    "learned": [f"{h}:{p}" for h, p in self._learned],
                    "note": "Replace with the real Kademlia DHT (Module 2)."}
