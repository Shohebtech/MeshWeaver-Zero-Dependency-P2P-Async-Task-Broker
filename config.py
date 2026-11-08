"""MeshWeaver configuration.

All timing values for Module 3 (gossip) are derived from one number, the gossip
interval. The PDF requires 5 seconds; tests use a much smaller value.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class GossipConfig:
    interval: float = 5.0            # seconds between load broadcasts (PDF: 5 s)
    stale_after_missed: int = 2      # load data older than 2 intervals is "stale"
    offline_after_missed: int = 3    # no heartbeat/update for 3 intervals => offline
    fanout: int = 3                  # neighbours that receive each gossip push
    max_entries_per_message: int = 20  # keeps UDP datagrams well under 64 KB
    max_nodes: int = 500             # cap on table size (protects against floods)
    series_points: int = 120         # CPU/RAM samples kept per node for charts
    event_history: int = 500         # gossip/heartbeat events kept in memory
    purge_after: float = 600.0       # forget nodes silent for this long (seconds)
    secret: Optional[str] = None     # optional shared secret => HMAC-signed gossip
    max_clock_skew: float = 120.0    # accepted |now - message ts| when secret is set

    @property
    def stale_after(self) -> float:
        return self.interval * self.stale_after_missed

    @property
    def offline_after(self) -> float:
        return self.interval * self.offline_after_missed

    @classmethod
    def from_env(cls) -> "GossipConfig":
        cfg = cls()
        if os.getenv("MESHWEAVER_INTERVAL"):
            cfg.interval = float(os.environ["MESHWEAVER_INTERVAL"])
        if os.getenv("MESHWEAVER_SECRET"):
            cfg.secret = os.environ["MESHWEAVER_SECRET"]
        return cfg


DEFAULT_HTTP_PORT = 8000
DEFAULT_GOSSIP_PORT = 9000
