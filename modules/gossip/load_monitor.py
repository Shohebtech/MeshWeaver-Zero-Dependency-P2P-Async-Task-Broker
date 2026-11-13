"""CPU / RAM monitoring for this node (Module 3, requirement 1 and 2).

Backends, chosen automatically:
  psutil  - used when installed.
  proc    - pure-Python reader of /proc/stat and /proc/meminfo (Linux only).
  loadavg - last resort: os.getloadavg() scaled by CPU count; RAM unavailable (0.0).

The PDF asks whether pure Python is enough for the "zero-dependency" idea; the
``proc`` backend shows it is, on Linux, so psutil stays optional.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional, Tuple

try:  # optional dependency
    import psutil  # type: ignore
except Exception:  # pragma: no cover - depends on environment
    psutil = None


@dataclass
class LoadSample:
    cpu: float        # percent 0-100
    ram: float        # percent 0-100
    ts: float         # unix seconds
    backend: str


def _clamp(v: float) -> float:
    return max(0.0, min(100.0, float(v)))


def parse_proc_stat(text: str) -> Optional[Tuple[int, int]]:
    """Return (busy, total) jiffies from the first 'cpu' line of /proc/stat."""
    for line in text.splitlines():
        if line.startswith("cpu "):
            parts = [int(x) for x in line.split()[1:]]
            if len(parts) < 4:
                return None
            idle = parts[3] + (parts[4] if len(parts) > 4 else 0)  # idle + iowait
            total = sum(parts[:8]) if len(parts) >= 8 else sum(parts)
            return total - idle, total
    return None


def parse_meminfo(text: str) -> Optional[float]:
    """Return RAM used percent from /proc/meminfo text."""
    values = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        bits = rest.split()
        if bits:
            values[key] = int(bits[0])
    total = values.get("MemTotal")
    avail = values.get("MemAvailable", values.get("MemFree"))
    if not total or avail is None:
        return None
    return _clamp((total - avail) * 100.0 / total)


class LoadMonitor:
    def __init__(self, backend: str = "auto"):
        self._prev: Optional[Tuple[int, int]] = None
        if backend == "auto":
            if psutil is not None:
                backend = "psutil"
            elif os.path.exists("/proc/stat") and os.path.exists("/proc/meminfo"):
                backend = "proc"
            else:
                backend = "loadavg"
        self.backend = backend
        if backend == "psutil":
            psutil.cpu_percent(interval=None)  # prime: first call always returns 0.0
        elif backend == "proc":
            self._prev = self._read_stat()

    @staticmethod
    def _read_stat() -> Optional[Tuple[int, int]]:
        with open("/proc/stat") as fh:
            return parse_proc_stat(fh.read())

    def sample(self) -> LoadSample:
        now = time.time()
        if self.backend == "psutil":
            return LoadSample(_clamp(psutil.cpu_percent(interval=None)),
                              _clamp(psutil.virtual_memory().percent), now, "psutil")
        if self.backend == "proc":
            cur = self._read_stat()
            cpu = 0.0
            if cur and self._prev and cur[1] > self._prev[1]:
                cpu = (cur[0] - self._prev[0]) * 100.0 / (cur[1] - self._prev[1])
            self._prev = cur
            with open("/proc/meminfo") as fh:
                ram = parse_meminfo(fh.read()) or 0.0
            return LoadSample(_clamp(cpu), _clamp(ram), now, "proc")
        cores = os.cpu_count() or 1
        return LoadSample(_clamp(os.getloadavg()[0] * 100.0 / cores), 0.0, now, "loadavg")
