"""Heartbeat monitor: detects peers that stopped gossiping.

Every load update doubles as a heartbeat. A peer whose entry has not been
refreshed (higher seq received) for `interval * missed_threshold` seconds is
declared lost; if fresh data arrives later it is declared recovered.
Callbacks may be plain functions or coroutines: cb(node_id, entry).
"""
import asyncio
import inspect
import time
from typing import Callable, List, Optional, Set

from .table import LoadTable


class HeartbeatMonitor:
    def __init__(self, table: LoadTable, interval: float = 5.0, missed_threshold: int = 3):
        self.table = table
        self.interval = interval
        self.missed_threshold = missed_threshold
        self.dead: Set[str] = set()
        self._lost_cbs: List[Callable] = []
        self._recovered_cbs: List[Callable] = []
        self._task: Optional[asyncio.Task] = None

    @property
    def timeout(self) -> float:
        return self.interval * self.missed_threshold

    def on_lost(self, cb: Callable) -> None:
        self._lost_cbs.append(cb)

    def on_recovered(self, cb: Callable) -> None:
        self._recovered_cbs.append(cb)

    def is_alive(self, node_id: str) -> bool:
        return node_id not in self.dead

    async def check(self, now: Optional[float] = None) -> None:
        """One scan of the table. Public so tests can drive it with fake time."""
        now = time.monotonic() if now is None else now
        for e in self.table.peers(include_stale=True):
            silent_for = self.table.age(e, now)
            if silent_for > self.timeout and e.node_id not in self.dead:
                self.dead.add(e.node_id)
                await self._fire(self._lost_cbs, e)
            elif silent_for <= self.timeout and e.node_id in self.dead:
                self.dead.discard(e.node_id)
                await self._fire(self._recovered_cbs, e)

    async def _fire(self, cbs, entry) -> None:
        for cb in cbs:
            try:
                res = cb(entry.node_id, entry)
                if inspect.isawaitable(res):
                    await res
            except Exception as exc:  # a bad callback must not kill the monitor
                print(f"[heartbeat] callback error: {exc!r}")

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.interval / 2)
            await self.check()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None