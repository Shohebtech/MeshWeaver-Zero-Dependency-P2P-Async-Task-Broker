import asyncio
import inspect
import random
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from .contact import Contact
from .lookup import iterative_find_node
from .network_store import put_value


class Maintenance:
    """
    Keeps one DHT node healthy. Each job can be called by hand (easy to test)
    or run forever in the background with start() and stop().
    `node` is a DHTProtocol.
    """

    def __init__(self, node: Any,
                 ping_interval: float = 30.0,
                 refresh_interval: float = 600.0,
                 purge_interval: float = 60.0,
                 republish_interval: float = 1800.0,
                 liveness_per_bucket: int = 3,
                 clock: Callable[[], float] = time.monotonic):
        for value in (ping_interval, refresh_interval, purge_interval, republish_interval):
            if value <= 0:
                raise ValueError("intervals must be positive")
        self.node = node
        self.ping_interval = ping_interval
        self.refresh_interval = refresh_interval
        self.purge_interval = purge_interval
        self.republish_interval = republish_interval
        self.liveness_per_bucket = liveness_per_bucket
        self._clock = clock  # tests replace this with a fake clock
        self._last_refresh: Dict[Tuple[int, int], float] = {}
        self._published: Dict[int, Tuple[Any, Optional[float]]] = {}
        self._tasks: List["asyncio.Task[None]"] = []

    # ------------------------------------------------------------------
    # 1. Dead-node eviction
    # ------------------------------------------------------------------
    async def check_liveness(self, per_bucket: Optional[int] = None) -> List[Contact]:
        """
        Ping the least recently seen peers of every bucket (all pings at once).
        Peers that do not answer are removed. Returns the removed contacts.
        """
        count = self.liveness_per_bucket if per_bucket is None else per_bucket
        suspects: List[Contact] = []
        for bucket in list(self.node.routing_table.buckets):
            suspects.extend(bucket.get_contacts()[:count])
        if not suspects:
            return []
        answers = await asyncio.gather(*(self.node.ping(c) for c in suspects))
        removed: List[Contact] = []
        for contact, alive in zip(suspects, answers):
            if not alive and self.node.routing_table.remove_contact(contact.node_id):
                removed.append(contact)
        return removed

    # ------------------------------------------------------------------
    # 2. Bucket refresh
    # ------------------------------------------------------------------
    async def refresh_buckets(self, force: bool = False) -> int:
        """
        For every bucket not refreshed recently, search for a random ID inside
        its range. Returns how many buckets were refreshed.
        """
        now = self._clock()
        due: List[Tuple[int, int]] = []
        for bucket in list(self.node.routing_table.buckets):
            key = (bucket.range_min, bucket.range_max)
            last = self._last_refresh.get(key)
            if force or last is None or now - last >= self.refresh_interval:
                due.append(key)
        for low, high in due:
            await iterative_find_node(self.node, random.randrange(low, high))
            self._last_refresh[(low, high)] = self._clock()
        return len(due)

    # ------------------------------------------------------------------
    # 3. Storage sweep
    # ------------------------------------------------------------------
    def purge_storage(self) -> int:
        """Delete expired values. Returns how many were removed."""
        return self.node.storage.purge_expired()

    # ------------------------------------------------------------------
    # 4. Publish and republish
    # ------------------------------------------------------------------
    async def publish(self, key: int, value: Any, ttl: Optional[float] = None) -> int:
        """Store a value on the closest nodes AND remember it for republishing."""
        count = await put_value(self.node, key, value, ttl)
        self._published[key] = (value, ttl)  # remembered even if count is 0
        return count

    def unpublish(self, key: int) -> bool:
        """Stop republishing a key (copies on other nodes just expire)."""
        return self._published.pop(key, None) is not None

    @property
    def published_keys(self) -> List[int]:
        return list(self._published)

    async def republish_all(self) -> int:
        """Store every published value again. Returns the total copies accepted."""
        total = 0
        for key, (value, ttl) in list(self._published.items()):
            total += await put_value(self.node, key, value, ttl)
        return total

    # ------------------------------------------------------------------
    # Background running
    # ------------------------------------------------------------------
    @property
    def running(self) -> bool:
        return bool(self._tasks)

    def start(self) -> None:
        """Start all four jobs in the background. Call from inside asyncio."""
        if self._tasks:
            return
        loop = asyncio.get_running_loop()
        jobs: List[Tuple[float, Callable[[], Any]]] = [
            (self.ping_interval, self.check_liveness),
            (self.refresh_interval, lambda: self.refresh_buckets(force=True)),
            (self.purge_interval, self.purge_storage),
            (self.republish_interval, self.republish_all),
        ]
        self._tasks = [loop.create_task(self._loop(i, j)) for i, j in jobs]

    async def stop(self) -> None:
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _loop(self, interval: float, job: Callable[[], Any]) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                result = job()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass  # one failed round must never kill the background job