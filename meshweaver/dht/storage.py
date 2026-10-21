import json
import time
from typing import Any, Callable, Dict, Optional, Tuple

DEFAULT_TTL = 3600.0      # seconds a value lives if the sender gives no TTL
MAX_TTL = 86400.0         # nobody can make a value live longer than 24 hours
MAX_ITEMS = 1000          # a node never holds more than this many values
MAX_VALUE_BYTES = 4096    # one value must fit comfortably inside an 8 KB message

MISSING = object()        # "no value here", different from a stored None


class Storage:
    """
    A key/value store where every value expires. Works like a small dict:
    storage[key] = value, key in storage, storage.get(key).
    Expired values are removed when touched, or by purge_expired().
    """

    def __init__(self, max_items: int = MAX_ITEMS,
                 max_value_bytes: int = MAX_VALUE_BYTES,
                 default_ttl: float = DEFAULT_TTL,
                 max_ttl: float = MAX_TTL,
                 clock: Callable[[], float] = time.monotonic):
        self.max_items = max_items
        self.max_value_bytes = max_value_bytes
        self.default_ttl = default_ttl
        self.max_ttl = max_ttl
        self._clock = clock  # tests replace this with a fake clock
        self._items: Dict[int, Tuple[Any, float]] = {}  # key -> (value, expires_at)

    # ---- writing ----
    def set(self, key: int, value: Any, ttl: Optional[float] = None) -> bool:
        """True if stored. False if the TTL is invalid, the value is too big
        or not JSON-safe, or the store is full."""
        if ttl is None:
            ttl = self.default_ttl
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
            return False
        if ttl != ttl or ttl <= 0:      # NaN or zero/negative
            return False
        ttl = min(float(ttl), self.max_ttl)
        try:
            # same encoding the wire uses, so "fits here" means "fits in a message"
            size = len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
        except (TypeError, ValueError):
            return False
        if size > self.max_value_bytes:
            return False
        if key not in self._items and len(self._items) >= self.max_items:
            self.purge_expired()
            if len(self._items) >= self.max_items:
                return False
        self._items[key] = (value, self._clock() + ttl)
        return True

    def purge_expired(self) -> int:
        """Delete every expired value. Returns how many were removed."""
        now = self._clock()
        dead = [k for k, (_, expires) in self._items.items() if expires <= now]
        for k in dead:
            del self._items[k]
        return len(dead)

    # ---- reading ----
    def get(self, key: int, default: Any = None) -> Any:
        entry = self._items.get(key)
        if entry is None:
            return default
        value, expires = entry
        if expires <= self._clock():
            del self._items[key]
            return default
        return value

    def remaining_ttl(self, key: int) -> Optional[float]:
        """Seconds left, or None if the key is missing or expired."""
        entry = self._items.get(key)
        if entry is None:
            return None
        left = entry[1] - self._clock()
        if left <= 0:
            del self._items[key]
            return None
        return left

    # ---- dict-style access ----
    def __contains__(self, key: int) -> bool:
        return self.get(key, MISSING) is not MISSING

    def __getitem__(self, key: int) -> Any:
        value = self.get(key, MISSING)
        if value is MISSING:
            raise KeyError(key)
        return value

    def __setitem__(self, key: int, value: Any) -> None:
        if not self.set(key, value):
            raise ValueError("value rejected (too big, not JSON-safe, or store full)")

    def __len__(self) -> int:
        self.purge_expired()
        return len(self._items)