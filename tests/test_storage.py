import pytest

from meshweaver.dht.storage import Storage


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_set_and_get():
    s = Storage()
    assert s.set(1, {"cpu": 5}) is True
    assert s.get(1) == {"cpu": 5}
    assert 1 in s


def test_missing_key_returns_default():
    s = Storage()
    assert s.get(99) is None
    assert s.get(99, "nope") == "nope"
    assert 99 not in s


def test_value_expires_after_ttl():
    clock = FakeClock()
    s = Storage(clock=clock)
    s.set(1, "a", ttl=10)
    clock.now += 9.9
    assert s.get(1) == "a"
    clock.now += 0.2
    assert s.get(1) is None
    assert 1 not in s


def test_default_ttl_is_used():
    clock = FakeClock()
    s = Storage(default_ttl=5, clock=clock)
    s.set(1, "a")
    clock.now += 4
    assert 1 in s
    clock.now += 2
    assert 1 not in s


def test_ttl_is_capped_at_max():
    clock = FakeClock()
    s = Storage(max_ttl=10, clock=clock)
    assert s.set(1, "a", ttl=1000) is True
    clock.now += 11
    assert 1 not in s


def test_invalid_ttl_is_rejected():
    s = Storage()
    for bad in (0, -1, True, "5", float("nan")):
        assert s.set(1, "a", ttl=bad) is False #type: ignore 
    assert 1 not in s


def test_overwrite_refreshes_expiry():
    clock = FakeClock()
    s = Storage(clock=clock)
    s.set(1, "old", ttl=10)
    clock.now += 8
    s.set(1, "new", ttl=10)
    clock.now += 8          # 16 s after the first set, 8 s after the second
    assert s.get(1) == "new"


def test_oversized_value_is_rejected():
    s = Storage(max_value_bytes=20)
    assert s.set(1, "x" * 100) is False
    assert s.set(2, "ok") is True
    assert 1 not in s


def test_non_json_value_is_rejected():
    s = Storage()
    assert s.set(1, object()) is False
    assert 1 not in s


def test_max_items_enforced():
    s = Storage(max_items=2)
    assert s.set(1, "a") is True
    assert s.set(2, "b") is True
    assert s.set(3, "c") is False       # full, and 3 is a new key
    assert s.set(1, "a2") is True       # overwriting an existing key is fine
    assert len(s) == 2


def test_full_store_reclaims_expired_items():
    clock = FakeClock()
    s = Storage(max_items=2, clock=clock)
    s.set(1, "a", ttl=5)
    s.set(2, "b", ttl=5)
    clock.now += 6
    assert s.set(3, "c") is True        # the two expired items made room
    assert len(s) == 1


def test_purge_expired_counts_removed():
    clock = FakeClock()
    s = Storage(clock=clock)
    s.set(1, "a", ttl=5)
    s.set(2, "b", ttl=5)
    s.set(3, "c", ttl=100)
    clock.now += 10
    assert s.purge_expired() == 2
    assert 3 in s


def test_len_ignores_expired():
    clock = FakeClock()
    s = Storage(clock=clock)
    s.set(1, "a", ttl=5)
    s.set(2, "b", ttl=100)
    clock.now += 10
    assert len(s) == 1


def test_remaining_ttl():
    clock = FakeClock()
    s = Storage(clock=clock)
    s.set(1, "a", ttl=10)
    clock.now += 4
    assert s.remaining_ttl(1) == pytest.approx(6.0)
    clock.now += 7
    assert s.remaining_ttl(1) is None
    assert s.remaining_ttl(404) is None


def test_dict_style_access():
    s = Storage(max_value_bytes=10)
    s[1] = "a"
    assert s[1] == "a"
    with pytest.raises(KeyError):
        _ = s[2]
    with pytest.raises(ValueError):
        s[3] = "x" * 100


def test_none_value_is_a_real_value():
    s = Storage()
    s.set(1, None)
    assert 1 in s
    assert s[1] is None