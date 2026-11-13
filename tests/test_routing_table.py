from meshweaver.dht.contact import Contact
from meshweaver.dht.routing_table import RoutingTable, ID_SPACE

FAR = 2 ** 159 


def make(i):
    return Contact(i, "127.0.0.1", 9000)


def test_new_table_is_empty():
    t = RoutingTable(own_id=0)
    assert len(t) == 0
    assert len(t.buckets) == 1


def test_add_and_find_contact():
    t = RoutingTable(own_id=0)
    c = make(10)
    assert t.add_contact(c) is True
    assert 10 in t
    assert t.get(10) == c


def test_own_id_is_never_added():
    t = RoutingTable(own_id=5)
    assert t.add_contact(make(5)) is False
    assert len(t) == 0


def test_readding_same_contact_does_not_duplicate():
    t = RoutingTable(own_id=0)
    t.add_contact(make(10))
    t.add_contact(make(10))
    assert len(t) == 1


def test_remove_contact():
    t = RoutingTable(own_id=0)
    t.add_contact(make(10))
    assert t.remove_contact(10) is True
    assert t.remove_contact(10) is False
    assert 10 not in t


def test_close_bucket_splits_when_full():
    t = RoutingTable(own_id=0, k=2)
    for i in (1, 2, 3):
        assert t.add_contact(make(i)) is True
    assert len(t) == 3
    assert len(t.buckets) > 1


def test_far_bucket_does_not_split():
    t = RoutingTable(own_id=0, k=2)
    assert t.add_contact(make(FAR + 1)) is True
    assert t.add_contact(make(FAR + 2)) is True
    assert t.add_contact(make(FAR + 3)) is False 
    assert len(t) == 2
    assert len(t.buckets) == 2 


def test_eviction_candidate_for_full_far_bucket():
    t = RoutingTable(own_id=0, k=2)
    t.add_contact(make(FAR + 1))
    t.add_contact(make(FAR + 2))
    newcomer = make(FAR + 3)
    t.add_contact(newcomer)
    oldest = t.get_eviction_candidate(newcomer)
    assert oldest is not None
    assert oldest.node_id == FAR + 1


def test_find_closest_uses_xor_not_numeric_order():
    t = RoutingTable(own_id=0)
    for i in (10, 20, 30, 40):
        t.add_contact(make(i))
   
    result = t.find_closest(16, n=3)
    assert [c.node_id for c in result] == [20, 30, 10]


def test_find_closest_respects_n_and_exclude():
    t = RoutingTable(own_id=0)
    for i in (10, 20, 30, 40):
        t.add_contact(make(i))
    assert len(t.find_closest(16, n=2)) == 2
    result = t.find_closest(16, n=2, exclude={20})
    assert [c.node_id for c in result] == [30, 10]


def test_find_closest_on_empty_table():
    assert RoutingTable(own_id=0).find_closest(123) == []


def test_buckets_always_cover_whole_id_space():
    t = RoutingTable(own_id=0, k=2)
    for i in (1, 2, 3, 4, 5, FAR + 1, FAR + 2):
        t.add_contact(make(i))
    assert t.buckets[0].range_min == 0
    assert t.buckets[-1].range_max == ID_SPACE
    for a, b in zip(t.buckets, t.buckets[1:]):
        assert a.range_max == b.range_min  