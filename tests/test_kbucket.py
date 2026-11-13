# tests/test_kbucket.py
from meshweaver.dht.contact import Contact
from meshweaver.dht.kbucket import KBucket

ID_SPACE = 2 ** 160


def make(i):
    return Contact(i, "127.0.0.1", 9000)


def new_bucket(k=20):
    return KBucket(0, ID_SPACE, k)


def test_new_bucket_is_empty():
    b = new_bucket()
    assert len(b) == 0
    assert not b.is_full()
    assert b.least_recently_seen() is None


def test_add_contacts_until_full():
    b = new_bucket(k=3)
    for i in (1, 2, 3):
        assert b.add_contact(make(i)) is True
    assert len(b) == 3
    assert b.is_full()


def test_full_bucket_rejects_new_contact():
    b = new_bucket(k=3)
    for i in (1, 2, 3):
        b.add_contact(make(i))
    assert b.add_contact(make(4)) is False
    assert 4 not in b
    assert len(b) == 3


def test_readding_moves_contact_to_tail():
    b = new_bucket(k=3)
    for i in (1, 2, 3):
        b.add_contact(make(i))
    b.add_contact(make(1))  # node 1 was just seen again
    assert [c.node_id for c in b.get_contacts()] == [2, 3, 1]
    oldest = b.least_recently_seen()
    assert oldest is not None
    assert oldest.node_id == 2


def test_readding_works_even_when_full():
    b = new_bucket(k=2)
    b.add_contact(make(1))
    b.add_contact(make(2))
    assert b.add_contact(make(1)) is True
    assert len(b) == 2


def test_remove_contact():
    b = new_bucket()
    b.add_contact(make(1))
    b.add_contact(make(2))
    assert b.remove(1) is True
    assert b.remove(1) is False
    assert len(b) == 1


def test_covers_range():
    b = KBucket(10, 20)
    assert b.covers(10)
    assert b.covers(19)
    assert not b.covers(20)
    assert not b.covers(9)


def test_split_divides_range_and_contacts():
    b = new_bucket()
    mid = 2 ** 159
    for i in (1, 2, mid + 1, mid + 2):
        b.add_contact(make(i))
    left, right = b.split()
    assert (left.range_min, left.range_max) == (0, mid)
    assert (right.range_min, right.range_max) == (mid, ID_SPACE)
    assert len(left) == 2 and 1 in left and 2 in left
    assert len(right) == 2 and (mid + 1) in right and (mid + 2) in right


def test_split_preserves_order():
    b = new_bucket()
    mid = 2 ** 159
    for i in (5, mid + 5, 3, mid + 3):
        b.add_contact(make(i))
    left, right = b.split()
    assert [c.node_id for c in left.get_contacts()] == [5, 3]
    assert [c.node_id for c in right.get_contacts()] == [mid + 5, mid + 3]