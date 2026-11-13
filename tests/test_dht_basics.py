from meshweaver.dht.node_id import generate_node_id, id_from_key, distance, bucket_index
from meshweaver.dht.contact import Contact

def test_distance_properties():
    a, b = generate_node_id(), generate_node_id()
    assert distance(a, a) == 0
    assert distance(a, b) == distance(b, a)

def test_bucket_index_range():
    a, b = generate_node_id(), generate_node_id()
    assert 0 <= bucket_index(a, b) < 160

def test_key_hash_is_stable():
    assert id_from_key("task-1") == id_from_key(b"task-1")

def test_contact_roundtrip():
    c = Contact(generate_node_id(), "127.0.0.1", 9000)
    assert Contact.from_dict(c.to_dict()) == c