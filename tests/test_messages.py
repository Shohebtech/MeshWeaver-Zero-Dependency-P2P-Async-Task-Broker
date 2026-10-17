import json

import pytest

from meshweaver.dht.contact import Contact
from meshweaver.dht.messages import (
    PING, FIND_NODE, STORE, FIND_VALUE, MessageError,
    make_request, make_response, encode, decode,
    id_to_hex, hex_to_id, contacts_to_payload, contacts_from_payload,
)

ME = Contact(1, "127.0.0.1", 9000)
OTHER = Contact(2, "127.0.0.1", 9001)


def tamper(message, **changes):
    obj = json.loads(encode(message))
    obj.update(changes)
    return json.dumps(obj).encode()


def test_request_ids_are_unique():
    ids = {make_request(PING, ME).msg_id for _ in range(100)}
    assert len(ids) == 100


def test_ping_roundtrip():
    m = make_request(PING, ME)
    assert decode(encode(m)) == m


def test_find_node_roundtrip():
    m = make_request(FIND_NODE, ME, {"target": id_to_hex(12345)})
    got = decode(encode(m))
    assert got == m
    assert hex_to_id(got.payload["target"]) == 12345


def test_store_roundtrip():
    m = make_request(STORE, ME, {"key": id_to_hex(7), "value": {"cpu": 12.5, "tags": [1, 2]}, "ttl": 60})
    assert decode(encode(m)) == m


def test_find_value_response_with_contacts():
    contacts = [Contact(1, "10.0.0.1", 9001), Contact(2, "10.0.0.2", 9002)]
    req = make_request(FIND_VALUE, ME, {"key": id_to_hex(5)})
    resp = make_response(req, OTHER, {"contacts": contacts_to_payload(contacts)})
    got = decode(encode(resp))
    assert got.is_response
    assert contacts_from_payload(got.payload["contacts"]) == contacts


def test_response_keeps_request_id_and_type():
    req = make_request(FIND_NODE, ME, {"target": id_to_hex(9)})
    resp = make_response(req, OTHER, {"contacts": []})
    assert resp.msg_id == req.msg_id
    assert resp.type == req.type
    assert resp.is_response and not req.is_response


def test_decode_rejects_garbage():
    with pytest.raises(MessageError):
        decode(b"this is not json")
    with pytest.raises(MessageError):
        decode(b"\xff\xfe\x00")


def test_decode_rejects_unknown_type():
    with pytest.raises(MessageError):
        decode(tamper(make_request(PING, ME), type="EXPLODE"))
    with pytest.raises(MessageError):
        make_request("EXPLODE", ME)


def test_decode_rejects_missing_fields():
    with pytest.raises(MessageError):
        decode(json.dumps({"v": 1, "type": "PING"}).encode())


def test_decode_rejects_bad_sender():
    m = make_request(PING, ME)
    bad_id = {"id": "f" * 41, "host": "h", "port": 1}   
    bad_port = {"id": id_to_hex(1), "host": "h", "port": 70000}
    for sender in (bad_id, bad_port, "nobody"):
        with pytest.raises(MessageError):
            decode(tamper(m, sender=sender))


def test_encode_rejects_oversized_message():
    m = make_request(STORE, ME, {"key": id_to_hex(1), "value": "x" * 10000})
    with pytest.raises(MessageError):
        encode(m)


def test_hex_id_roundtrip_and_range_check():
    assert hex_to_id(id_to_hex(12345)) == 12345
    with pytest.raises(MessageError):
        hex_to_id("f" * 41)