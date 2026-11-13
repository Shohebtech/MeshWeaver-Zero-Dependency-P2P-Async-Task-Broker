# tests/test_protocol.py
import asyncio
import socket

from meshweaver.dht.contact import Contact
from meshweaver.dht.node_id import generate_node_id, id_from_key
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport


async def make_node(timeout: float = 1.0) -> DHTProtocol:
    transport = UDPTransport("127.0.0.1", 0)  # port 0 = any free port
    await transport.start()
    return DHTProtocol(generate_node_id(), transport, timeout=timeout)


def test_ping_between_two_nodes():
    async def scenario():
        a, b = await make_node(), await make_node()
        try:
            ok = await a.ping(b.contact)
            return ok, a.node_id in b.routing_table, b.node_id in a.routing_table
        finally:
            a.close()
            b.close()

    # ping worked, and each node learned about the other
    assert asyncio.run(scenario()) == (True, True, True)


def test_ping_silent_peer_times_out():
    silent = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    silent.bind(("127.0.0.1", 0))  # bound but never answers
    port = silent.getsockname()[1]

    async def scenario():
        a = await make_node(timeout=0.2)
        try:
            return await a.ping(Contact(generate_node_id(), "127.0.0.1", port))
        finally:
            a.close()

    try:
        assert asyncio.run(scenario()) is False
    finally:
        silent.close()


def test_find_node_returns_closest_contacts():
    async def scenario():
        a, b = await make_node(), await make_node()
        try:
            for i in (1, 2, 3, 4, 5):
                b.routing_table.add_contact(Contact(i, "127.0.0.1", 9000 + i))
            result = await a.find_node(b.contact, 3)
            return [c.node_id for c in result] if result is not None else None
        finally:
            a.close()
            b.close()

    # XOR distances to 3: 3->0, 2->1, 1->2, 5->6, 4->7
    # (a itself is excluded from the answer)
    assert asyncio.run(scenario()) == [3, 2, 1, 5, 4]


def test_store_then_find_value():
    async def scenario():
        a, b = await make_node(), await make_node()
        try:
            key = id_from_key("node-1-load")
            stored = await a.store(b.contact, key, {"cpu": 5})
            found = await a.find_value(b.contact, key)
            return stored, found, b.storage.get(key)
        finally:
            a.close()
            b.close()

    stored, found, in_storage = asyncio.run(scenario())
    assert stored is True
    assert found == (True, {"cpu": 5}, [])
    assert in_storage == {"cpu": 5}


def test_find_value_missing_returns_contacts():
    async def scenario():
        a, b = await make_node(), await make_node()
        try:
            for i in (1, 2, 3):
                b.routing_table.add_contact(Contact(i, "127.0.0.1", 9000 + i))
            return await a.find_value(b.contact, id_from_key("nobody-stored-this"))
        finally:
            a.close()
            b.close()

    result = asyncio.run(scenario())
    assert result is not None
    found, value, contacts = result
    assert found is False
    assert value is None
    assert {c.node_id for c in contacts} == {1, 2, 3}


def test_garbage_datagram_is_ignored():
    async def scenario():
        a, b = await make_node(), await make_node()
        try:
            a.transport.send(b"this is not a dht message", b.contact.addr)
            await asyncio.sleep(0.1)
            known_after_junk = len(b.routing_table)
            still_works = await a.ping(b.contact)
            return known_after_junk, still_works
        finally:
            a.close()
            b.close()

    # junk added nobody to the table and did not crash the node
    assert asyncio.run(scenario()) == (0, True)


def test_node_contact_uses_bound_port():
    async def scenario():
        node = await make_node()
        try:
            return node.contact
        finally:
            node.close()

    c = asyncio.run(scenario())
    assert c.host == "127.0.0.1"
    assert c.port != 0  # the real port the OS gave us, not the 0 we asked for