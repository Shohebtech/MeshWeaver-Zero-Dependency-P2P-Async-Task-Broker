import asyncio

from meshweaver.dht.network_store import get_value, put_value
from meshweaver.dht.node_id import generate_node_id, id_from_key
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.storage import Storage
from meshweaver.dht.transport import UDPTransport


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


async def make_node(timeout: float = 1.0) -> DHTProtocol:
    transport = UDPTransport("127.0.0.1", 0)
    await transport.start()
    return DHTProtocol(generate_node_id(), transport, timeout=timeout)


async def make_mesh(n: int):
    """n nodes that all know each other."""
    nodes = [await make_node() for _ in range(n)]
    for a in nodes:
        for b in nodes:
            if a is not b:
                a.routing_table.add_contact(b.contact)
    return nodes


def test_store_with_ttl_expires():
    async def scenario():
        clock = FakeClock()
        a, b = await make_node(), await make_node()
        b.storage = Storage(clock=clock)
        key = id_from_key("ttl-test")
        try:
            stored = await a.store(b.contact, key, {"cpu": 1}, ttl=10)
            before = await a.find_value(b.contact, key)
            clock.now += 11
            after = await a.find_value(b.contact, key)
            return stored, before, after
        finally:
            a.close()
            b.close()

    stored, before, after = asyncio.run(scenario())
    assert stored is True
    assert before == (True, {"cpu": 1}, [])
    assert after is not None
    assert after[0] is False        # the value has expired


def test_store_rejects_oversized_value():
    async def scenario():
        a, b = await make_node(), await make_node()
        key = id_from_key("too-big")
        try:
            stored = await a.store(b.contact, key, "x" * 5000)
            return stored, key in b.storage
        finally:
            a.close()
            b.close()

    assert asyncio.run(scenario()) == (False, False)


def test_store_with_invalid_ttl_gets_no_answer():
    async def scenario():
        a, b = await make_node(timeout=0.3), await make_node()
        key = id_from_key("bad-ttl")
        try:
            stored = await a.store(b.contact, key, "v", ttl=-5)
            return stored, key in b.storage
        finally:
            a.close()
            b.close()

    # the peer treats a negative TTL as a malformed request and stays silent
    assert asyncio.run(scenario()) == (False, False)


def test_put_value_replicates_to_every_node():
    async def scenario():
        nodes = await make_mesh(5)
        key = id_from_key("load:node-0")
        try:
            count = await put_value(nodes[0], key, {"cpu": 12.5}, ttl=60)
            return count, [key in n.storage for n in nodes]
        finally:
            for n in nodes:
                n.close()

    count, holders = asyncio.run(scenario())
    assert count == 5               # 4 peers + ourselves
    assert all(holders)


def test_get_value_from_another_node():
    async def scenario():
        nodes = await make_mesh(5)
        outsider = await make_node()
        outsider.routing_table.add_contact(nodes[0].contact)  # knows just one peer
        key = id_from_key("load:node-0")
        try:
            await put_value(nodes[0], key, {"cpu": 12.5}, ttl=60)
            return await get_value(outsider, key)
        finally:
            for n in nodes:
                n.close()
            outsider.close()

    assert asyncio.run(scenario()) == (True, {"cpu": 12.5})


def test_put_and_get_on_a_lonely_node():
    async def scenario():
        node = await make_node()
        key = id_from_key("alone")
        try:
            count = await put_value(node, key, "hello")
            return count, await get_value(node, key)
        finally:
            node.close()

    # nobody else exists, so the only copy is our own
    assert asyncio.run(scenario()) == (1, (True, "hello"))


def test_get_missing_value():
    async def scenario():
        nodes = await make_mesh(3)
        try:
            return await get_value(nodes[0], id_from_key("never-stored"))
        finally:
            for n in nodes:
                n.close()

    assert asyncio.run(scenario()) == (False, None)


def test_value_expires_across_the_mesh():
    async def scenario():
        clock = FakeClock()
        nodes = await make_mesh(5)
        outsider = await make_node()
        outsider.routing_table.add_contact(nodes[0].contact)
        for n in nodes + [outsider]:
            n.storage = Storage(clock=clock)
        key = id_from_key("short-lived")
        try:
            await put_value(nodes[0], key, "v", ttl=10)
            before = await get_value(outsider, key)
            clock.now += 11
            after = await get_value(outsider, key)
            return before, after
        finally:
            for n in nodes:
                n.close()
            outsider.close()

    before, after = asyncio.run(scenario())
    assert before == (True, "v")
    assert after == (False, None)