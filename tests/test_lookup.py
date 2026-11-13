import asyncio

from meshweaver.dht.contact import Contact
from meshweaver.dht.lookup import iterative_find_node, iterative_find_value
from meshweaver.dht.node_id import generate_node_id, id_from_key
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.routing_table import RoutingTable
from meshweaver.dht.transport import UDPTransport


def make(i):
    return Contact(i, "127.0.0.1", 9000)


class FakeNode:
    

    def __init__(self, network, dead=(), values=None, delay=0.0):
        self.node_id = 0
        self.k = 20
        self.routing_table = RoutingTable(self.node_id)
        self.network = network
        self.dead = set(dead)
        self.values = values or {}   # {peer_id: {key: value}}
        self.delay = delay
        self.calls = []              # every peer we asked, in order
        self.active = 0
        self.max_active = 0          # most requests in flight at once

    def know(self, *ids):
        for i in ids:
            self.routing_table.add_contact(make(i))

    async def _ask(self, contact, target):
        self.calls.append(contact.node_id)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delay)
            if contact.node_id in self.dead:
                return None
            known = sorted(self.network.get(contact.node_id, []),
                           key=lambda i: i ^ target)
            return [make(i) for i in known[:20]]
        finally:
            self.active -= 1

    async def find_node(self, contact, target):
        return await self._ask(contact, target)

    async def find_value(self, contact, key):
        contacts = await self._ask(contact, key)
        if contacts is None:
            return None
        stored = self.values.get(contact.node_id, {})
        if key in stored:
            return (True, stored[key], [])
        return (False, None, contacts)


async def make_node(timeout: float = 1.0) -> DHTProtocol:
    transport = UDPTransport("127.0.0.1", 0)
    await transport.start()
    return DHTProtocol(generate_node_id(), transport, timeout=timeout)


async def make_chain(length: int):
    """nodes[0] knows nodes[1], nodes[1] knows nodes[2], and so on."""
    nodes = [await make_node() for _ in range(length)]
    for a, b in zip(nodes, nodes[1:]):
        a.routing_table.add_contact(b.contact)
    return nodes


# ---------------- fake-network tests (fast, deterministic) ----------------

def test_lookup_follows_a_chain_of_peers():
    node = FakeNode({i: [i + 1] for i in range(1, 10)})  # 1 knows 2, 2 knows 3, ...
    node.know(1)                                          # we only know node 1
    result = asyncio.run(iterative_find_node(node, 10))
    ids = [c.node_id for c in result]
    assert set(ids) == set(range(1, 11))
    assert ids[0] == 10                                   # closest to target 10 is 10 itself
    assert ids == sorted(ids, key=lambda i: i ^ 10)       # nearest first, by XOR
    assert len(node.calls) == len(set(node.calls))        # nobody asked twice


def test_asks_alpha_peers_in_parallel():
    node = FakeNode({}, delay=0.05)
    node.know(1, 2, 3, 4, 5, 6)
    asyncio.run(iterative_find_node(node, 100, alpha=3))
    assert node.max_active == 3
    assert len(node.calls) == 6


def test_alpha_one_asks_one_at_a_time():
    node = FakeNode({}, delay=0.01)
    node.know(1, 2, 3, 4)
    asyncio.run(iterative_find_node(node, 100, alpha=1))
    assert node.max_active == 1
    assert len(node.calls) == 4


def test_dead_peers_are_skipped():
    node = FakeNode({}, dead={2, 4})
    node.know(1, 2, 3, 4, 5)
    result = asyncio.run(iterative_find_node(node, 100))
    assert {c.node_id for c in result} == {1, 3, 5}
    assert sorted(node.calls) == [1, 2, 3, 4, 5]          # dead ones were tried once


def test_own_id_in_answers_is_ignored():
    node = FakeNode({1: [0, 2]})                          # node 1 reveals our own ID (0)
    node.know(1)
    result = asyncio.run(iterative_find_node(node, 5))
    assert {c.node_id for c in result} == {1, 2}
    assert 0 not in node.calls


def test_empty_routing_table_gives_empty_result():
    node = FakeNode({})
    assert asyncio.run(iterative_find_node(node, 123)) == []


def test_far_peers_are_not_queried():
    # target 1, k=2. Node 100 reveals 2 and 3 (very close) and 200/300/400 (far).
    node = FakeNode({100: [2, 3, 200, 300, 400]})
    node.know(100)
    result = asyncio.run(iterative_find_node(node, 1, k=2))
    assert [c.node_id for c in result] == [3, 2]
    assert set(node.calls) == {100, 2, 3}
    assert not {200, 300, 400} & set(node.calls)


def test_find_value_stops_when_value_found():
    key = 7
    node = FakeNode({1: [2, 3, 4, 5, 6]}, values={6: {key: "hello"}})
    node.know(1)
    found, value, _ = asyncio.run(iterative_find_value(node, key))
    assert found is True
    assert value == "hello"
    assert 2 not in node.calls and 3 not in node.calls    # no need to ask further


def test_find_value_not_found_returns_closest():
    node = FakeNode({1: [2, 3]})
    node.know(1)
    found, value, closest = asyncio.run(iterative_find_value(node, 9))
    assert found is False
    assert value is None
    assert {c.node_id for c in closest} == {1, 2, 3}


# ---------------- real UDP tests ----------------

def test_real_udp_find_node_through_chain():
    async def scenario():
        nodes = await make_chain(5)
        try:
            result = await iterative_find_node(nodes[0], generate_node_id())
            return nodes, result
        finally:
            for n in nodes:
                n.close()

    nodes, result = asyncio.run(scenario())
    # nodes[0] started out knowing only nodes[1], yet found all four others
    assert {c.node_id for c in result} == {n.node_id for n in nodes[1:]}


def test_real_udp_find_value_through_chain():
    async def scenario():
        nodes = await make_chain(5)
        key = id_from_key("chain-value")
        nodes[-1].storage[key] = {"cpu": 7}               # only the far end has it
        try:
            return await iterative_find_value(nodes[0], key)
        finally:
            for n in nodes:
                n.close()

    found, value, _ = asyncio.run(scenario())
    assert found is True
    assert value == {"cpu": 7}