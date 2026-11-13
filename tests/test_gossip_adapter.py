import asyncio
import time

import pytest

from meshweaver.dht.contact import Contact
from meshweaver.dht.node import DHTNode
from meshweaver.dht.node_id import generate_node_id
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport
from meshweaver.integration.dispatch import Dispatcher
from meshweaver.integration.gossip_adapter import (
    GossipTransport, expire_entry, gossip_id, link_departures, neighbor_provider,
)


async def make_dispatcher():
    transport = UDPTransport("127.0.0.1", 0)
    await transport.start()
    return Dispatcher(transport)


async def close_all(adapters, dispatchers):
    for adapter in adapters:
        await adapter.stop()
    for dispatcher in dispatchers:
        dispatcher.close()


# ---------------- fakes (no network, no gossip package needed) ----------------

class FakeDHT:
    def __init__(self, contacts, running=True):
        self.contacts = contacts
        self.is_running = running
        self.asked = []
        self.left_callbacks = []

    def get_neighbors(self, n=None):
        self.asked.append(n)
        return self.contacts if n is None else self.contacts[:n]

    def on_peer_left(self, callback):
        self.left_callbacks.append(callback)

        def off():
            if callback in self.left_callbacks:
                self.left_callbacks.remove(callback)

        return off


class FakeEntry:
    def __init__(self):
        self.received_at = time.monotonic()


class FakeTable:
    def __init__(self, entries):
        self.entries = entries

    def get(self, peer_id):
        return self.entries.get(peer_id)


class FakeHeartbeat:
    timeout = 15.0


class FakeEngine:
    def __init__(self, entries):
        self.table = FakeTable(entries)
        self.heartbeat = FakeHeartbeat()


# ---------------- GossipTransport ----------------

def test_async_handler_receives_message_and_sender_address():
    async def scenario():
        a, b = await make_dispatcher(), await make_dispatcher()
        ga, gb = GossipTransport(a), GossipTransport(b)
        seen = []

        async def handler(msg, addr):
            seen.append((msg, addr))

        gb.register_handler("LOAD_UPDATE", handler)
        try:
            await ga.send(b.local_addr, {"type": "LOAD_UPDATE", "n": 1})
            await asyncio.sleep(0.1)
            return seen, a.local_addr
        finally:
            await close_all([ga, gb], [a, b])

    seen, sender = asyncio.run(scenario())
    assert seen == [({"type": "LOAD_UPDATE", "n": 1}, sender)]


def test_plain_function_handler_also_works():
    async def scenario():
        a, b = await make_dispatcher(), await make_dispatcher()
        ga, gb = GossipTransport(a), GossipTransport(b)
        seen = []
        gb.register_handler("LOAD_UPDATE", lambda msg, addr: seen.append(msg))
        try:
            await ga.send(b.local_addr, {"type": "LOAD_UPDATE", "n": 2})
            await asyncio.sleep(0.1)
            return seen
        finally:
            await close_all([ga, gb], [a, b])

    assert asyncio.run(scenario()) == [{"type": "LOAD_UPDATE", "n": 2}]


def test_failing_handlers_do_not_break_the_socket():
    async def scenario():
        a, b = await make_dispatcher(), await make_dispatcher()
        ga, gb = GossipTransport(a), GossipTransport(b)
        good = []

        async def bad_async(msg, addr):
            raise RuntimeError("boom")

        def bad_sync(msg, addr):
            raise RuntimeError("boom")

        gb.register_handler("BAD_ASYNC", bad_async)
        gb.register_handler("BAD_SYNC", bad_sync)
        gb.register_handler("GOOD", lambda msg, addr: good.append(msg))
        try:
            await ga.send(b.local_addr, {"type": "BAD_ASYNC"})
            await ga.send(b.local_addr, {"type": "BAD_SYNC"})
            await ga.send(b.local_addr, {"type": "GOOD"})
            await asyncio.sleep(0.15)
            return good
        finally:
            await close_all([ga, gb], [a, b])

    assert asyncio.run(scenario()) == [{"type": "GOOD"}]


def test_oversized_message_is_refused():
    async def scenario():
        a = await make_dispatcher()
        ga = GossipTransport(a)
        try:
            with pytest.raises(ValueError):
                await ga.send(("127.0.0.1", 9), {"type": "LOAD_UPDATE", "pad": "x" * 9000})
        finally:
            await close_all([ga], [a])

    asyncio.run(scenario())


def test_dht_message_types_cannot_be_registered():
    async def scenario():
        a = await make_dispatcher()
        ga = GossipTransport(a)
        try:
            with pytest.raises(ValueError):
                ga.register_handler("PING", lambda msg, addr: None)
            return a.routes
        finally:
            await close_all([ga], [a])

    assert asyncio.run(scenario()) == []


def test_stop_removes_routes_but_keeps_the_socket_open():
    async def scenario():
        a = await make_dispatcher()
        ga = GossipTransport(a)
        ga.register_handler("LOAD_UPDATE", lambda msg, addr: None)
        before = a.routes
        await ga.stop()
        after = a.routes
        a.close()
        return before, after

    assert asyncio.run(scenario()) == (["LOAD_UPDATE"], [])


def test_dht_still_works_next_to_the_gossip_transport():
    async def scenario():
        da, db = await make_dispatcher(), await make_dispatcher()
        a = DHTProtocol(generate_node_id(), da, timeout=1.0)
        b = DHTProtocol(generate_node_id(), db, timeout=1.0)
        ga, gb = GossipTransport(da), GossipTransport(db)
        seen = []
        gb.register_handler("LOAD_UPDATE", lambda msg, addr: seen.append(msg))
        try:
            await ga.send(db.local_addr, {"type": "LOAD_UPDATE", "cpu": 3})
            await asyncio.sleep(0.1)
            return seen, await a.ping(b.contact)
        finally:
            await close_all([ga, gb], [da, db])

    assert asyncio.run(scenario()) == ([{"type": "LOAD_UPDATE", "cpu": 3}], True)


# ---------------- neighbor provider ----------------

def test_gossip_id_is_forty_hex_characters():
    text = gossip_id(255)
    assert text == "0" * 38 + "ff"
    assert len(text) == 40
    assert int(text, 16) == 255


def test_provider_converts_contacts_to_gossip_tuples():
    dht = FakeDHT([Contact(255, "10.0.0.1", 9001), Contact(2, "10.0.0.2", 9002)])
    assert neighbor_provider(dht)() == [
        ("0" * 38 + "ff", "10.0.0.1", 9001),
        ("0" * 39 + "2", "10.0.0.2", 9002),
    ]


def test_provider_passes_the_limit_on():
    dht = FakeDHT([Contact(1, "h", 1), Contact(2, "h", 2), Contact(3, "h", 3)])
    assert len(neighbor_provider(dht, limit=2)()) == 2
    assert dht.asked == [2]


def test_provider_returns_nothing_while_the_node_is_stopped():
    dht = FakeDHT([Contact(1, "h", 1)], running=False)
    assert neighbor_provider(dht)() == []
    assert dht.asked == []


def test_provider_with_real_dht_nodes():
    async def scenario():
        a = DHTNode(ports=range(47850, 47860), probe_timeout=0.3, rpc_timeout=0.5,
                    maintenance=False)
        b = DHTNode(ports=range(47850, 47860), probe_timeout=0.3, rpc_timeout=0.5,
                    maintenance=False)
        await a.start()
        await b.start()
        provide = neighbor_provider(a)
        try:
            running = provide()
            b_contact = b.contact
            await a.stop()
            return running, provide(), b_contact
        finally:
            await b.stop()
            await a.stop()

    running, stopped, b_contact = asyncio.run(scenario())
    assert running == [(gossip_id(b_contact.node_id), b_contact.host, b_contact.port)]
    assert stopped == []


# ---------------- DHT departures reach gossip ----------------

def test_expire_entry_backdates_the_entry():
    entry = FakeEntry()
    engine = FakeEngine({"peer-1": entry})
    assert expire_entry(engine, "peer-1", now=1000.0) is True
    assert entry.received_at == 984.0          # 1000 - 15 (timeout) - 1


def test_expire_entry_for_unknown_peer_returns_false():
    assert expire_entry(FakeEngine({}), "nobody") is False


def test_link_departures_expires_only_the_peer_that_left():
    leaver, stayer = Contact(7, "h", 1), Contact(8, "h", 2)
    leaver_entry, stayer_entry = FakeEntry(), FakeEntry()
    engine = FakeEngine({gossip_id(7): leaver_entry, gossip_id(8): stayer_entry})
    dht = FakeDHT([])
    link_departures(dht, engine)
    dht.left_callbacks[0](leaver)
    assert leaver_entry.received_at < time.monotonic() - engine.heartbeat.timeout
    assert stayer_entry.received_at > time.monotonic() - 5


def test_link_departures_can_be_switched_off():
    dht = FakeDHT([])
    off = link_departures(dht, FakeEngine({}))
    assert len(dht.left_callbacks) == 1
    off()
    assert dht.left_callbacks == []


# ---------------- the real gossip engine (skipped until the package is importable) ----------------

class StubSampler:
    def sample(self):
        return 10.0, 20.0


def test_real_gossip_engine_over_the_adapter():
    gossip = pytest.importorskip("gossip")   # assumed package name: change it here later

    async def scenario():
        da, db = await make_dispatcher(), await make_dispatcher()
        ta, tb = GossipTransport(da), GossipTransport(db)
        ea = gossip.GossipEngine("node-a", *da.local_addr, ta,
                                 get_neighbors=lambda: [("node-b", *db.local_addr)],
                                 interval=0.2, fanout=1, sampler=StubSampler())
        eb = gossip.GossipEngine("node-b", *db.local_addr, tb,
                                 get_neighbors=lambda: [("node-a", *da.local_addr)],
                                 interval=0.2, fanout=1, sampler=StubSampler())
        await ea.start()
        await eb.start()
        try:
            await asyncio.sleep(0.8)
            return ea.table.get("node-b") is not None, eb.table.get("node-a") is not None
        finally:
            await ea.stop()
            await eb.stop()
            await close_all([ta, tb], [da, db])

    assert asyncio.run(scenario()) == (True, True)