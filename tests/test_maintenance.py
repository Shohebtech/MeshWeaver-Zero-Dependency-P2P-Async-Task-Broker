import asyncio
import socket

from meshweaver.dht.contact import Contact
from meshweaver.dht.maintenance import Maintenance
from meshweaver.dht.network_store import put_value
from meshweaver.dht.node_id import generate_node_id, id_from_key
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.storage import Storage
from meshweaver.dht.transport import UDPTransport


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


async def make_node(timeout: float = 0.3) -> DHTProtocol:
    transport = UDPTransport("127.0.0.1", 0)
    await transport.start()
    return DHTProtocol(generate_node_id(), transport, timeout=timeout)


async def make_mesh(n: int):
    """n nodes that all know each other."""
    nodes = [await make_node(timeout=1.0) for _ in range(n)]
    for a in nodes:
        for b in nodes:
            if a is not b:
                a.routing_table.add_contact(b.contact)
    return nodes


def silent_socket():
    """A UDP port that exists but never answers: a 'dead' peer."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    return sock, sock.getsockname()[1]


def test_liveness_removes_dead_peer_keeps_live_one():
    sock, port = silent_socket()
    dead = Contact(123456, "127.0.0.1", port)

    async def scenario():
        a, b = await make_node(), await make_node()
        a.routing_table.add_contact(dead)
        a.routing_table.add_contact(b.contact)
        try:
            removed = await Maintenance(a).check_liveness(per_bucket=5)
            return removed, dead.node_id in a.routing_table, b.node_id in a.routing_table
        finally:
            a.close()
            b.close()

    try:
        removed, dead_kept, live_kept = asyncio.run(scenario())
    finally:
        sock.close()
    assert removed == [dead]
    assert dead_kept is False
    assert live_kept is True


def test_liveness_with_empty_table():
    async def scenario():
        a = await make_node()
        try:
            return await Maintenance(a).check_liveness()
        finally:
            a.close()

    assert asyncio.run(scenario()) == []


def test_refresh_discovers_new_peers():
    async def scenario():
        a, b, c = await make_node(), await make_node(), await make_node()
        a.routing_table.add_contact(b.contact)   # a knows only b
        b.routing_table.add_contact(c.contact)   # b knows c
        try:
            refreshed = await Maintenance(a).refresh_buckets()
            return refreshed, c.node_id in a.routing_table
        finally:
            for n in (a, b, c):
                n.close()

    assert asyncio.run(scenario()) == (1, True)   # a learned about c through b


def test_refresh_skips_buckets_that_are_not_due():
    async def scenario():
        clock = FakeClock()
        a, b = await make_node(), await make_node()
        a.routing_table.add_contact(b.contact)
        m = Maintenance(a, refresh_interval=100, clock=clock)
        try:
            first = await m.refresh_buckets()
            again = await m.refresh_buckets()     # just refreshed: not due
            clock.now += 101
            later = await m.refresh_buckets()     # interval passed: due again
            return first, again, later
        finally:
            a.close()
            b.close()

    assert asyncio.run(scenario()) == (1, 0, 1)


def test_purge_storage_removes_expired():
    async def scenario():
        clock = FakeClock()
        a = await make_node()
        a.storage = Storage(clock=clock)
        a.storage.set(1, "short", ttl=5)
        a.storage.set(2, "long", ttl=500)
        clock.now += 6
        try:
            return Maintenance(a).purge_storage(), 2 in a.storage
        finally:
            a.close()

    assert asyncio.run(scenario()) == (1, True)


def test_republish_restores_lost_replica():
    async def scenario():
        nodes = await make_mesh(3)
        m = Maintenance(nodes[0])
        key = id_from_key("load:node-0")
        try:
            first = await m.publish(key, {"cpu": 9}, ttl=60)
            nodes[1].storage = Storage()          # node 1 "lost" its copy
            lost = key not in nodes[1].storage
            again = await m.republish_all()
            return first, lost, again, key in nodes[1].storage
        finally:
            for n in nodes:
                n.close()

    assert asyncio.run(scenario()) == (3, True, 3, True)


def test_unpublish_stops_republishing():
    async def scenario():
        nodes = await make_mesh(3)
        m = Maintenance(nodes[0])
        key = id_from_key("temp")
        try:
            await m.publish(key, "v", ttl=60)
            removed = m.unpublish(key)
            nodes[1].storage = Storage()
            total = await m.republish_all()
            return removed, m.unpublish(key), total, key in nodes[1].storage
        finally:
            for n in nodes:
                n.close()

    assert asyncio.run(scenario()) == (True, False, 0, False)


def test_background_tasks_remove_dead_peer():
    sock, port = silent_socket()

    async def scenario():
        a = await make_node(timeout=0.2)
        a.routing_table.add_contact(Contact(123456, "127.0.0.1", port))
        m = Maintenance(a, ping_interval=0.05, refresh_interval=3600,
                        purge_interval=3600, republish_interval=3600)
        try:
            m.start()
            was_running = m.running
            await asyncio.sleep(0.8)
            await m.stop()
            return was_running, m.running, len(a.routing_table)
        finally:
            a.close()

    try:
        assert asyncio.run(scenario()) == (True, False, 0)
    finally:
        sock.close()


def test_start_twice_and_stop_twice_are_safe():
    async def scenario():
        a = await make_node()
        m = Maintenance(a)
        try:
            m.start()
            m.start()
            await m.stop()
            await m.stop()
            return m.running
        finally:
            a.close()

    assert asyncio.run(scenario()) is False