import asyncio

import pytest

from meshweaver.dht.node import DHTNode
from meshweaver.dht.node_id import distance, id_from_key


def ports(base):
    return range(base, base + 10)


async def launch(base, **options):
    node = DHTNode(ports=ports(base), probe_timeout=0.3, rpc_timeout=0.5,
                   maintenance=False, **options)
    result = await node.start()
    return node, result


async def stop_all(nodes):
    for node in nodes:
        await node.stop()


def test_single_node_starts_alone():
    async def scenario():
        node, result = await launch(47700)
        try:
            return (result.joined, node.get_neighbors(), node.peer_count,
                    node.address[1], node.is_running)
        finally:
            await node.stop()

    joined, neighbors, count, port, running = asyncio.run(scenario())
    assert joined is False
    assert neighbors == []
    assert count == 0
    assert port == 47700
    assert running is True


def test_two_nodes_see_each_other():
    async def scenario():
        a, _ = await launch(47710)
        b, result = await launch(47710)
        try:
            return (result.joined,
                    [c.node_id for c in a.get_neighbors()],
                    [c.node_id for c in b.get_neighbors()],
                    a.node_id, b.node_id, a.knows(b.node_id))
        finally:
            await stop_all([a, b])

    joined, a_sees, b_sees, a_id, b_id, a_knows_b = asyncio.run(scenario())
    assert joined is True
    assert a_sees == [b_id]
    assert b_sees == [a_id]
    assert a_knows_b is True


def test_neighbors_are_sorted_by_xor_and_limited():
    async def scenario():
        nodes = []
        for _ in range(5):
            node, _ = await launch(47720)
            nodes.append(node)
        first = nodes[0]
        try:
            return (first.node_id, first.get_neighbors(), first.get_neighbors(2),
                    first.get_neighbors(0), first.peers())
        finally:
            await stop_all(nodes)

    own_id, neighbors, two, none_at_all, peers = asyncio.run(scenario())
    assert len(neighbors) == 4
    assert len(peers) == 4
    distances = [distance(c.node_id, own_id) for c in neighbors]
    assert distances == sorted(distances)
    assert two == neighbors[:2]
    assert none_at_all == []


def test_closest_peers_accepts_id_or_key():
    async def scenario():
        nodes = []
        for _ in range(3):
            node, _ = await launch(47730)
            nodes.append(node)
        a = nodes[0]
        try:
            target = id_from_key("task-1")
            return (a.closest_peers("task-1"), a.closest_peers(b"task-1"),
                    a.closest_peers(target), a.closest_peers(target, 1), target)
        finally:
            await stop_all(nodes)

    by_str, by_bytes, by_id, one, target = asyncio.run(scenario())
    assert len(by_id) == 2
    assert by_str == by_bytes == by_id
    assert [distance(c.node_id, target) for c in by_id] == sorted(
        distance(c.node_id, target) for c in by_id)
    assert len(one) == 1


def test_invalid_arguments_are_rejected():
    async def scenario():
        node, _ = await launch(47740)
        try:
            with pytest.raises(ValueError):
                node.get_neighbors(-1)
            with pytest.raises(ValueError):
                node.closest_peers(2 ** 160)
            with pytest.raises(TypeError):
                node.closest_peers(3.5)  # type: ignore[arg-type]
            with pytest.raises(TypeError):
                node.closest_peers(True)
        finally:
            await node.stop()

    asyncio.run(scenario())


def test_peer_joined_callback_fires_once_per_peer():
    async def scenario():
        a, _ = await launch(47750)
        seen = []
        a.on_peer_joined(seen.append)
        b, _ = await launch(47750)
        try:
            await asyncio.sleep(0.05)
            await b.ping(a.contact)       # more traffic from a known peer...
            await a.ping(b.contact)
            await asyncio.sleep(0.05)
            return [c.node_id for c in seen], b.node_id
        finally:
            await stop_all([a, b])

    seen_ids, b_id = asyncio.run(scenario())
    assert seen_ids == [b_id]             # ...does not fire the callback again


def test_async_callback_is_supported():
    async def scenario():
        a, _ = await launch(47760)
        seen = []

        async def on_join(contact):
            await asyncio.sleep(0)
            seen.append(contact.node_id)

        a.on_peer_joined(on_join)
        b, _ = await launch(47760)
        try:
            await asyncio.sleep(0.1)
            return seen, b.node_id
        finally:
            await stop_all([a, b])

    seen, b_id = asyncio.run(scenario())
    assert seen == [b_id]


def test_peer_left_callback_fires_when_dead_peer_is_removed():
    async def scenario():
        a, _ = await launch(47770)
        b, _ = await launch(47770)
        left = []
        a.on_peer_left(left.append)
        b_id = b.node_id
        await b.stop()                     # b "dies"
        try:
            removed = await a.maintenance.check_liveness(per_bucket=5)
            return [c.node_id for c in left], [c.node_id for c in removed], b_id, a.peer_count
        finally:
            await a.stop()

    left_ids, removed_ids, b_id, count = asyncio.run(scenario())
    assert left_ids == [b_id]
    assert removed_ids == [b_id]
    assert count == 0


def test_unsubscribe_stops_callbacks():
    async def scenario():
        a, _ = await launch(47780)
        seen = []
        off = a.on_peer_joined(seen.append)
        off()
        off()                              # calling it twice is harmless
        b, _ = await launch(47780)
        try:
            await asyncio.sleep(0.05)
            return seen
        finally:
            await stop_all([a, b])

    assert asyncio.run(scenario()) == []


def test_broken_callback_does_not_break_the_node():
    async def scenario():
        a, _ = await launch(47790)
        seen = []

        def bad(contact):
            raise RuntimeError("boom")

        a.on_peer_joined(bad)
        a.on_peer_joined(lambda c: seen.append(c.node_id))
        b, _ = await launch(47790)
        try:
            await asyncio.sleep(0.05)
            return a.knows(b.node_id), seen, b.node_id
        finally:
            await stop_all([a, b])

    knows, seen, b_id = asyncio.run(scenario())
    assert knows is True
    assert seen == [b_id]                  # the second callback still ran


def test_publish_and_get_across_nodes():
    async def scenario():
        nodes = []
        for _ in range(3):
            node, _ = await launch(47800)
            nodes.append(node)
        a, _, c = nodes
        try:
            count = await a.publish("load:a", {"cpu": 5}, ttl=60)
            found = await c.get("load:a")
            missing = await c.get("nobody-stored-this")
            return count, found, missing
        finally:
            await stop_all(nodes)

    count, found, missing = asyncio.run(scenario())
    assert count >= 2
    assert found == (True, {"cpu": 5})
    assert missing == (False, None)


def test_unpublish():
    async def scenario():
        node, _ = await launch(47810)
        try:
            await node.publish("temp", "v", ttl=60)
            return node.unpublish("temp"), node.unpublish("temp")
        finally:
            await node.stop()

    assert asyncio.run(scenario()) == (True, False)


def test_start_twice_fails_and_stop_twice_is_safe():
    async def scenario():
        node, _ = await launch(47820)
        with pytest.raises(RuntimeError):
            await node.start()
        await node.stop()
        await node.stop()
        return node.is_running

    assert asyncio.run(scenario()) is False


def test_context_manager_and_not_started_error():
    async def scenario():
        node = DHTNode(ports=ports(47830), probe_timeout=0.3, rpc_timeout=0.5,
                       maintenance=False)
        with pytest.raises(RuntimeError):
            node.get_neighbors()
        async with node as inside:
            same, running = inside is node, node.is_running
        return same, running, node.is_running

    assert asyncio.run(scenario()) == (True, True, False)


def test_background_maintenance_starts_and_stops_with_the_node():
    async def scenario():
        node = DHTNode(ports=ports(47840), probe_timeout=0.3, rpc_timeout=0.5,
                       maintenance=True)
        await node.start()
        jobs = node.maintenance
        was_running = jobs.running
        await node.stop()
        return was_running, jobs.running

    assert asyncio.run(scenario()) == (True, False)