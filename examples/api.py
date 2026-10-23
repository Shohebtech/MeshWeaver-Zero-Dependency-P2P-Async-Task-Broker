import asyncio

from meshweaver.dht.node import DHTNode


async def main():
    events = []
    nodes = []
    try:
        for i in range(4):
            node = DHTNode(rpc_timeout=0.5, probe_timeout=0.3, maintenance=False)
            if i == 0:
                node.on_peer_joined(lambda c: events.append(f"joined: port {c.port}"))
                node.on_peer_left(lambda c: events.append(f"left:   port {c.port}"))
            await node.start()
            nodes.append(node)
        first = nodes[0]

        print("Node 1 knows", first.peer_count, "peer(s)")
        print("2 nearest neighbours asked for, got:", len(first.get_neighbors(2)))
        print('Closest to "task-42":', len(first.closest_peers("task-42", 2)), "peer(s)")

        count = await nodes[1].publish("load:node-2", {"cpu": 12.5}, ttl=60)
        print("Published on", count, "node(s)")
        print("Node 4 reads it:", await nodes[3].get("load:node-2"))

        await nodes[3].stop()  # node 4 "dies"
        removed = await first.maintenance.check_liveness(per_bucket=5)
        print("Liveness check removed", len(removed), "peer(s)")

        print("Events seen by node 1:")
        for line in events:
            print("  " + line)
    finally:
        for node in nodes:
            await node.stop()


asyncio.run(main())