"""Demo: 10 local gossip nodes. Each node initially knows only 2 neighbours,
yet load information spreads to everyone; then one node is killed and detected.

    python demo_gossip.py
"""
import asyncio
import random

from gossip import GossipEngine, UDPTransport

N, INTERVAL, BASE_PORT = 10, 1.0, 9100


class FakeSampler:
    """Deterministic-ish fake load so 'lowest CPU' is meaningful on one machine."""
    def __init__(self, cpu): self.cpu = cpu
    def sample(self): return (self.cpu + random.uniform(-2, 2), 40.0)


async def main():
    nodes = []
    for i in range(N):
        t = UDPTransport("127.0.0.1", BASE_PORT + i)
        await t.start()
        # sparse seed list: ring neighbours only (stand-in for the DHT)
        seeds = [(f"node{(i + d) % N}", "127.0.0.1", BASE_PORT + (i + d) % N) for d in (1, 2)]
        g = GossipEngine(f"node{i}", "127.0.0.1", BASE_PORT + i, t,
                         get_neighbors=lambda s=seeds: s,
                         interval=INTERVAL, fanout=2, sampler=FakeSampler(10 + i * 8))
        nodes.append((g, t))

    nodes[0][0].on_peer_lost(lambda nid, e: print(f"  >> node0 detected LOST: {nid}"))
    for g, _ in nodes:
        await g.start()

    await asyncio.sleep(INTERVAL * 6)
    print("\nAfter 6 rounds, node0's load table:")
    for r in nodes[0][0].load_table():
        print(f"  {r['id']:7} cpu={r['cpu']:5.1f}% ram={r['ram']:4.1f}% age={r['age']}s")
    print("Entries known per node:", [len(g.table.entries()) for g, _ in nodes])
    best = nodes[0][0].lowest_cpu_node()
    print(f"Lowest-CPU node: {best.node_id} ({best.cpu:.1f}%)")

    print("\nKilling node5 ...")
    await nodes[5][0].stop(); await nodes[5][1].stop()
    await asyncio.sleep(INTERVAL * 6)
    print("node0 dead peers:", nodes[0][0].dead_peers())

    for i, (g, t) in enumerate(nodes):
        if i != 5:
            await g.stop(); await t.stop()


asyncio.run(main())