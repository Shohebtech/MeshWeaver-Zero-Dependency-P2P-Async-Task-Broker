import asyncio

from meshweaver.dht.lookup import iterative_find_node
from meshweaver.dht.node_id import generate_node_id
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport


async def main():
    nodes = []
    for _ in range(6):
        transport = UDPTransport("127.0.0.1", 0)
        await transport.start()
        nodes.append(DHTProtocol(generate_node_id(), transport))

    # A chain: node 0 knows 1, node 1 knows 2, ... node 4 knows 5.
    for a, b in zip(nodes, nodes[1:]):
        a.routing_table.add_contact(b.contact)

    searcher = nodes[0]
    print("Before lookup: node 0 knows", len(searcher.routing_table), "peer(s)")
    found = await iterative_find_node(searcher, generate_node_id())
    print("Lookup discovered", len(found), "peer(s)")
    print("After lookup: node 0 knows", len(searcher.routing_table), "peer(s)")

    for n in nodes:
        n.close()


asyncio.run(main())