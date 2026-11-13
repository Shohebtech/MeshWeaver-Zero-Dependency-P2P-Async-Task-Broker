import asyncio

from meshweaver.dht.maintenance import Maintenance
from meshweaver.dht.node_id import generate_node_id
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport


async def main():
    nodes = []
    for _ in range(3):
        transport = UDPTransport("127.0.0.1", 0)
        await transport.start()
        nodes.append(DHTProtocol(generate_node_id(), transport, timeout=0.3))
    a, b, c = nodes
    for x in nodes:
        for y in nodes:
            if x is not y:
                x.routing_table.add_contact(y.contact)

    print("Node A knows", len(a.routing_table), "peer(s)")
    print("Closing node C...")
    c.close()

    removed = await Maintenance(a).check_liveness(per_bucket=5)
    print("Peers removed by the liveness check:", len(removed))
    print("Node A now knows", len(a.routing_table), "peer(s)")

    a.close()
    b.close()


asyncio.run(main())