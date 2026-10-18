import asyncio

from meshweaver.dht.node_id import generate_node_id
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport


async def main():
    nodes = []
    for _ in range(2):
        transport = UDPTransport("127.0.0.1", 0)
        await transport.start()
        nodes.append(DHTProtocol(generate_node_id(), transport))
    a, b = nodes
    print("A listening on", a.contact.addr)
    print("B listening on", b.contact.addr)
    print("A pings B:", await a.ping(b.contact))
    print("A knows", len(a.routing_table), "peer(s); B knows", len(b.routing_table), "peer(s)")
    a.close()
    b.close()


asyncio.run(main())