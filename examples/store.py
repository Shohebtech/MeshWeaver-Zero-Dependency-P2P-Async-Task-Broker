import asyncio

from meshweaver.dht.network_store import get_value, put_value
from meshweaver.dht.node_id import generate_node_id, id_from_key
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport


async def main():
    nodes = []
    for _ in range(5):
        transport = UDPTransport("127.0.0.1", 0)
        await transport.start()
        nodes.append(DHTProtocol(generate_node_id(), transport))
    for a in nodes:
        for b in nodes:
            if a is not b:
                a.routing_table.add_contact(b.contact)

    key = id_from_key("load:node-0")
    count = await put_value(nodes[0], key, {"cpu": 12.5}, ttl=1)
    print(f"Stored on {count} node(s) (ttl = 1s)")
    print("Node 4 reads it:", await get_value(nodes[4], key))
    await asyncio.sleep(1.5)
    print("After 1.5s, node 4 reads it:", await get_value(nodes[4], key))

    for n in nodes:
        n.close()


asyncio.run(main())