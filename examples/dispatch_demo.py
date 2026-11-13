import asyncio
import json

from meshweaver.integration.dispatch import Dispatcher
from meshweaver.dht.node_id import generate_node_id
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport


async def make_node():
    transport = UDPTransport("127.0.0.1", 0)
    await transport.start()
    dispatcher = Dispatcher(transport)
    return DHTProtocol(generate_node_id(), dispatcher, timeout=1.0), dispatcher


async def main():
    a, da = await make_node()
    b, db = await make_node()

    received = []
    db.add_route("LOAD_UPDATE", lambda data, addr: received.append(json.loads(data)))
    print("Node B routes:", db.routes)

    print("A sends a LOAD_UPDATE message to B's port...")
    da.send(json.dumps({"type": "LOAD_UPDATE", "cpu": 12.5}).encode(), db.local_addr)
    await asyncio.sleep(0.2)
    print("B's gossip handler received:", received[0] if received else None)
    print("A pings B through the same port:", await a.ping(b.contact))

    a.close()
    b.close()


asyncio.run(main())