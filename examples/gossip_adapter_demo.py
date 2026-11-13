import asyncio

from meshweaver.integration.dispatch import Dispatcher
from meshweaver.integration.gossip_adapter import GossipTransport, gossip_id
from meshweaver.dht.transport import UDPTransport


async def make_dispatcher():
    transport = UDPTransport("127.0.0.1", 0)
    await transport.start()
    return Dispatcher(transport)


async def main():
    print("Gossip ID for DHT ID 255:", gossip_id(255))

    a, b = await make_dispatcher(), await make_dispatcher()
    ga, gb = GossipTransport(a), GossipTransport(b)
    seen = []

    async def on_update(msg, addr):
        seen.append((msg, addr))

    gb.register_handler("LOAD_UPDATE", on_update)
    await ga.send(b.local_addr, {"type": "LOAD_UPDATE", "cpu": 12.5})
    await asyncio.sleep(0.2)

    msg, addr = seen[0]
    print("Node B's gossip handler received:", msg)
    print("It came from node A's address:", addr == a.local_addr)

    await ga.stop()
    await gb.stop()
    a.close()
    b.close()


asyncio.run(main())