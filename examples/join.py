import argparse
import asyncio

from meshweaver.dht.bootstrap import DEFAULT_PORTS, parse_addr, start_node


async def main(count, bootstrap):
    nodes = []
    try:
        for i in range(1, count + 1):
            node, result = await start_node(bootstrap=bootstrap)
            nodes.append(node)
            port = node.contact.port
            if result.joined:
                print(f"Node {i} (port {port}): joined, knows {result.peers} peer(s)")
            else:
                print(f"Node {i} (port {port}): first node, nobody else found")
        everyone = all(len(n.routing_table) == count - 1 for n in nodes)
        print(f"Every node knows all {count - 1} others: {everyone}")
    finally:
        for n in nodes:
            n.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Start several DHT nodes that find each other automatically.")
    parser.add_argument("--nodes", type=int, default=10,
                        help="how many nodes to start (1 to 20)")
    parser.add_argument("--bootstrap", action="append", default=[], metavar="HOST:PORT",
                        help="optional manual entry point (can be repeated)")
    args = parser.parse_args()
    if not 1 <= args.nodes <= len(DEFAULT_PORTS):
        parser.error(f"--nodes must be between 1 and {len(DEFAULT_PORTS)}")
    try:
        entry_points = [parse_addr(text) for text in args.bootstrap]
    except ValueError as e:
        parser.error(str(e))
    asyncio.run(main(args.nodes, entry_points))