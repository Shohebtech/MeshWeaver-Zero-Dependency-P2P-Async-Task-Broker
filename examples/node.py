import argparse
import asyncio
import json
import os
import sys
import time

from meshweaver.dht.node import DHTNode


def write_status(path, name, node, joined):
    """Write this node's state to a small JSON file so the audit can read it."""
    data = {
        "name": name,
        "pid": os.getpid(),
        "node_id": format(node.node_id, "040x"),
        "port": node.address[1],
        "joined": joined,
        "peers": [format(c.node_id, "040x") for c in node.peers()],
    }
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)  # swap in one step so a reader never sees half a file
    except OSError:
        pass  # the audit may be reading the file right now: try again next tick


async def run(args):
    node = DHTNode(ports=range(args.port_base, args.port_base + args.ports))
    result = await node.start()   # no bootstrap address is passed: it finds the mesh itself

    status_path = stop_path = None
    if args.status_dir:
        status_path = os.path.join(args.status_dir, f"node-{args.name}.json")
        stop_path = os.path.join(args.status_dir, "STOP")
    deadline = None if args.max_seconds == 0 else time.monotonic() + args.max_seconds
    last_count = -1

    try:
        while deadline is None or time.monotonic() < deadline:
            if stop_path and os.path.exists(stop_path):
                break
            if status_path:
                write_status(status_path, args.name, node, result.joined)
            elif node.peer_count != last_count:   # manual mode: print when it changes
                last_count = node.peer_count
                print(f"node {args.name} (port {node.address[1]}): "
                      f"knows {last_count} peer(s)", flush=True)
            await asyncio.sleep(0.25)
    finally:
        await node.stop()


def main():
    parser = argparse.ArgumentParser(description="Run one MeshWeaver DHT node.")
    parser.add_argument("--name", type=int, default=1, help="a label for this node")
    parser.add_argument("--status-dir", default=None,
                        help="folder for the status file (used by the audit)")
    parser.add_argument("--port-base", type=int, default=9000)
    parser.add_argument("--ports", type=int, default=20,
                        help="how many ports the mesh may use, starting at --port-base")
    parser.add_argument("--max-seconds", type=float, default=120,
                        help="stop by itself after this long (0 = run until Ctrl+C)")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except RuntimeError as e:   # for example: no free port in the range
        print(f"node {args.name}: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()