import argparse
import asyncio
import importlib
import math
import pickle
import sys
from typing import Any, Callable, List, Optional

from meshweaver.dht.bootstrap import parse_addr
from meshweaver.integration.node_runner import MeshNode
from meshweaver.integration.signing import SigningError, load_secret

# Assumed locations of the other modules. Override them with the flags below.
DEFAULT_GOSSIP = "gossip:GossipEngine"
DEFAULT_EXECUTOR = "meshweaver.executer:execute_serialized_task"
DEFAULT_SERIALIZER = "meshweaver.serializer:serialize_task"


def load_callable(spec: str) -> Callable:
    """Turn 'package.module:name' into the function or class it points to."""
    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(f"expected module:name, got {spec!r}")
    try:
        target = getattr(importlib.import_module(module_name), attr)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"cannot load {spec!r}: {e}") from e
    if not callable(target):
        raise ValueError(f"{spec!r} is not callable")
    return target


def gossip_factory_for(engine_class: Callable, interval: float) -> Callable:
    """The factory MeshNode wants, built from the gossip engine class."""
    def factory(node_id: str, host: str, port: int, transport: Any, neighbors: Callable) -> Any:
        return engine_class(node_id, host, port, transport,
                            get_neighbors=neighbors, interval=interval)
    return factory


def format_record(record: Any) -> str:
    if record.status == "completed":
        return f"completed on node {record.node_id[:8]}: {pickle.loads(record.result)!r}"
    return f"{record.status}: {record.error}"


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--port-base", type=int, default=9000)
    common.add_argument("--ports", type=int, default=20,
                        help="how many ports the mesh may use, starting at --port-base")
    common.add_argument("--bootstrap", action="append", default=[], metavar="HOST:PORT",
                        help="optional manual entry point (can be repeated)")
    common.add_argument("--gossip", default=DEFAULT_GOSSIP, metavar="MODULE:CLASS")
    common.add_argument("--executor", default=DEFAULT_EXECUTOR, metavar="MODULE:FUNCTION")
    common.add_argument("--interval", type=float, default=5.0, help="gossip interval in seconds")
    parser = argparse.ArgumentParser(prog="python -m meshweaver.integration.cli",
                                     description="Run a MeshWeaver node, or send it a test task.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", parents=[common], help="start a node and keep it running")
    submit = commands.add_parser("submit", parents=[common],
                                 help="join the mesh, send a test task (20!), print the result")
    submit.add_argument("--wait", type=float, default=8.0,
                        help="seconds to wait for load information before sending")
    submit.add_argument("--serializer", default=DEFAULT_SERIALIZER, metavar="MODULE:FUNCTION")
    return parser


def make_node(args: argparse.Namespace) -> MeshNode:
    secret = load_secret()                       # needs the MESHWEAVER_SECRET variable
    engine_class = load_callable(args.gossip)
    execute = load_callable(args.executor)
    bootstrap = [parse_addr(text) for text in args.bootstrap]
    return MeshNode(secret, execute, gossip_factory_for(engine_class, args.interval),
                    ports=range(args.port_base, args.port_base + args.ports),
                    bootstrap=bootstrap)


async def run(args: argparse.Namespace) -> int:
    node = make_node(args)
    result = await node.start()
    assert node.dht is not None
    print(f"Node {format(node.dht.node_id, '040x')[:8]} listening on {node.dht.address}, "
          f"tasks on {node.task_address}")
    print("Joined an existing mesh." if result.joined else "First node: nobody else found.")
    print("Press Ctrl+C to stop.")
    try:
        last = -1
        while True:
            count = node.dht.peer_count if node.dht else 0
            if count != last:
                last = count
                print(f"knows {count} peer(s)", flush=True)
            await asyncio.sleep(1)
    finally:
        await node.stop()


async def submit(args: argparse.Namespace) -> int:
    serialize = load_callable(args.serializer)
    task = serialize(math.factorial, (20,))
    node = make_node(args)
    await node.start()
    try:
        assert node.manager is not None
        node.manager.include_self = False        # a test task should run on ANOTHER node
        print(f"Waiting {args.wait}s for load information from other nodes...")
        await asyncio.sleep(args.wait)
        record = await node.submit(task)
        print(format_record(record))
        return 0 if record.status == "completed" else 1
    finally:
        await node.stop()


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return asyncio.run(run(args) if args.command == "run" else submit(args))
    except KeyboardInterrupt:
        return 0
    except (ValueError, SigningError, RuntimeError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())