import asyncio
import pickle
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import pytest

from meshweaver.integration.node_runner import MeshNode
from meshweaver.integration.task_channel import TaskChannelError, send_task

SECRET = b"test-secret-value-1234"


def execute(task_bytes):  # stands in for Module 4's executor
    return {"status": "completed", "result": task_bytes.upper(), "error": None}


def make_node(base):
    engines = []

    def factory(node_id, host, port, transport, neighbors):  # stands in for the gossip engine
        transport.register_handler("LOAD_UPDATE", lambda msg, addr: None)
        engine = MagicMock(neighbors=neighbors, start=AsyncMock(), stop=AsyncMock())
        engine.lowest_cpu_node.return_value = NS(node_id=node_id, cpu=1, host=host, port=port)
        engines.append(engine)
        return engine

    node = MeshNode(SECRET, execute, factory, ports=range(base, base + 10),
                    probe_timeout=0.3, rpc_timeout=0.5, maintenance=False)
    return node, engines


def test_start_shares_one_socket_and_stop_releases_everything():
    async def scenario():
        node, engines = make_node(47870)
        await node.start()
        port, routes = node.dht.address[1], node.dht.protocol.transport.routes #type: ignore
        task_port = node.task_address[1]
        await node.stop()
        with pytest.raises(TaskChannelError):
            await send_task("127.0.0.1", task_port, b"hi", timeout=2)
        return task_port - port, routes, engines[0].stop.await_count

    assert asyncio.run(scenario()) == (1000, ["LOAD_UPDATE"], 1)


def test_gossip_gets_the_dht_neighbours_as_hex_ids():
    async def scenario():
        (a, _), (b, engines) = make_node(47880), make_node(47880)
        await a.start()
        await b.start()
        try:
            return engines[0].neighbors(), a.dht.node_id, a.dht.address #type: ignore
        finally:
            await b.stop()
            await a.stop()

    neighbors, a_id, a_addr = asyncio.run(scenario())
    assert neighbors == [(format(a_id, "040x"), *a_addr)]


def test_task_runs_end_to_end_on_a_started_node():
    async def scenario():
        node, _ = make_node(47890)
        await node.start()
        try:
            return await node.submit(b"hello")
        finally:
            await node.stop()

    record = asyncio.run(scenario())
    assert (record.status, pickle.loads(record.result)) == ("completed", b"HELLO") #type: ignore