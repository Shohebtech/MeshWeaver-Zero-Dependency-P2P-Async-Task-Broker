import asyncio
from types import SimpleNamespace as NS

from meshweaver.integration.signing import sign, verify
from meshweaver.integration.task_channel import TaskChannelError
from meshweaver.integration.task_manager import TaskManager, decode_request, encode_reply

SECRET = b"test-secret-value-1234"


class FakeEngine:
    def __init__(self):
        self.entries = [NS(node_id="a", cpu=10, host="h", port=1),
                        NS(node_id="b", cpu=20, host="h", port=2)]
        self.callbacks = []

    def lowest_cpu_node(self, exclude=(), include_self=True):
        left = [e for e in self.entries if e.node_id not in exclude]
        return min(left, key=lambda e: e.cpu) if left else None

    def on_peer_lost(self, callback):
        self.callbacks.append(callback)


def replier(ok, body):
    async def node(data):
        task_id, _ = decode_request(verify(data, SECRET))
        return sign(encode_reply(task_id, ok, body), SECRET)
    return node


async def down(data):
    raise TaskChannelError("down")


async def hang(data):
    await asyncio.sleep(60)


def manager(nodes):
    calls, engine = [], FakeEngine()

    async def send(host, port, data, timeout=0):
        calls.append(port)
        return await nodes[port](data)

    return TaskManager(engine, SECRET, send=send, port_for=lambda p: p), engine, calls


def test_least_busy_node_gets_a_signed_task():
    seen, ok = [], replier(True, b"RESULT")

    async def node(data):
        seen.append(data)
        return await ok(data)

    mgr, _, calls = manager({1: node, 2: ok})
    record = asyncio.run(mgr.submit(b"task-bytes"))
    assert (record.status, record.result, record.node_id, calls) == ("completed", b"RESULT", "a", [1])
    assert decode_request(verify(seen[0], SECRET)) == (record.task_id, b"task-bytes")


def test_remote_error_is_reported_and_not_retried():
    mgr, _, calls = manager({1: replier(False, b"ValueError: boom"), 2: replier(True, b"x")})
    record = asyncio.run(mgr.submit(b"t"))
    assert (record.status, record.error, calls) == ("failed", "ValueError: boom", [1])


def test_unreachable_node_is_skipped_for_the_next_one():
    mgr, _, _ = manager({1: down, 2: replier(True, b"R")})
    record = asyncio.run(mgr.submit(b"t"))
    assert (record.status, record.node_id, record.tried) == ("completed", "b", ["a", "b"])


def test_node_lost_while_running_reroutes_without_waiting():
    async def scenario():
        mgr, engine, calls = manager({1: hang, 2: replier(True, b"R")})
        job = asyncio.ensure_future(mgr.submit(b"t"))
        await asyncio.sleep(0.1)                  # the task is now waiting on node a
        engine.callbacks[0]("a", None)            # gossip reports node a lost
        return await asyncio.wait_for(job, 5), calls

    record, calls = asyncio.run(scenario())
    assert (record.status, record.node_id, calls) == ("completed", "b", [1, 2])