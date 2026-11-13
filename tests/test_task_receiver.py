import asyncio
import pickle
from types import SimpleNamespace as NS

import pytest

from meshweaver.integration.signing import SigningError, sign, verify
from meshweaver.integration.task_channel import TaskServer
from meshweaver.integration.task_manager import TaskManager, decode_reply, encode_request
from meshweaver.integration.task_receiver import TaskReceiver

SECRET = b"test-secret-value-1234"
ID = "ab" * 16


def execute(task_bytes):                      # stands in for execute_serialized_task
    if task_bytes == b"boom":
        return {"status": "failed", "result": None, "error": "Execution error: boom"}
    return {"status": "completed", "result": task_bytes.upper(), "error": None}


def request(task_bytes, task_id=ID, secret=SECRET):
    return sign(encode_request(task_id, task_bytes), secret)


def handle(*requests):
    async def scenario():
        receiver = TaskReceiver(SECRET, execute)
        return [decode_reply(verify(await receiver.handle(r), SECRET)) for r in requests]
    return asyncio.run(scenario())


def test_valid_task_runs_and_the_reply_is_signed():
    ((task_id, ok, body),) = handle(request(b"hello"))
    assert (task_id, ok, pickle.loads(body)) == (ID, True, b"HELLO")


def test_failing_task_is_reported_as_an_error():
    ((_, ok, body),) = handle(request(b"boom"))
    assert (ok, body) == (False, b"Execution error: boom")


def test_replayed_task_id_is_refused():
    first, second = handle(request(b"hello"), request(b"hello"))
    assert first[1] is True
    assert (second[1], second[2]) == (False, b"duplicate task id")


def test_wrong_secret_is_rejected_before_anything_runs():
    receiver = TaskReceiver(SECRET, execute)
    with pytest.raises(SigningError):
        asyncio.run(receiver.handle(request(b"hello", secret=b"some-other-secret-xx")))


def test_manager_to_receiver_over_tcp():
    async def scenario():
        server = TaskServer(TaskReceiver(SECRET, execute).handle)
        await server.start()
        host, port = server.address
        engine = NS(lowest_cpu_node=lambda exclude=(), include_self=True:
                    NS(node_id="a", cpu=1, host=host, port=port),
                    on_peer_lost=lambda cb: None)
        try:
            return await TaskManager(engine, SECRET, port_for=lambda p: p).submit(b"hello")
        finally:
            await server.stop()

    record = asyncio.run(scenario())
    assert (record.status, pickle.loads(record.result)) == ("completed", b"HELLO") #type: ignore 