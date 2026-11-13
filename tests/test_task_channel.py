import asyncio

import pytest

from meshweaver.integration.task_channel import TaskChannelError, TaskServer, send_task, task_port


async def with_server(handler, action, **options):
    server = TaskServer(handler, **options)
    await server.start()
    try:
        return await action(server.address)
    finally:
        await server.stop()


def test_large_binary_payload_survives_the_trip():
    payload = bytes(range(256)) * 1000          # 256 KB, full of newline bytes

    async def act(addr):
        return await send_task(*addr, payload) #type: ignore

    assert asyncio.run(with_server(lambda data, peer: data[::-1], act)) == payload[::-1]


def test_async_handler_reply_is_returned():
    async def handler(data, peer):
        await asyncio.sleep(0)
        return b"got:" + data

    async def act(addr):
        return await send_task(*addr, b"abc") #type: ignore

    assert asyncio.run(with_server(handler, act)) == b"got:abc"


def test_oversized_request_is_refused_and_handler_not_called():
    calls = []

    def handler(data, peer):
        calls.append(data)
        return b"ok"

    async def act(addr):
        with pytest.raises(TaskChannelError):
            await send_task(*addr, b"x" * 200, timeout=2) #type: ignore

    asyncio.run(with_server(handler, act, max_bytes=100))
    assert calls == []


def test_failing_handler_does_not_stop_the_server():
    def handler(data, peer):
        if data == b"bad":
            raise RuntimeError("boom")
        return b"ok"

    async def act(addr):
        with pytest.raises(TaskChannelError):
            await send_task(*addr, b"bad", timeout=2) #type: ignore
        return await send_task(*addr, b"good", timeout=2) #type: ignore

    assert asyncio.run(with_server(handler, act)) == b"ok"


def test_unreachable_server_raises_task_channel_error():
    async def scenario():
        server = TaskServer(lambda data, peer: b"x")
        await server.start()
        addr = server.address
        await server.stop()                      # now nothing listens on that port
        with pytest.raises(TaskChannelError):
            await send_task(*addr, b"hi", timeout=2)

    asyncio.run(scenario())


def test_task_port_is_dht_port_plus_offset():
    assert task_port(9000) == 10000
    with pytest.raises(ValueError):
        task_port(65000)