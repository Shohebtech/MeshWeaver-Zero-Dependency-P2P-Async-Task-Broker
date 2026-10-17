import asyncio

import pytest

from meshweaver.dht.contact import Contact
from meshweaver.dht.messages import PING, make_request, make_response, decode
from meshweaver.dht.rpc import PendingRequests, RPCTimeout

ME = Contact(1, "127.0.0.1", 9000)
OTHER = Contact(2, "127.0.0.1", 9001)


def test_request_gets_response():
    async def scenario():
        pending = PendingRequests(timeout=1)
        req = make_request(PING, ME)
        sent = []

        async def reply_later():
            await asyncio.sleep(0.01)
            pending.resolve(make_response(req, OTHER, {"ok": True}))

        task = asyncio.create_task(reply_later())
        resp = await pending.request(req, sent.append)
        await task
        return req, resp, sent, pending

    req, resp, sent, pending = asyncio.run(scenario())
    assert resp.payload == {"ok": True}
    assert decode(sent[0]) == req      
    assert len(pending) == 0            


def test_request_times_out():
    async def scenario():
        pending = PendingRequests(timeout=0.05)
        with pytest.raises(RPCTimeout):
            await pending.request(make_request(PING, ME), lambda data: None)
        return pending

    pending = asyncio.run(scenario())
    assert len(pending) == 0


def test_unknown_response_is_ignored():
    pending = PendingRequests()
    stranger = make_response(make_request(PING, ME), OTHER)
    assert pending.resolve(stranger) is False


def test_request_message_is_not_a_response():
    pending = PendingRequests()
    assert pending.resolve(make_request(PING, ME)) is False


def test_late_response_after_timeout_is_ignored():
    async def scenario():
        pending = PendingRequests(timeout=0.05)
        req = make_request(PING, ME)
        with pytest.raises(RPCTimeout):
            await pending.request(req, lambda data: None)
        return pending.resolve(make_response(req, OTHER))

    assert asyncio.run(scenario()) is False


def test_concurrent_requests_are_matched_by_id():
    async def scenario():
        p = PendingRequests(timeout=1)
        r1, r2 = make_request(PING, ME), make_request(PING, ME)

        async def answer_in_reverse():
            await asyncio.sleep(0.01)
            p.resolve(make_response(r2, OTHER, {"n": 2}))
            p.resolve(make_response(r1, OTHER, {"n": 1}))

        task = asyncio.create_task(answer_in_reverse())
        a, b = await asyncio.gather(
            p.request(r1, lambda d: None),
            p.request(r2, lambda d: None),
        )
        await task
        return a, b

    a, b = asyncio.run(scenario())
    assert a.payload == {"n": 1}
    assert b.payload == {"n": 2}