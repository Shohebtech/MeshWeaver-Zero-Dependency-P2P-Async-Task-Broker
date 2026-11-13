import asyncio
import json

import pytest

from meshweaver.integration.dispatch import Dispatcher
from meshweaver.dht.node_id import generate_node_id
from meshweaver.dht.protocol import DHTProtocol
from meshweaver.dht.transport import UDPTransport


def msg(msg_type, **fields):
    return json.dumps({"type": msg_type, **fields}).encode()


async def make_udp():
    transport = UDPTransport("127.0.0.1", 0)
    await transport.start()
    return transport


async def make_dispatcher():
    return Dispatcher(await make_udp())


async def make_dht_node():
    dispatcher = await make_dispatcher()
    return DHTProtocol(generate_node_id(), dispatcher, timeout=1.0), dispatcher


def test_dht_nodes_work_over_dispatchers():
    async def scenario():
        a, _ = await make_dht_node()
        b, _ = await make_dht_node()
        try:
            ok = await a.ping(b.contact)
            return ok, a.node_id in b.routing_table, b.node_id in a.routing_table
        finally:
            a.close()
            b.close()

    assert asyncio.run(scenario()) == (True, True, True)


def test_registered_type_goes_to_its_handler():
    async def scenario():
        d = await make_dispatcher()
        default, routed = [], []
        d.set_receiver(lambda data, addr: default.append(data))
        d.add_route("LOAD_UPDATE", lambda data, addr: routed.append((data, addr)))
        sender = await make_udp()
        try:
            sender.send(msg("LOAD_UPDATE", cpu=5), d.local_addr)
            sender.send(msg("FIND_NODE"), d.local_addr)
            await asyncio.sleep(0.1)
            return routed, default, sender.local_addr
        finally:
            sender.close()
            d.close()

    routed, default, sender_addr = asyncio.run(scenario())
    assert len(routed) == 1
    assert json.loads(routed[0][0])["cpu"] == 5
    assert routed[0][1] == sender_addr
    assert [json.loads(x)["type"] for x in default] == ["FIND_NODE"]


def test_unreadable_messages_go_to_default_receiver():
    odd = [b"\xff\xfe\x00", b"not json", b"[1, 2]", b'{"no": "type"}', b'{"type": 5}']

    async def scenario():
        d = await make_dispatcher()
        default = []
        d.set_receiver(lambda data, addr: default.append(data))
        d.add_route("LOAD_UPDATE", lambda data, addr: default.append(b"WRONG"))
        sender = await make_udp()
        try:
            for data in odd:
                sender.send(data, d.local_addr)
            await asyncio.sleep(0.15)
            return default
        finally:
            sender.close()
            d.close()

    assert sorted(asyncio.run(scenario())) == sorted(odd)


def test_dht_message_types_cannot_be_claimed():
    async def scenario():
        d = await make_dispatcher()
        try:
            for msg_type in ("PING", "FIND_NODE", "STORE", "FIND_VALUE"):
                with pytest.raises(ValueError):
                    d.add_route(msg_type, lambda data, addr: None)
            return d.routes
        finally:
            d.close()

    assert asyncio.run(scenario()) == []


def test_bad_route_arguments_are_rejected():
    async def scenario():
        d = await make_dispatcher()
        handler = lambda data, addr: None
        try:
            d.add_route("LOAD_UPDATE", handler)
            with pytest.raises(ValueError):
                d.add_route("LOAD_UPDATE", handler)      # already routed
            with pytest.raises(ValueError):
                d.add_route("", handler)
            with pytest.raises(ValueError):
                d.add_route(5, handler)                   # type: ignore[arg-type]
            with pytest.raises(TypeError):
                d.add_route("OTHER", "not callable")      # type: ignore[arg-type]
            return d.routes
        finally:
            d.close()

    assert asyncio.run(scenario()) == ["LOAD_UPDATE"]


def test_remove_route_sends_traffic_back_to_default():
    async def scenario():
        d = await make_dispatcher()
        default, routed = [], []
        d.set_receiver(lambda data, addr: default.append(data))
        d.add_route("LOAD_UPDATE", lambda data, addr: routed.append(data))
        sender = await make_udp()
        try:
            removed = d.remove_route("LOAD_UPDATE")
            removed_again = d.remove_route("LOAD_UPDATE")
            sender.send(msg("LOAD_UPDATE"), d.local_addr)
            await asyncio.sleep(0.1)
            return removed, removed_again, len(routed), len(default)
        finally:
            sender.close()
            d.close()

    assert asyncio.run(scenario()) == (True, False, 0, 1)


def test_failing_handler_does_not_break_the_dispatcher():
    async def scenario():
        d = await make_dispatcher()
        good = []

        def bad(data, addr):
            raise RuntimeError("boom")

        d.add_route("BAD", bad)
        d.add_route("GOOD", lambda data, addr: good.append(data))
        sender = await make_udp()
        try:
            sender.send(msg("BAD"), d.local_addr)
            sender.send(msg("GOOD"), d.local_addr)
            await asyncio.sleep(0.1)
            return len(good)
        finally:
            sender.close()
            d.close()

    assert asyncio.run(scenario()) == 1


def test_messages_are_dropped_when_nobody_wants_them():
    async def scenario():
        d = await make_dispatcher()            # no default receiver, no routes
        sender = await make_udp()
        try:
            sender.send(msg("ANYTHING"), d.local_addr)
            sender.send(b"junk", d.local_addr)
            await asyncio.sleep(0.1)
            got = []
            d.add_route("LATER", lambda data, addr: got.append(data))
            sender.send(msg("LATER"), d.local_addr)
            await asyncio.sleep(0.1)
            return len(got)
        finally:
            sender.close()
            d.close()

    assert asyncio.run(scenario()) == 1


def test_one_socket_serves_dht_and_gossip_traffic():
    async def scenario():
        a, da = await make_dht_node()
        b, db = await make_dht_node()
        received = []
        db.add_route("LOAD_UPDATE", lambda data, addr: received.append(json.loads(data)))
        try:
            da.send(msg("LOAD_UPDATE", n=7), db.local_addr)
            await asyncio.sleep(0.1)
            ping_ok = await a.ping(b.contact)
            return received, ping_ok, b.contact.port == db.local_addr[1]
        finally:
            a.close()
            b.close()

    received, ping_ok, same_port = asyncio.run(scenario())
    assert received == [{"type": "LOAD_UPDATE", "n": 7}]
    assert ping_ok is True
    assert same_port is True            # gossip and the DHT share one port


def test_two_routes_are_independent():
    async def scenario():
        d = await make_dispatcher()
        first, second = [], []
        d.add_route("ONE", lambda data, addr: first.append(data))
        d.add_route("TWO", lambda data, addr: second.append(data))
        sender = await make_udp()
        try:
            sender.send(msg("TWO"), d.local_addr)
            sender.send(msg("ONE"), d.local_addr)
            sender.send(msg("TWO"), d.local_addr)
            await asyncio.sleep(0.15)
            return len(first), len(second), d.routes
        finally:
            sender.close()
            d.close()

    assert asyncio.run(scenario()) == (1, 2, ["ONE", "TWO"])


def test_send_and_local_addr_pass_through():
    async def scenario():
        d = await make_dispatcher()
        plain = await make_udp()
        got = []
        plain.set_receiver(lambda data, addr: got.append((data, addr)))
        try:
            d.send(b"hello", plain.local_addr)
            await asyncio.sleep(0.1)
            return got, d.local_addr
        finally:
            plain.close()
            d.close()

    got, dispatcher_addr = asyncio.run(scenario())
    assert got == [(b"hello", dispatcher_addr)]