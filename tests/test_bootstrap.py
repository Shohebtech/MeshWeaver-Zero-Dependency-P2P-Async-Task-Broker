import asyncio
import socket

import pytest

from meshweaver.dht.bootstrap import join_mesh, open_transport, parse_addr, start_node
from meshweaver.dht.node_id import generate_node_id
from meshweaver.dht.protocol import DHTProtocol

HOST = "127.0.0.1"


async def make_node(ports) -> DHTProtocol:
    transport = await open_transport(HOST, ports)
    return DHTProtocol(generate_node_id(), transport, timeout=1.0)


def test_parse_addr_valid():
    assert parse_addr("127.0.0.1:9000") == ("127.0.0.1", 9000)
    assert parse_addr("raspberrypi.local:8080") == ("raspberrypi.local", 8080)


def test_parse_addr_invalid():
    for bad in ("9000", "host:", ":9000", "host:abc", "host:0", "host:70000"):
        with pytest.raises(ValueError):
            parse_addr(bad)


def test_open_transport_uses_next_free_port():
    async def scenario():
        ports = range(47100, 47110)
        t1 = await open_transport(HOST, ports)
        t2 = await open_transport(HOST, ports)
        try:
            return t1.local_addr[1], t2.local_addr[1]
        finally:
            t1.close()
            t2.close()

    p1, p2 = asyncio.run(scenario())
    assert 47100 <= p1 < 47110
    assert p2 > p1  # the first port was taken, so the second node moved on


def test_open_transport_raises_when_every_port_is_busy():
    async def scenario():
        t1 = await open_transport(HOST, range(47120, 47130))
        try:
            with pytest.raises(RuntimeError):
                await open_transport(HOST, [t1.local_addr[1]])
        finally:
            t1.close()

    asyncio.run(scenario())


def test_first_node_finds_nobody():
    async def scenario():
        ports = range(47200, 47210)
        node = await make_node(ports)
        try:
            return await join_mesh(node, scan_host=HOST, scan_ports=ports, probe_timeout=0.2)
        finally:
            node.close()

    result = asyncio.run(scenario())
    assert result.joined is False
    assert result.bootstrap is None
    assert result.peers == 0


def test_second_node_joins_by_scanning_ports():
    async def scenario():
        ports = range(47300, 47310)
        a = await make_node(ports)
        b = await make_node(ports)
        try:
            result = await join_mesh(b, scan_host=HOST, scan_ports=ports, probe_timeout=0.3)
            return result, a.contact, a.node_id in b.routing_table, b.node_id in a.routing_table
        finally:
            a.close()
            b.close()

    result, a_contact, b_knows_a, a_knows_b = asyncio.run(scenario())
    assert result.joined is True
    assert result.bootstrap == a_contact
    assert b_knows_a and a_knows_b  # both sides learned about each other


def test_explicit_bootstrap_address():
    async def scenario():
        a = await make_node([0])  # port 0 = random port, so nothing could be scanned
        b = await make_node([0])
        try:
            result = await join_mesh(b, bootstrap=[a.contact.addr], scan_ports=[],
                                     probe_timeout=0.3)
            return result, a.contact
        finally:
            a.close()
            b.close()

    result, a_contact = asyncio.run(scenario())
    assert result.joined is True
    assert result.bootstrap == a_contact


def test_dead_bootstrap_falls_back_to_scan():
    silent = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    silent.bind((HOST, 0))  # bound but never answers
    dead_addr = silent.getsockname()

    async def scenario():
        ports = range(47500, 47510)
        a = await make_node(ports)
        b = await make_node(ports)
        try:
            result = await join_mesh(b, bootstrap=[dead_addr], scan_host=HOST,
                                     scan_ports=ports, probe_timeout=0.3)
            return result, a.contact
        finally:
            a.close()
            b.close()

    try:
        result, a_contact = asyncio.run(scenario())
    finally:
        silent.close()
    assert result.joined is True
    assert result.bootstrap == a_contact


def test_ten_nodes_form_a_mesh_without_hardcoded_addresses():
    async def scenario():
        ports = range(47600, 47620)
        nodes, results = [], []
        try:
            for _ in range(10):
                node, result = await start_node(HOST, ports, probe_timeout=0.3)
                nodes.append(node)
                results.append(result)
            ids = [n.node_id for n in nodes]
            known = [{c.node_id for c in n.routing_table.all_contacts()} for n in nodes]
            return results, ids, known
        finally:
            for n in nodes:
                n.close()

    results, ids, known = asyncio.run(scenario())
    assert results[0].joined is False                 # the very first node is alone
    assert all(r.joined for r in results[1:])
    for node_id, peers in zip(ids, known):
        assert peers == set(ids) - {node_id}          # everyone knows everyone else