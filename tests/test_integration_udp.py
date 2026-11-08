"""Integration test over REAL UDP sockets on localhost (3 engines, one seed)."""
import asyncio
import unittest

from config import GossipConfig
from modules.dht.mock_directory import MockPeerDirectory
from modules.gossip.engine import GossipEngine
from modules.gossip.load_monitor import LoadSample
from modules.interfaces import NodeIdentity
from modules.networking.mock_transport import MockUdpTransport


class FixedMonitor:
    backend = "fixed"

    def __init__(self, cpu):
        self.cpu = cpu

    def sample(self):
        return LoadSample(self.cpu, 40.0, 0.0, "fixed")


def make(idx, seed_port, cpu):
    ident = NodeIdentity(f"u{idx}", f"udp-{idx}", "127.0.0.1", 0, 0)
    transport = MockUdpTransport("127.0.0.1", 0)
    directory = MockPeerDirectory([("127.0.0.1", seed_port)] if seed_port else [])
    cfg = GossipConfig(interval=0.2)           # stale 0.4 s, offline 0.6 s
    return GossipEngine(ident, transport, directory, cfg, monitor=FixedMonitor(cpu))


class UdpIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def wait_for(self, cond, timeout=6.0):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            if cond():
                return True
            await asyncio.sleep(0.05)
        return False

    async def test_mesh_forms_spreads_load_and_detects_failure(self):
        a = make(0, None, 60)
        await a.start()
        a.directory.self_addr = ("127.0.0.1", a.identity.port)
        b = make(1, a.identity.port, 30)
        c = make(2, a.identity.port, 10)
        await b.start()
        await c.start()
        engines = (a, b, c)
        try:
            full = lambda e: {r["node_id"] for r in e.get_table() if r["cpu"] is not None} == {"u0", "u1", "u2"}
            self.assertTrue(await self.wait_for(lambda: all(full(e) for e in engines)),
                            [[r["node_id"] for r in e.get_table()] for e in engines])
            for e in engines:
                self.assertEqual(e.get_least_loaded()["node_id"], "u2")
                self.assertEqual(e.table.get("u1")["source"], "real")
            self.assertGreater(a.transport.stats["received"], 0)

            await c.stop()                                          # the least-loaded node dies
            self.assertTrue(await self.wait_for(lambda: not a.is_online("u2") and not b.is_online("u2")))
            self.assertEqual(a.get_least_loaded()["node_id"], "u1")
            self.assertEqual(a.table.summary()["offline_nodes"], 1)
        finally:
            for e in (a, b, c):
                await e.stop()


if __name__ == "__main__":
    unittest.main()
