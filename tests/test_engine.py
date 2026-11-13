import asyncio
import unittest

from config import GossipConfig
from modules.dht.mock_directory import MockPeerDirectory
from modules.gossip.engine import GossipEngine
from modules.gossip.load_monitor import LoadSample
from modules.interfaces import NodeIdentity
from modules.networking.memory_transport import MemoryHub


class FixedMonitor:
    backend = "fixed"

    def __init__(self, cpu, ram=30.0):
        self.cpu, self.ram = cpu, ram

    def sample(self):
        return LoadSample(self.cpu, self.ram, 0.0, "fixed")


def make(hub, idx, seeds, cpu, secret=None):
    addr = ("127.0.0.1", 7000 + idx)
    cfg = GossipConfig(interval=0.1, secret=secret)       # stale 0.2 s, offline 0.3 s
    ident = NodeIdentity(f"n{idx}", f"node-{idx}", addr[0], addr[1], 8000 + idx)
    d = MockPeerDirectory([("127.0.0.1", 7000 + s) for s in seeds], addr)
    return GossipEngine(ident, hub.transport(addr), d, cfg, monitor=FixedMonitor(cpu))


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.hub = MemoryHub()
        self.engines = []

    async def asyncTearDown(self):
        for e in self.engines:
            await e.stop()

    async def start(self, *engines):
        for e in engines:
            self.engines.append(e)
            await e.start()

    async def wait_for(self, cond, timeout=3.0):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            if cond():
                return True
            await asyncio.sleep(0.02)
        return False

    async def test_chain_topology_spreads_load_to_everyone(self):
        # n1<-n0 seed, n2 seeds n1, n3 seeds n2: n3 has never been told about n0
        es = [make(self.hub, 0, [], 50), make(self.hub, 1, [0], 40),
              make(self.hub, 2, [1], 30), make(self.hub, 3, [2], 10)]
        await self.start(*es)
        ok = await self.wait_for(lambda: all(
            {r["node_id"] for r in e.get_table() if r["cpu"] is not None} == {"n0", "n1", "n2", "n3"} for e in es))
        self.assertTrue(ok, [[r["node_id"] for r in e.get_table()] for e in es])
        for e in es:
            self.assertEqual(e.get_least_loaded()["node_id"], "n3")

    async def test_five_second_cadence_uses_config_interval(self):
        e = make(self.hub, 0, [], 1)
        e.cfg.interval = 5.0
        self.assertEqual(e.cfg.stale_after, 10.0)
        self.assertEqual(e.cfg.offline_after, 15.0)
        self.assertEqual(GossipConfig().interval, 5.0)

    async def test_crashed_node_detected_offline_and_excluded(self):
        es = [make(self.hub, 0, [], 50), make(self.hub, 1, [0], 40), make(self.hub, 2, [0], 5)]
        await self.start(*es)
        self.assertTrue(await self.wait_for(lambda: es[0].get_least_loaded()["node_id"] == "n2"))
        self.hub.down.add(("127.0.0.1", 7002))                            # n2 crashes
        self.assertTrue(await self.wait_for(lambda: not es[0].is_online("n2"), 3))
        self.assertTrue(await self.wait_for(lambda: not es[1].is_online("n2"), 3))
        self.assertEqual(es[0].get_least_loaded()["node_id"], "n1")
        kinds = [e["detail"] for e in es[0].events(kinds=["status_change"])]
        self.assertTrue(any("OFFLINE" in d for d in kinds), kinds)
        self.hub.down.clear()                                             # n2 recovers
        self.assertTrue(await self.wait_for(lambda: es[0].is_online("n2"), 3))

    async def test_signed_gossip_rejects_wrong_secret(self):
        a, b = make(self.hub, 0, [], 10, secret="s3cret"), make(self.hub, 1, [0], 20, secret="WRONG")
        c = make(self.hub, 2, [0], 30, secret="s3cret")
        await self.start(a, b, c)
        self.assertTrue(await self.wait_for(lambda: a.table.get("n2") and a.table.get("n2")["cpu"] == 30))
        await asyncio.sleep(0.4)
        self.assertIsNone(a.table.get("n1"))
        self.assertGreater(a.stats["rejected"], 0)

    async def test_malformed_messages_are_ignored(self):
        e = make(self.hub, 0, [], 10)
        await self.start(e)
        for junk in ({}, {"type": "gossip"}, {"sender": "x"}, {"sender": {"node_id": "q"}, "type": "gossip"},
                     {"sender": {"node_id": "q", "host": "h", "port": "nope"}, "type": "gossip"}):
            e._on_message(junk, ("1.2.3.4", 1))
        self.assertEqual(len(e.get_table()), 1)

    async def test_failure_events_survive_heartbeat_chatter(self):
        e = make(self.hub, 0, [], 10)
        await self.start(e)
        e._event("status_change", "x", "x went OFFLINE (test)")
        for i in range(e.cfg.event_history * 2):
            e._event("heartbeat_recv", "y", "noise")
        self.assertTrue(any("OFFLINE (test)" in ev["detail"] for ev in e.events(10, ["status_change"])))

    async def test_rest_heartbeat_marks_node_reported(self):
        e = make(self.hub, 0, [], 10)
        await self.start(e)
        e.record_heartbeat("ext", {"name": "external", "host": "10.0.0.9", "port": 9000})
        row = e.table.get("ext")
        self.assertEqual(row["source"], "reported")
        self.assertEqual(row["status"], "online")


if __name__ == "__main__":
    unittest.main()
