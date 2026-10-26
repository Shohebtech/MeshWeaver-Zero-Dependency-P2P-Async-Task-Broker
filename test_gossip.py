import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gossip import GossipEngine, HeartbeatMonitor, LoadEntry, LoadSampler, LoadTable, UDPTransport


def entry(nid, cpu=10.0, seq=1, port=1000):
    return LoadEntry(nid, "127.0.0.1", port, cpu, 30.0, seq, 0.0)


class TestLoadTable(unittest.TestCase):
    def test_merge_keeps_newest_seq(self):
        t = LoadTable("a")
        self.assertTrue(t.merge(entry("b", 10, seq=5)))
        self.assertFalse(t.merge(entry("b", 99, seq=4)))      # older ignored
        self.assertFalse(t.merge(entry("b", 99, seq=5)))      # duplicate ignored
        self.assertTrue(t.merge(entry("b", 20, seq=6)))
        self.assertEqual(t.get("b").cpu, 20)

    def test_staleness(self):
        t = LoadTable("a", stale_after=10)
        t.merge(entry("b"), now=100)
        self.assertFalse(t.is_stale(t.get("b"), now=105))
        self.assertTrue(t.is_stale(t.get("b"), now=111))
        self.assertEqual(len(t.entries(now=111)), 0)
        self.assertEqual(len(t.entries(include_stale=True, now=111)), 1)

    def test_lowest_cpu(self):
        t = LoadTable("a")
        for n, c in (("a", 50), ("b", 30), ("c", 5), ("d", 70)):
            t.merge(entry(n, c))
        self.assertEqual(t.lowest_cpu().node_id, "c")
        self.assertEqual(t.lowest_cpu(exclude=["c"]).node_id, "b")
        self.assertEqual(t.lowest_cpu(include_self=False).node_id, "c")
        self.assertIsNone(LoadTable("z").lowest_cpu())

    def test_wire_roundtrip(self):
        e = entry("x", 12.345, seq=9)
        self.assertEqual(LoadEntry.from_wire(e.to_wire()).seq, 9)


class TestSampler(unittest.TestCase):
    def test_ranges(self):
        s = LoadSampler()
        cpu, ram = s.sample()
        self.assertTrue(0 <= cpu <= 100)
        self.assertTrue(0 <= ram <= 100)


class TestHeartbeat(unittest.IsolatedAsyncioTestCase):
    async def test_lost_and_recovered(self):
        t = LoadTable("a", stale_after=100)
        hb = HeartbeatMonitor(t, interval=5, missed_threshold=3)   # timeout 15s
        lost, back = [], []
        hb.on_lost(lambda n, e: lost.append(n))
        hb.on_recovered(lambda n, e: back.append(n))
        t.merge(entry("b", seq=1), now=0)
        await hb.check(now=10)
        self.assertEqual(lost, [])
        await hb.check(now=16)
        self.assertEqual(lost, ["b"])
        await hb.check(now=20)
        self.assertEqual(lost, ["b"])                               # fired once only
        t.merge(entry("b", seq=2), now=21)
        await hb.check(now=22)
        self.assertEqual(back, ["b"])


class TestMeshSpread(unittest.IsolatedAsyncioTestCase):
    async def test_info_spreads_and_failure_detected(self):
        n, base = 6, 9300
        nodes = []
        for i in range(n):
            tr = UDPTransport("127.0.0.1", base + i)
            await tr.start()
            seeds = [(f"n{(i + 1) % n}", "127.0.0.1", base + (i + 1) % n)]   # one neighbour only
            g = GossipEngine(f"n{i}", "127.0.0.1", base + i, tr,
                             get_neighbors=lambda s=seeds: s, interval=0.2, fanout=1,
                             missed_threshold=3)
            nodes.append((g, tr))
        for g, _ in nodes:
            await g.start()
        await asyncio.sleep(2.0)
        for g, _ in nodes:
            self.assertEqual(len(g.table.entries()), n)             # everyone knows everyone
        await nodes[3][0].stop(); await nodes[3][1].stop()
        await asyncio.sleep(2.0)
        self.assertIn("n3", nodes[0][0].dead_peers())
        self.assertNotEqual(nodes[0][0].lowest_cpu_node().node_id, "n3")
        for i, (g, tr) in enumerate(nodes):
            if i != 3:
                await g.stop(); await tr.stop()


if __name__ == "__main__":
    unittest.main()