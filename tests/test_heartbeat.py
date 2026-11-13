import unittest

from config import GossipConfig
from modules.gossip.heartbeat import HeartbeatMonitor
from modules.gossip.load_table import LoadTable
from tests.test_load_table import FakeClock, wire


class HeartbeatTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.cfg = GossipConfig(interval=5.0)
        self.table = LoadTable(self.cfg, self.clock)
        self.hb = HeartbeatMonitor(self.table, self.cfg, self.clock)

    def test_unknown_node_creates_placeholder_then_joins(self):
        self.assertTrue(self.hb.beat("a", {"name": "a", "host": "h", "port": 1}))
        self.assertFalse(self.hb.beat("a"))
        out = self.hb.sweep()
        self.assertEqual([(t["node_id"], t["old"], t["new"]) for t in out], [("a", None, "stale")])

    def test_online_stale_offline_online_cycle(self):
        self.table.merge([wire("a", 1, 10)])
        self.hb.beat("a")
        self.assertEqual(self.hb.sweep()[0]["new"], "online")
        self.clock.t += 11
        self.assertEqual([t["new"] for t in self.hb.sweep()], ["stale"])
        self.clock.t += 5
        self.assertEqual([t["new"] for t in self.hb.sweep()], ["offline"])
        self.assertEqual(self.hb.sweep(), [])                         # no repeat events
        self.table.merge([wire("a", 2, 15)])
        self.hb.beat("a")
        self.assertEqual([t["new"] for t in self.hb.sweep()], ["online"])

    def test_direct_peers_expire(self):
        self.hb.beat("a")
        self.hb.beat("b", direct=False, source="reported")
        self.assertEqual(self.hb.direct_peers(), ["a"])
        self.clock.t += 16
        self.assertEqual(self.hb.direct_peers(), [])


if __name__ == "__main__":
    unittest.main()
