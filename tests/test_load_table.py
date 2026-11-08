import unittest

from config import GossipConfig
from modules.gossip.load_table import LoadTable
from modules.interfaces import NodeIdentity


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


def wire(nid, seq, cpu, ram=10.0, **kw):
    d = {"node_id": nid, "name": nid, "host": "127.0.0.1", "port": 9001, "http_port": 8001,
         "cpu": cpu, "ram": ram, "seq": seq, "origin_ts": 1.0, "links": []}
    d.update(kw)
    return d


class LoadTableTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.cfg = GossipConfig(interval=5.0)           # stale 10 s, offline 15 s
        self.t = LoadTable(self.cfg, self.clock)
        self.me = NodeIdentity("me", "me", "127.0.0.1", 9000, 8000)
        self.t.update_local(self.me, 5.0, 20.0)

    def test_local_row_sequence_always_increases(self):
        s1 = self.t.get("me")["seq"]
        s2 = self.t.update_local(self.me, 6, 21).seq
        self.assertGreater(s2, s1)

    def test_merge_newest_seq_wins_and_old_is_ignored(self):
        self.assertEqual(self.t.merge([wire("a", 5, 50)]), ["a"])
        self.assertEqual(self.t.merge([wire("a", 4, 99)]), [])      # older
        self.assertEqual(self.t.merge([wire("a", 5, 99)]), [])      # duplicate
        self.assertEqual(self.t.get("a")["cpu"], 50)
        self.assertEqual(self.t.merge([wire("a", 6, 70)]), ["a"])
        self.assertEqual(self.t.get("a")["cpu"], 70)

    def test_cannot_overwrite_own_row_or_inject_garbage(self):
        self.assertEqual(self.t.merge([wire("me", 10**15, 99)]), [])
        self.assertEqual(self.t.get("me")["cpu"], 5.0)
        bad = [{"node_id": "x"}, {"node_id": "y", "seq": "q", "cpu": 1, "ram": 1},
               wire("z", 1, float("nan")), "string", wire("w", 1, 150)]
        out = self.t.merge([b for b in bad if isinstance(b, dict)])
        self.assertEqual(out, ["w"])
        self.assertEqual(self.t.get("w")["cpu"], 100.0)               # clamped

    def test_timestamps_and_status_progression(self):
        self.t.merge([wire("a", 1, 10)])
        self.assertEqual(self.t.get("a")["status"], "online")
        self.assertFalse(self.t.get("a")["stale"])
        self.clock.t += 11                                            # > 2 intervals
        row = self.t.get("a")
        self.assertTrue(row["stale"])
        self.assertEqual(row["status"], "online")
        self.assertAlmostEqual(row["update_age"], 11, places=1)
        self.clock.t += 5                                             # > 3 intervals total
        self.assertEqual(self.t.get("a")["status"], "offline")

    def test_heartbeat_keeps_node_online_but_data_stays_stale(self):
        self.t.merge([wire("a", 1, 10)])
        self.clock.t += 12
        self.t.touch_heartbeat("a")
        row = self.t.get("a")
        self.assertEqual(row["status"], "online")
        self.assertTrue(row["stale"])

    def test_least_loaded_only_online_fresh_nodes(self):
        self.t.merge([wire("a", 1, 40), wire("b", 1, 20), wire("c", 1, 80)])
        self.assertEqual(self.t.least_loaded()["node_id"], "me")      # 5 % wins
        self.assertEqual(self.t.least_loaded(exclude=["me"])["node_id"], "b")
        self.clock.t += 20                                            # everyone silent
        self.t.update_local(self.me, 90, 50)                          # only 'me' refreshed
        self.assertEqual(self.t.least_loaded()["node_id"], "me")      # offline b is skipped
        self.assertIsNone(self.t.least_loaded(exclude=["me"]))

    def test_tie_break_uses_ram_then_id(self):
        self.t.merge([wire("a", 1, 5.0, 30.0), wire("b", 1, 5.0, 10.0)])
        self.assertEqual(self.t.least_loaded(exclude=["me"])["node_id"], "b")

    def test_summary_counts_and_averages(self):
        self.t.merge([wire("a", 1, 30, 40), wire("b", 1, 50, 60)])
        self.clock.t += 20
        self.t.update_local(self.me, 5, 20)
        self.t.touch_heartbeat("a")                                   # a alive, b offline
        s = self.t.summary()
        self.assertEqual((s["total_nodes"], s["online_nodes"], s["offline_nodes"]), (3, 2, 1))
        self.assertEqual(s["stale_nodes"], 1)
        self.assertAlmostEqual(s["avg_cpu"], 17.5)

    def test_simulated_rows_are_labelled_and_not_gossiped(self):
        self.t.upsert_simulated("sim1", "sim-1", 33, 44)
        row = self.t.get("sim1")
        self.assertEqual(row["source"], "simulated")
        self.assertNotIn("sim1", [w["node_id"] for w in self.t.wire_entries(50)])
        self.assertEqual(self.t.merge([wire("sim1", 10**12, 1)]), [])  # cannot be hijacked

    def test_series_and_purge(self):
        self.t.merge([wire("a", 1, 10), wire("a", 2, 20)])
        self.t.merge([wire("a", 3, 30)])
        self.assertEqual([p["cpu"] for p in self.t.series()["a"]], [10, 20, 30])
        self.clock.t += self.cfg.purge_after + 1
        self.assertEqual(self.t.purge(), ["a"])
        self.assertIsNone(self.t.get("a"))
        self.assertIsNotNone(self.t.get("me"))


if __name__ == "__main__":
    unittest.main()
