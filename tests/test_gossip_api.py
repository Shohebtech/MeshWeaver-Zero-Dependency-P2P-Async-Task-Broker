import unittest

from app import create_app
from config import GossipConfig
from modules.dht.mock_directory import MockPeerDirectory
from modules.gossip.engine import GossipEngine
from modules.gossip.load_monitor import LoadSample
from modules.interfaces import NodeIdentity
from modules.networking.memory_transport import MemoryHub
from modules.task_execution.mock_executor import MockTaskExecutor
from tests.test_load_table import FakeClock, wire


class FixedMonitor:
    backend = "fixed"

    def sample(self):
        return LoadSample(12.0, 34.0, 0.0, "fixed")


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        cfg = GossipConfig(interval=5.0)
        ident = NodeIdentity("me", "node-me", "127.0.0.1", 9000, 8000)
        self.engine = GossipEngine(ident, MemoryHub().transport(("127.0.0.1", 9000)),
                                   MockPeerDirectory([], ("127.0.0.1", 9000)), cfg,
                                   monitor=FixedMonitor(), clock=self.clock)
        self.engine.running = True
        self.engine.table.update_local(ident, 12.0, 34.0)
        self.engine.table.merge([wire("a", 1, 40.0, 50.0), wire("b", 1, 70.0, 20.0)])
        self.engine.heartbeat.beat("a")
        self.client = create_app(self.engine, MockTaskExecutor(self.engine, clock=self.clock)).test_client()

    def test_nodes(self):
        j = self.client.get("/api/gossip/nodes").get_json()
        self.assertEqual(j["count"], 3)
        self.assertEqual({n["node_id"] for n in j["nodes"]}, {"me", "a", "b"})
        for key in ("cpu", "ram", "last_heartbeat", "last_update", "status"):
            self.assertIn(key, j["nodes"][0])

    def test_summary(self):
        j = self.client.get("/api/gossip/summary").get_json()
        self.assertEqual((j["total_nodes"], j["online_nodes"], j["offline_nodes"]), (3, 3, 0))
        self.assertAlmostEqual(j["avg_cpu"], (12 + 40 + 70) / 3, places=1)
        self.assertEqual(j["least_loaded"]["node_id"], "me")

    def test_least_loaded_and_exclude(self):
        self.assertEqual(self.client.get("/api/gossip/least-loaded").get_json()["node"]["node_id"], "me")
        j = self.client.get("/api/gossip/least-loaded?exclude=me").get_json()
        self.assertEqual(j["node"]["node_id"], "a")

    def test_least_loaded_null_when_nobody_qualifies(self):
        j = self.client.get("/api/gossip/least-loaded?exclude=me,a,b").get_json()
        self.assertIsNone(j["node"])

    def test_health(self):
        r = self.client.get("/api/gossip/health")
        self.assertEqual(r.status_code, 200)
        j = r.get_json()
        self.assertEqual(j["gossip_interval"], 5.0)
        self.assertTrue(j["transport"]["mock"])
        self.assertTrue(j["directory"]["mock"])

    def test_history(self):
        j = self.client.get("/api/gossip/history?limit=5&kinds=tick").get_json()
        self.assertIn("events", j)
        self.assertIn("a", j["series"])
        self.assertEqual(self.client.get("/api/gossip/history?limit=x").status_code, 400)

    def test_post_heartbeat(self):
        r = self.client.post("/api/gossip/heartbeat", json={"node_id": "zz", "name": "ext", "host": "10.0.0.2", "port": 9100})
        self.assertEqual(r.status_code, 202)
        self.assertTrue(r.get_json()["new_node"])
        self.assertEqual(r.get_json()["node"]["source"], "reported")
        for bad in ({}, {"node_id": ""}, {"node_id": 5}, {"node_id": "me"}):
            self.assertEqual(self.client.post("/api/gossip/heartbeat", json=bad).status_code, 400)
        self.assertEqual(self.client.post("/api/gossip/heartbeat", data="nope").status_code, 400)

    def test_offline_node_reflected_in_api(self):
        self.clock.t += 20
        self.engine.table.update_local(self.engine.identity, 12.0, 34.0)
        j = self.client.get("/api/gossip/summary").get_json()
        self.assertEqual(j["offline_nodes"], 2)
        self.assertEqual(j["least_loaded"]["node_id"], "me")

    def test_mock_tasks_reroute_when_node_goes_offline(self):
        ex = MockTaskExecutor(self.engine, run_seconds=100, clock=self.clock)
        self.engine.table.merge([wire("c", 1, 1.0)])           # c has the lowest CPU
        task = ex.submit("demo")
        self.assertEqual(task["node_id"], "c")
        self.clock.t += 20                                       # c silent -> offline
        self.engine.table.update_local(self.engine.identity, 12.0, 34.0)
        self.engine.table.touch_heartbeat("a")
        self.engine.table.merge([wire("a", 2, 40.0)])
        t = ex.list_tasks()[0]
        self.assertEqual(t["status"], "running")
        self.assertEqual(t["node_id"], "me")
        self.assertEqual(len(t["reroutes"]), 1)

    def test_dashboard_endpoints_and_pages(self):
        for url in ("/api/overview", "/api/mesh", "/api/tasks", "/api/network"):
            self.assertEqual(self.client.get(url).status_code, 200, url)
        self.assertEqual(self.client.post("/api/tasks/demo", json={}).status_code, 201)
        mesh = self.client.get("/api/mesh").get_json()
        self.assertTrue(mesh["discovery"]["mock"])


if __name__ == "__main__":
    unittest.main()
