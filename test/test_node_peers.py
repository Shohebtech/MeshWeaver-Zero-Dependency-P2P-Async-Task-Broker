import unittest

from networking.node import MeshNode


class TestMeshNodePeers(unittest.TestCase):

    def setUp(self):
        self.node = MeshNode(node_id="node_1")

        self.node.register_peer(
            "node_2",
            "127.0.0.1",
            25001,
            9998,
        )

    def test_unregister_existing_peer(self):
        result = self.node.unregister_peer("node_2")

        self.assertTrue(result)
        self.assertNotIn("node_2", self.node.peers)

    def test_unregister_unknown_peer(self):
        result = self.node.unregister_peer("unknown_node")

        self.assertFalse(result)

    def test_update_peer_host_and_port(self):
        result = self.node.update_peer(
            peer_id="node_2",
            host="192.168.1.10",
            tcp_port=25005,
        )

        self.assertTrue(result)
        self.assertEqual(
            self.node.peers["node_2"]["host"],
            "192.168.1.10",
        )
        self.assertEqual(
            self.node.peers["node_2"]["tcp_port"],
            25005,
        )

        # UDP port should remain unchanged.
        self.assertEqual(
            self.node.peers["node_2"]["udp_port"],
            9998,
        )

    def test_update_unknown_peer(self):
        result = self.node.update_peer(
            peer_id="unknown_node",
            host="192.168.1.20",
        )

        self.assertFalse(result)

    def test_get_existing_peer(self):
        peer = self.node.get_peer("node_2")

        self.assertIsNotNone(peer)
        self.assertEqual(peer["host"], "127.0.0.1")
        self.assertEqual(peer["tcp_port"], 25001)
        self.assertEqual(peer["udp_port"], 9998)

    def test_get_peer_returns_copy(self):
        peer = self.node.get_peer("node_2")

        peer["host"] = "192.168.1.100"

        # Original peer information must remain unchanged.
        self.assertEqual(
            self.node.peers["node_2"]["host"],
            "127.0.0.1",
        )

    def test_get_unknown_peer(self):
        peer = self.node.get_peer("unknown_node")

        self.assertIsNone(peer)

    def test_register_peer_with_empty_id(self):
        with self.assertRaises(ValueError):
            self.node.register_peer(
                "", "127.0.0.1", 25002, 9997
            )

    def test_register_peer_with_empty_host(self):
        with self.assertRaises(ValueError):
            self.node.register_peer(
                "node_3", "", 25002, 9997
            )

    def test_register_peer_with_invalid_tcp_port(self):
        with self.assertRaises(ValueError):
            self.node.register_peer(
                "node_3", "127.0.0.1", 70000, 9997
            )

    def test_register_peer_with_invalid_udp_port(self):
        with self.assertRaises(ValueError):
            self.node.register_peer(
                "node_3", "127.0.0.1", 25002, -1
            )

    def test_register_peer_with_string_port(self):
        with self.assertRaises(TypeError):
            self.node.register_peer(
                "node_3", "127.0.0.1", "25002", 9997
            )

    def test_register_self_as_peer(self):
        with self.assertRaises(ValueError):
            self.node.register_peer(
                "node_1", "127.0.0.1", 25002, 9997
            )

    def test_update_peer_with_empty_host(self):
        with self.assertRaises(ValueError):
            self.node.update_peer("node_2", host="")

    def test_update_peer_with_invalid_tcp_port(self):
        with self.assertRaises(ValueError):
            self.node.update_peer("node_2", tcp_port=70000)

    def test_update_peer_with_invalid_udp_port(self):
        with self.assertRaises(ValueError):
            self.node.update_peer("node_2", udp_port=-1)

    def test_update_peer_with_string_port(self):
        with self.assertRaises(TypeError):
            self.node.update_peer("node_2", tcp_port="25002")

    def test_update_peer_does_not_partially_modify(self):
        original_peer = self.node.get_peer("node_2")

        with self.assertRaises(ValueError):
            self.node.update_peer(
                "node_2",
                host="192.168.1.10",
                tcp_port=70000,
            )

        self.assertEqual(
            self.node.get_peer("node_2"),
            original_peer,
        )


if __name__ == "__main__":
    unittest.main()
