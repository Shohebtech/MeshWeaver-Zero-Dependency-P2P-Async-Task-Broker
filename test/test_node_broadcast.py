import unittest
from unittest.mock import AsyncMock

from networking.node import MeshNode


class TestMeshNodeBroadcast(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.node = MeshNode(node_id="node_1")

        self.node.register_peer(
            "node_2", "127.0.0.1", 25001, 9998
        )
        self.node.register_peer(
            "node_3", "127.0.0.1", 25002, 9997
        )

    async def test_broadcast_to_all_peers(self):
        self.node.send_message = AsyncMock(
            side_effect=[
                {"type": "PONG", "sender_id": "node_2"},
                {"type": "PONG", "sender_id": "node_3"},
            ]
        )

        results = await self.node.broadcast(
            message_type="PING",
            payload={"message": "Hello"},
            protocol="udp",
        )

        self.assertEqual(len(results), 2)
        self.assertTrue(results["node_2"]["success"])
        self.assertTrue(results["node_3"]["success"])

    async def test_broadcast_handles_peer_failure(self):
        self.node.send_message = AsyncMock(
            side_effect=[
                {"type": "PONG"},
                ConnectionError("Peer unreachable"),
            ]
        )

        results = await self.node.broadcast(
            message_type="PING",
            protocol="udp",
        )

        self.assertTrue(results["node_2"]["success"])
        self.assertFalse(results["node_3"]["success"])

    async def test_broadcast_without_peers(self):
        self.node.peers.clear()

        results = await self.node.broadcast(
            message_type="DISCOVERY"
        )

        self.assertEqual(results, {})


if __name__ == "__main__":
    unittest.main()