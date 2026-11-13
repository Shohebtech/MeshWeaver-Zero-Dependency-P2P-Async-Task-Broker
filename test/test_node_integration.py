
import unittest
import asyncio

from networking.node import MeshNode
from networking.protocol import create_message


class TestMeshNodeIntegration(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.node_1 = MeshNode(
            node_id="node_1",
            tcp_port=0,
            udp_port=0,
        )

        self.node_2 = MeshNode(
            node_id="node_2",
            tcp_port=0,
            udp_port=0,
        )

        self.node_3 = MeshNode(
            node_id="node_3",
            tcp_port=0,
            udp_port=0,
        )

        await self.node_1.start()
        await self.node_2.start()
        await self.node_3.start()

        # Register each node as a peer of the other.
        self.node_1.register_peer(
            "node_2",
            "127.0.0.1",
            self.node_2.tcp_port,
            self.node_2.udp_port,
        )

        self.node_1.register_peer(
            "node_3",
            "127.0.0.1",
            self.node_3.tcp_port,
            self.node_3.udp_port,
        )

        self.node_2.register_peer(
            "node_1",
            "127.0.0.1",
            self.node_1.tcp_port,
            self.node_1.udp_port,
        )

    async def asyncTearDown(self):
        await self.node_1.stop()
        await self.node_2.stop()
        await self.node_3.stop()

    async def test_custom_discovery_handler(self):

        async def handle_discovery(message):
            return create_message(
                message_type="DISCOVERY_RESPONSE",
                sender_id=self.node_2.node_id,
                receiver_id=message["sender_id"],
                payload={"status": "discovered"},
            )

        # Simulate another module registering its handler.
        self.node_2.register_handler(
            "DISCOVERY",
            handle_discovery,
        )

        response = await self.node_1.send_message(
            receiver_id="node_2",
            message_type="DISCOVERY",
            payload={"request": "find_peers"},
            protocol="tcp",
        )

        self.assertEqual(
            response["type"],
            "DISCOVERY_RESPONSE",
        )

        self.assertEqual(
            response["payload"]["status"],
            "discovered",
        )

    async def test_custom_discovery_handler_udp(self):

        async def handle_discovery(message):
            return create_message(
                message_type="DISCOVERY_RESPONSE",
                sender_id=self.node_2.node_id,
                receiver_id=message["sender_id"],
                payload={"status": "discovered_over_udp"},
            )

        self.node_2.register_handler(
            "DISCOVERY",
            handle_discovery,
        )

        response = await self.node_1.send_message(
            receiver_id="node_2",
            message_type="DISCOVERY",
            payload={"request": "find_peers"},
            protocol="udp",
        )

        self.assertEqual(
            response["type"],
            "DISCOVERY_RESPONSE",
        )

        self.assertEqual(
            response["payload"]["status"],
            "discovered_over_udp",
        )

    async def test_broadcast_to_real_nodes(self):
        async def handle_discovery(message):
            return create_message(
                message_type="DISCOVERY_RESPONSE",
                sender_id=self.node_2.node_id,
                receiver_id=message["sender_id"],
                payload={"status": "received"},
            )

        self.node_2.register_handler(
            "DISCOVERY",
            handle_discovery,
        )

        results = await self.node_1.broadcast(
            message_type="DISCOVERY",
            payload={"request": "find_peers"},
            protocol="tcp",
        )

        self.assertTrue(results["node_2"]["success"])

        self.assertEqual(
            results["node_2"]["response"]["type"],
            "DISCOVERY_RESPONSE",
        )

    async def test_concurrent_communication_with_multiple_peers(self):

        async def handle_discovery_node_2(message):
            return create_message(
                message_type="DISCOVERY_RESPONSE",
                sender_id=self.node_2.node_id,
                receiver_id=message["sender_id"],
                payload={"node": "node_2"},
            )

        async def handle_discovery_node_3(message):
            return create_message(
                message_type="DISCOVERY_RESPONSE",
                sender_id=self.node_3.node_id,
                receiver_id=message["sender_id"],
                payload={"node": "node_3"},
            )

        self.node_2.register_handler(
            "DISCOVERY",
            handle_discovery_node_2,
        )

        self.node_3.register_handler(
            "DISCOVERY",
            handle_discovery_node_3,
        )

        # Send requests to both peers concurrently.
        responses = await asyncio.gather(
            self.node_1.send_message(
                receiver_id="node_2",
                message_type="DISCOVERY",
                payload={"request": "find_peers"},
                protocol="tcp",
            ),
            self.node_1.send_message(
                receiver_id="node_3",
                message_type="DISCOVERY",
                payload={"request": "find_peers"},
                protocol="tcp",
            ),
        )

        self.assertEqual(
            responses[0]["payload"]["node"],
            "node_2",
        )

        self.assertEqual(
            responses[1]["payload"]["node"],
            "node_3",
        )

        self.assertNotEqual(
            responses[0]["message_id"],
            responses[1]["message_id"],
        )

    async def test_concurrent_udp_communication_with_multiple_peers(self):

        async def handle_discovery_node_2(message):
            return create_message(
                message_type="DISCOVERY_RESPONSE",
                sender_id=self.node_2.node_id,
                receiver_id=message["sender_id"],
                payload={"node": "node_2"},
            )

        async def handle_discovery_node_3(message):
            return create_message(
                message_type="DISCOVERY_RESPONSE",
                sender_id=self.node_3.node_id,
                receiver_id=message["sender_id"],
                payload={"node": "node_3"},
            )

        self.node_2.register_handler(
            "DISCOVERY",
            handle_discovery_node_2,
        )

        self.node_3.register_handler(
            "DISCOVERY",
            handle_discovery_node_3,
        )

        # Send requests to both peers concurrently.
        responses = await asyncio.gather(
            self.node_1.send_message(
                receiver_id="node_2",
                message_type="DISCOVERY",
                payload={"request": "find_peers"},
                protocol="udp",
            ),
            self.node_1.send_message(
                receiver_id="node_3",
                message_type="DISCOVERY",
                payload={"request": "find_peers"},
                protocol="udp",
            ),
        )

        self.assertEqual(
            responses[0]["payload"]["node"],
            "node_2",
        )

        self.assertEqual(
            responses[1]["payload"]["node"],
            "node_3",
        )

        self.assertNotEqual(
            responses[0]["message_id"],
            responses[1]["message_id"],
        )


if __name__ == "__main__":
    unittest.main()
