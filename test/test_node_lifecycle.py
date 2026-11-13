
import unittest
import asyncio

from networking.node import MeshNode


class TestMeshNodeLifecycle(unittest.IsolatedAsyncioTestCase):

    async def test_start_node(self):
        node = MeshNode(
            node_id="test_node",
            tcp_port=0,
            udp_port=0,
        )

        await node.start()

        try:
            self.assertTrue(node.is_running)
            self.assertIsNotNone(node.tcp_server)
            self.assertIsNotNone(node.udp_transport)

            self.assertGreater(node.tcp_port, 0)
            self.assertGreater(node.udp_port, 0)

        finally:
            await node.stop()

    async def test_stop_node(self):
        node = MeshNode(
            node_id="test_node",
            tcp_port=0,
            udp_port=0,
        )

        await node.start()
        await node.stop()

        self.assertFalse(node.is_running)
        self.assertIsNone(node.tcp_server)
        self.assertIsNone(node.udp_transport)

    async def test_start_twice(self):
        node = MeshNode(
            node_id="test_node",
            tcp_port=0,
            udp_port=0,
        )

        await node.start()
        original_port = node.tcp_port

        try:
            await node.start()
            self.assertEqual(node.tcp_port, original_port)
            self.assertTrue(node.is_running)

        finally:
            await node.stop()

    async def test_restart_node(self):
        node = MeshNode(
            node_id="test_node",
            tcp_port=0,
            udp_port=0
        )

        await node.start()
        await node.stop()

        await node.start()

        try:
            self.assertTrue(node.is_running)
            self.assertIsNotNone(node.tcp_server)
            self.assertIsNotNone(node.udp_transport)
        finally:
            await node.stop()

    async def test_send_message_to_unreachable_peer(self):
        node = MeshNode(node_id="test_node")

        await node.start()

        try:
            # Register a peer on a port where no server is running
            node.register_peer(
                peer_id="unreachable_peer",
                host="127.0.0.1",
                tcp_port=59999,
                udp_port=59998,
            )

            with self.assertRaises(ConnectionRefusedError):
                await node.send_message(
                    receiver_id="unreachable_peer",
                    message_type="PING",
                    protocol="tcp",
                )

        finally:
            await node.stop()


    async def test_send_message_to_unreachable_udp_peer(self):
        node = MeshNode(node_id="test_node")

        await node.start()

        try:
            node.register_peer(
                peer_id="unreachable_peer",
                host="127.0.0.1",
                tcp_port=59999,
                udp_port=59998,
            )

            with self.assertRaises(asyncio.TimeoutError):
                await node.send_message(
                    receiver_id="unreachable_peer",
                    message_type="PING",
                    protocol="udp",
                    timeout=1,
                )

        finally:
            await node.stop()
