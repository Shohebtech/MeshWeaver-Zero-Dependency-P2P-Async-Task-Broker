
import asyncio
import unittest

from networking.node import MeshNode
from networking.tcp_server import handle_client
from networking.udp_server import UDPServer
from networking.protocol import create_message


class TestMeshNodeTransport(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        # Start a temporary TCP server
        self.tcp_server = await asyncio.start_server(
            handle_client,
            "127.0.0.1",
            0,
        )

        self.tcp_port = self.tcp_server.sockets[0].getsockname()[1]

        # Start a temporary UDP server
        loop = asyncio.get_running_loop()

        self.udp_transport, self.udp_protocol = (
            await loop.create_datagram_endpoint(
                UDPServer,
                local_addr=("127.0.0.1", 0),
            )
        )

        self.udp_port = self.udp_transport.get_extra_info(
            "sockname"
        )[1]

        # Create the sending node
        self.node = MeshNode("test_client")

        self.node.register_peer(
            peer_id="node_server",
            host="127.0.0.1",
            tcp_port=self.tcp_port,
            udp_port=self.udp_port,
        )

    async def asyncTearDown(self):
        self.tcp_server.close()
        await self.tcp_server.wait_closed()

        self.udp_transport.close()

    # ---------------------------------
    # Basic TCP communication
    # ---------------------------------

    async def test_send_message_over_tcp(self):
        response = await self.node.send_message(
            receiver_id="node_server",
            message_type="PING",
            payload={"message": "Hello TCP"},
            protocol="tcp",
        )

        self.assertEqual(response["type"], "PONG")
        self.assertEqual(response["sender_id"], "node_server")
        self.assertEqual(response["receiver_id"], "test_client")

    # ---------------------------------
    # Basic UDP communication
    # ---------------------------------

    async def test_send_message_over_udp(self):
        response = await self.node.send_message(
            receiver_id="node_server",
            message_type="PING",
            payload={"message": "Hello UDP"},
            protocol="udp",
        )

        self.assertEqual(response["type"], "PONG")
        self.assertEqual(response["sender_id"], "node_server")
        self.assertEqual(response["receiver_id"], "test_client")

    # ---------------------------------
    # Custom TCP handler integration
    # ---------------------------------

    async def test_custom_task_handler_over_tcp(self):

        async def handle_task(message):
            return create_message(
                message_type="RESULT",
                sender_id="node_server",
                receiver_id=message["sender_id"],
                payload={
                    "status": "completed",
                    "task": message["payload"]["task"],
                },
            )

        from networking.tcp_server import node as server_node

        server_node.register_handler("TASK", handle_task)

        try:
            response = await self.node.send_message(
                receiver_id="node_server",
                message_type="TASK",
                payload={"task": "calculate_sum"},
                protocol="tcp",
            )

            self.assertEqual(response["type"], "RESULT")
            self.assertEqual(response["sender_id"], "node_server")
            self.assertEqual(response["receiver_id"], "test_client")
            self.assertEqual(
                response["payload"]["status"],
                "completed",
            )
            self.assertEqual(
                response["payload"]["task"],
                "calculate_sum",
            )

            # Verify TCP response contains the original request ID.
            self.assertIn(
                "request_message_id",
                response["payload"],
            )

            self.assertTrue(
                response["payload"]["request_message_id"]
            )

        finally:
            server_node.unregister_handler("TASK")

    # ---------------------------------
    # Custom UDP handler integration
    # ---------------------------------

    async def test_custom_task_handler_over_udp(self):

        async def handle_task(message):
            return create_message(
                message_type="RESULT",
                sender_id="node_server",
                receiver_id=message["sender_id"],
                payload={
                    "status": "completed",
                    "task": message["payload"]["task"],
                },
            )

        self.udp_protocol.node.register_handler(
            "TASK",
            handle_task,
        )

        try:
            response = await self.node.send_message(
                receiver_id="node_server",
                message_type="TASK",
                payload={"task": "calculate_sum"},
                protocol="udp",
            )

            self.assertEqual(response["type"], "RESULT")
            self.assertEqual(response["sender_id"], "node_server")
            self.assertEqual(response["receiver_id"], "test_client")
            self.assertEqual(
                response["payload"]["status"],
                "completed",
            )
            self.assertEqual(
                response["payload"]["task"],
                "calculate_sum",
            )
            # Verify UDP response contains the original request ID.
            self.assertIn(
                "request_message_id",
                response["payload"],
            )

            self.assertTrue(
                response["payload"]["request_message_id"]
            )

        finally:
            self.udp_protocol.node.unregister_handler("TASK")


if __name__ == "__main__":
    unittest.main()
