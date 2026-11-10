
import asyncio
import socket
import unittest

from networking.udp_server import UDPServer
from networking.protocol import create_message, encode_message, decode_message


class TestUDPServer(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        loop = asyncio.get_running_loop()

        self.transport, _ = await loop.create_datagram_endpoint(
            UDPServer,
            local_addr=("127.0.0.1", 0),
        )

        self.server_address = self.transport.get_extra_info("sockname")

        self.client_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        )
        self.client_socket.setblocking(False)

    async def asyncTearDown(self):
        self.client_socket.close()
        self.transport.close()
        await asyncio.sleep(0)

    async def test_ping_receives_pong(self):
        loop = asyncio.get_running_loop()

        # Create a valid PING protocol message
        request = create_message(
            "PING",
            "test_client",
            "node_server",
        )

        # Send the encoded message to the UDP server
        await loop.sock_sendto(
            self.client_socket,
            encode_message(request).encode("utf-8"),
            self.server_address,
        )

        # Receive the server response
        response, _ = await asyncio.wait_for(
            loop.sock_recvfrom(self.client_socket, 1024),
            timeout=2,
        )

        # Decode the response
        message = decode_message(response.decode("utf-8"))

        # Verify the PONG response
        self.assertEqual(message["type"], "PONG")
        self.assertEqual(message["sender_id"], "node_server")
        self.assertEqual(message["receiver_id"], "test_client")

        # Verify that the response belongs to the original request
        self.assertEqual(
            message["payload"]["request_message_id"],
            request["message_id"],
        )

    async def test_unexpected_message_receives_error_response(self):
        loop = asyncio.get_running_loop()

        # Create a valid but unsupported TASK message
        request = create_message(
            "TASK",
            "test_client",
            "node_server",
            {"task": "example"},
        )

        await loop.sock_sendto(
            self.client_socket,
            encode_message(request).encode("utf-8"),
            self.server_address,
        )

        # Receive the server response
        response, _ = await asyncio.wait_for(
            loop.sock_recvfrom(self.client_socket, 1024),
            timeout=2,
        )

        # Decode and verify the ERROR response
        message = decode_message(response.decode("utf-8"))

        self.assertEqual(message["type"], "ERROR")
        self.assertEqual(
            message["payload"]["message"],
            "Unsupported message type",
        )


if __name__ == "__main__":
    unittest.main()
