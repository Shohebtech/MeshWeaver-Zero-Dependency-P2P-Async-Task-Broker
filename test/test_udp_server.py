
import asyncio
import socket
import unittest

from networking.udp_server import UDPServer


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

        await loop.sock_sendto(
            self.client_socket,
            b"PING",
            self.server_address,
        )

        response, _ = await asyncio.wait_for(
            loop.sock_recvfrom(self.client_socket, 1024),
            timeout=2,
        )

        self.assertEqual(response, b"PONG")

    
    async def test_unexpected_message_receives_error_response(self):
        loop = asyncio.get_running_loop()

        await loop.sock_sendto(
            self.client_socket,
            b"HELLO",
            self.server_address,
        )

        response, _ = await asyncio.wait_for(
            loop.sock_recvfrom(self.client_socket, 1024),
            timeout=2,
        )

        self.assertEqual(response, b"UNKNOWN_MESSAGE")



if __name__ == "__main__":
    unittest.main()
