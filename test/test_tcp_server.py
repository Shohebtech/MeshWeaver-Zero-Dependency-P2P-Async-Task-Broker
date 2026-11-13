
import asyncio
import json
import unittest

from networking.tcp_server import handle_client
from networking.protocol import create_message, encode_message, decode_message


class TestTCPServer(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.server = await asyncio.start_server(
            handle_client,
            "127.0.0.1",
            0,
        )

        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    async def send_message(self, message):
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            self.port,
        )

        try:
            writer.write(message)
            await writer.drain()

            response = await asyncio.wait_for(
                reader.readline(),
                timeout=2,
            )

            return response

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_ping_receives_pong(self):
        request = create_message(
            "PING",
            "test_client",
            "node_server",
        )

        response = await self.send_message(
            (encode_message(request) + "\n").encode()
        )

        result = decode_message(response.decode())

        self.assertEqual(result["type"], "PONG")
        self.assertEqual(result["sender_id"], "node_server")
        self.assertEqual(result["receiver_id"], "test_client")

    async def test_multiple_messages_on_same_connection(self):
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            self.port,
        )

        try:
            first_message = create_message(
                "PING",
                "test_client",
                "node_server",
            )

            second_message = create_message(
                "PING",
                "test_client",
                "node_server",
            )

            first_encoded = (
                encode_message(first_message) + "\n"
            ).encode()

            second_encoded = (
                encode_message(second_message) + "\n"
            ).encode()

            writer.write(first_encoded + second_encoded)
            await writer.drain()

            first_response = await asyncio.wait_for(
                reader.readline(),
                timeout=2,
            )

            second_response = await asyncio.wait_for(
                reader.readline(),
                timeout=2,
            )

            first_result = decode_message(
                first_response.decode()
            )

            second_result = decode_message(
                second_response.decode()
            )

            self.assertEqual(first_result["type"], "PONG")
            self.assertEqual(second_result["type"], "PONG")

        finally:
            writer.close()
            await writer.wait_closed()

    async def test_unsupported_message_returns_error(self):
        request = {
            "message_id": "test-message-1",
            "sender_id": "test_client",
            "receiver_id": "node_server",
            "type": "HELLO",
            "payload": {},
        }

        response = await self.send_message(
            (json.dumps(request) + "\n").encode()
        )

        result = decode_message(response.decode())

        self.assertEqual(result["type"], "ERROR")

    async def test_invalid_json_returns_error(self):
        response = await self.send_message(
            b"{invalid json}\n"
        )

        result = decode_message(response.decode())

        self.assertEqual(result["type"], "ERROR")

    async def test_client_disconnection_is_handled(self):
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            self.port,
        )

        writer.close()
        await writer.wait_closed()

        data = await asyncio.wait_for(
            reader.readline(),
            timeout=2,
        )

        self.assertEqual(data, b"")

    async def test_json_array_returns_error(self):
        response = await self.send_message(b"[]\n")

        result = decode_message(response.decode())

        self.assertEqual(result["type"], "ERROR")


if __name__ == "__main__":
    unittest.main()
