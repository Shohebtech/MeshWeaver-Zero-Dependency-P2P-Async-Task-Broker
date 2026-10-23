
import asyncio
import json
import unittest

from networking.tcp_server import handle_client


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
        request = json.dumps({"type": "ping"}).encode() + b"\n"

        response = await self.send_message(request)
        result = json.loads(response.decode())

        self.assertEqual(result["type"], "pong")

    async def test_unsupported_message_returns_error(self):
        request = json.dumps(
            {"type": "hello"}
        ).encode() + b"\n"

        response = await self.send_message(request)
        result = json.loads(response.decode())

        self.assertEqual(result["type"], "error")
        self.assertEqual(
            result["message"],
            "Unsupported message type",
        )

    async def test_invalid_json_returns_error(self):
        response = await self.send_message(b"{invalid json}\n")
        result = json.loads(response.decode())

        self.assertEqual(result["type"], "error")
        self.assertEqual(result["message"], "Invalid JSON message")

    async def test_client_disconnection_is_handled(self):
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            self.port,
        )

        writer.close()
        await writer.wait_closed()

        data = await asyncio.wait_for(reader.readline(), timeout=2)
        self.assertEqual(data, b"")


if __name__ == "__main__":
    unittest.main()
