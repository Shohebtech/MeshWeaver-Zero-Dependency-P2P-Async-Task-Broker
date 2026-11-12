
import asyncio
import unittest

from networking.node import UDPResponseProtocol
from networking.protocol import create_message, encode_message


class TestUDPResponseProtocol(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.loop = asyncio.get_running_loop()

        self.request = create_message(
            message_type="PING",
            sender_id="node_client",
            receiver_id="node_server",
            payload={"message": "Hello"},
        )

        self.address = ("127.0.0.1", 9999)

        self.response_future = self.loop.create_future()

        self.protocol = UDPResponseProtocol(
            response_future=self.response_future,
            encoded_message=encode_message(
                self.request
            ).encode("utf-8"),
            address=self.address,
        )

    def send_response(self, sender_id, receiver_id, request_id):
        response = create_message(
            message_type="PONG",
            sender_id=sender_id,
            receiver_id=receiver_id,
            payload={
                "request_message_id": request_id,
            },
        )

        self.protocol.datagram_received(
            encode_message(response).encode("utf-8"),
            self.address,
        )

    async def test_valid_pong_response(self):
        self.send_response(
            "node_server",
            "node_client",
            self.request["message_id"],
        )

        response = self.response_future.result()

        self.assertEqual(response["sender_id"], "node_server")
        self.assertEqual(response["receiver_id"], "node_client")
        self.assertEqual(response["type"], "PONG")

    async def test_unexpected_sender_id(self):
        self.send_response(
            "unknown_server",
            "node_client",
            self.request["message_id"],
        )

        with self.assertRaisesRegex(
            ValueError,
            "Unexpected response sender",
        ):
            self.response_future.result()

    async def test_unexpected_receiver_id(self):
        self.send_response(
            "node_server",
            "unknown_client",
            self.request["message_id"],
        )

        with self.assertRaisesRegex(
            ValueError,
            "Unexpected response receiver",
        ):
            self.response_future.result()

    async def test_incorrect_request_message_id(self):
        self.send_response(
            "node_server",
            "node_client",
            "incorrect-message-id",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Response does not match the original request",
        ):
            self.response_future.result()


if __name__ == "__main__":
    unittest.main()
