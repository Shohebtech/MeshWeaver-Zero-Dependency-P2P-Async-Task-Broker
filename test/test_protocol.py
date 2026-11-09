
import unittest

from networking.protocol import (
    create_message,
    encode_message,
    decode_message,
    validate_message,
)


class TestProtocol(unittest.TestCase):

    def test_create_ping_message(self):
        message = create_message(
            "PING",
            "node_A",
            "node_B",
        )

        self.assertEqual(message["type"], "PING")
        self.assertEqual(message["sender_id"], "node_A")
        self.assertEqual(message["receiver_id"], "node_B")

    def test_encode_and_decode_message(self):
        message = create_message(
            "PING",
            "node_A",
            "node_B",
        )

        encoded = encode_message(message)
        decoded = decode_message(encoded)

        self.assertEqual(decoded, message)

    def test_invalid_json(self):
        with self.assertRaises(ValueError):
            decode_message("invalid json")

    def test_missing_required_field(self):
        message = {
            "sender_id": "node_A",
            "receiver_id": "node_B",
            "type": "PING",
            "payload": {},
        }

        with self.assertRaises(ValueError):
            validate_message(message)

    def test_unsupported_message_type(self):
        with self.assertRaises(ValueError):
            create_message(
                "UNKNOWN",
                "node_A",
                "node_B",
            )

    def test_message_ids_are_unique(self):
        message1 = create_message(
            "PING",
            "node_A",
            "node_B",
        )

        message2 = create_message(
            "PING",
            "node_A",
            "node_B",
        )

        self.assertNotEqual(
            message1["message_id"],
            message2["message_id"],
        )


if __name__ == "__main__":
    unittest.main()
