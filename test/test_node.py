
import unittest

from networking.node import MeshNode
from networking.protocol import create_message


class TestMeshNode(unittest.IsolatedAsyncioTestCase):

    async def test_registered_handler_receives_message(self):
        node = MeshNode("node_1")

        received_messages = []

        async def handle_gossip(message):
            received_messages.append(message)
            return {"status": "received"}

        node.register_handler("GOSSIP", handle_gossip)

        message = {
            "message_id": "msg-1",
            "sender_id": "node_2",
            "receiver_id": "node_1",
            "type": "GOSSIP",
            "payload": {"data": "hello"},
        }

        response = await node.handle_message(message)

        self.assertEqual(received_messages, [message])
        self.assertEqual(response, {"status": "received"})

    async def test_unknown_message_type_returns_error(self):
        node = MeshNode("node_1")

        message = {
            "message_id": "msg-2",
            "sender_id": "node_2",
            "receiver_id": "node_1",
            "type": "UNKNOWN",
            "payload": {},
        }

        response = await node.handle_message(message)

        self.assertEqual(response["type"], "ERROR")
        self.assertEqual(response["sender_id"], "node_1")

    async def test_async_handler_receives_message(self):
        node = MeshNode("node_1")

        async def handle_task(message):
            return create_message(
                message_type="RESULT",
                sender_id="node_1",
                receiver_id=message["sender_id"],
                payload={"status": "completed"},
            )

        node.register_handler("TASK", handle_task)

        message = create_message(
            message_type="TASK",
            sender_id="node_2",
            receiver_id="node_1",
            payload={"task": "example"},
        )

        response = await node.handle_message(message)

        self.assertEqual(response["type"], "RESULT")
        self.assertEqual(response["sender_id"], "node_1")
        self.assertEqual(response["receiver_id"], "node_2")
        self.assertEqual(response["payload"]["status"], "completed")

    async def test_handler_can_return_none(self):
        node = MeshNode("node_1")

        async def handle_load_update(message):
            return None

        node.register_handler("LOAD_UPDATE", handle_load_update)

        message = create_message(
            message_type="LOAD_UPDATE",
            sender_id="node_2",
            receiver_id="node_1",
            payload={"cpu_load": 0.5},
        )

        response = await node.handle_message(message)

        self.assertIsNone(response)

    async def test_handler_exception_returns_error(self):
        node = MeshNode("node_1")

        async def handle_task(message):
            raise RuntimeError("Task processing failed")

        node.register_handler("TASK", handle_task)

        message = create_message(
            message_type="TASK",
            sender_id="node_2",
            receiver_id="node_1",
            payload={"task": "example"},
        )

        response = await node.handle_message(message)

        self.assertEqual(response["type"], "ERROR")
        self.assertEqual(response["sender_id"], "node_1")
        self.assertEqual(response["receiver_id"], "node_2")
        self.assertIn(
            "Task processing failed",
            response["payload"]["message"],
        )

    async def test_message_with_correct_receiver_id(self):
        node = MeshNode("node_1")

        node.register_handler(
            "TASK",
            lambda message: create_message(
                "RESULT",
                node.node_id,
                message["sender_id"],
                {"status": "completed"},
            ),
        )

        message = create_message(
            message_type="TASK",
            sender_id="node_2",
            receiver_id="node_1",
            payload={},
        )

        response = await node.handle_message(message)

        self.assertEqual(response["type"], "RESULT")

    async def test_message_with_wrong_receiver_id(self):
        node = MeshNode("node_1")

        message = create_message(
            message_type="TASK",
            sender_id="node_2",
            receiver_id="node_3",
            payload={},
        )

        response = await node.handle_message(message)

        self.assertEqual(response["type"], "ERROR")
        self.assertEqual(
            response["payload"]["message"],
            "Message is not intended for this node",
        )


if __name__ == "__main__":
    unittest.main()
