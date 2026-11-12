
import asyncio

from networking.protocol import (
    create_message,
    encode_message,
    decode_message,
)

NODE_ID = "node_server"

class UDPServer(asyncio.DatagramProtocol):

    def __init__(self, node=None):
        # Reuse the existing MeshNode when provided.
        # Otherwise, create one for standalone server usage.
        from networking.node import MeshNode

        self.node = node if node is not None else MeshNode(
            node_id=NODE_ID
        )

        self.tasks = set()
        self.transport = None

        # Register the default PING handler only if not already registered.
        if "PING" not in self.node.handlers:
            self.node.register_handler("PING", self.handle_ping)

    async def handle_ping(self, message):
        """Handle PING messages."""

        return create_message(
            message_type="PONG",
            sender_id=self.node.node_id,
            receiver_id=message["sender_id"],
            payload={
                "message": "UDP connection successful",
            },
        )

    def connection_made(self, transport):
        self.transport = transport
        print("UDP server is running")

    def datagram_received(self, data, addr):
        """Receive a datagram and schedule asynchronous processing."""

        try:
            message = decode_message(data.decode("utf-8"))

        except (UnicodeDecodeError, ValueError):
            response = create_message(
                message_type="ERROR",
                sender_id=self.node.node_id,
                receiver_id="unknown",
                payload={"message": "Invalid UDP message"},
            )

            self.send_response(response, addr)
            return

        task = asyncio.create_task(
            self.process_message(message, addr)
        )

        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def process_message(self, message, addr):
        """Dispatch the message through the shared MeshNode."""

        try:
            response = await self.node.handle_message(message)

            if response is not None:
                response["payload"]["request_message_id"] = message["message_id"]

                self.send_response(response, addr)

        except Exception as exc:
            print(f"Error processing UDP message: {exc}")

    def send_response(self, response, addr):
        """Encode and send a response."""

        if self.transport is None:
            return

        encoded_response = encode_message(response).encode("utf-8")
        self.transport.sendto(encoded_response, addr)

    def error_received(self, exc):
        print(f"UDP transport error: {exc}")

    def connection_lost(self, exc):
        if exc:
            print(f"UDP server closed with error: {exc}")

    async def close(self):
        """Cancel pending tasks when shutting down."""

        for task in self.tasks:
            task.cancel()

        if self.tasks:
            await asyncio.gather(
                *self.tasks,
                return_exceptions=True,
            )


async def main():
    from networking.node import MeshNode

    node = MeshNode(
        node_id=NODE_ID,
        host="127.0.0.1",
        tcp_port=25000,
        udp_port=9999,
    )

    await node.start()

    try:
        print("TCP and UDP servers are running.")
        print("Press Ctrl+C to stop.")

        await asyncio.Future()

    finally:
        await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nUDP server stopped")
