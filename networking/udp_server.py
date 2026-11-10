
import asyncio

from networking.protocol import (
    create_message,
    encode_message,
    decode_message,
)


class UDPServer(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport

        print("UDP server is running on 127.0.0.1:9999")

    def datagram_received(self, data, addr):
        try:
            message = decode_message(data.decode("utf-8"))

        except (UnicodeDecodeError, ValueError) as exc:
            print(f"Invalid message received from {addr}: {exc}")

            response = create_message(
                "ERROR",
                "node_server",
                "unknown",
                {
                    "message": "Invalid UDP message",
                },
            )

            self.send_response(response, addr)
            return

        print(f"Received from {addr}: {message}")

        message_type = message["type"]
        sender_id = message["sender_id"]

        if message_type == "PING":

            response = create_message(
                "PONG",
                "node_server",
                sender_id,
                {
                    "message": "UDP connection successful",
                    "request_message_id": message["message_id"],
                },
            )

        else:

            response = create_message(
                "ERROR",
                "node_server",
                sender_id,
                {
                    "message": "Unsupported message type",
                },
            )

        self.send_response(response, addr)

    def send_response(self, response, addr):
        encoded_response = encode_message(response).encode("utf-8")

        self.transport.sendto(encoded_response, addr)

        print(f"Sent {response['type']} to {addr}")

    def error_received(self, exc):
        print(f"UDP transport error: {exc}")

    def connection_lost(self, exc):
        if exc:
            print(f"UDP server closed with error: {exc}")
        else:
            print("UDP server transport closed")


async def main():
    loop = asyncio.get_running_loop()

    transport, _ = await loop.create_datagram_endpoint(
        UDPServer,
        local_addr=("127.0.0.1", 9999),
    )

    try:
        print("Press Ctrl+C to stop the server.")
        await asyncio.Future()

    finally:
        transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nUDP server stopped")
