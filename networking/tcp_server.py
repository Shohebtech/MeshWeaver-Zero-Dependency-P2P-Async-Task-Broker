
import asyncio

from networking.protocol import (
    create_message,
    encode_message,
    decode_message,
)


HOST = "127.0.0.1"
PORT = 25000

NODE_ID = "node_server"


def create_error_response(receiver_id, error_message):
    return create_message(
        message_type="ERROR",
        sender_id=NODE_ID,
        receiver_id=receiver_id,
        payload={
            "message": error_message,
        },
    )


async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")

    print(f"TCP client connected: {peer}")

    try:
        while True:
            data = await reader.readline()

            if not data:
                break

            try:
                # Decode and validate incoming message
                message = decode_message(data.decode("utf-8"))

            except (UnicodeDecodeError, ValueError) as exc:
                response = create_error_response(
                    receiver_id="unknown",
                    error_message=str(exc),
                )

            else:
                print(f"Received from {peer}: {message}")

                message_type = message["type"]
                sender_id = message["sender_id"]

                if message_type == "PING":

                    response = create_message(
                        message_type="PONG",
                        sender_id=NODE_ID,
                        receiver_id=sender_id,
                        payload={
                            "message": "TCP connection successful",
                            "request_message_id": message["message_id"],
                        },
                    )

                else:
                    response = create_error_response(
                        receiver_id=sender_id,
                        error_message="Unsupported message type",
                    )

            # Encode response using the common protocol
            encoded_response = encode_message(response)

            writer.write(
                (encoded_response + "\n").encode("utf-8")
            )

            await writer.drain()

    except (ConnectionError, asyncio.CancelledError):
        raise

    finally:
        writer.close()
        await writer.wait_closed()

        print(f"TCP client disconnected: {peer}")


async def main():
    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT,
    )

    addresses = ", ".join(
        str(sock.getsockname()) for sock in server.sockets
    )

    print(f"TCP server listening on {addresses}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nTCP server stopped")
