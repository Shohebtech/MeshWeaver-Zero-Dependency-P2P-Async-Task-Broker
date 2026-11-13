
import asyncio
import ssl

from networking.protocol import (
    create_message,
    encode_message,
    decode_message,
)
from networking.node import MeshNode


HOST = "127.0.0.1"
PORT = 25000

NODE_ID = "node_server"


# Create the networking integration interface
node = MeshNode(node_id=NODE_ID)


async def handle_ping(message):
    """
    Handle PING messages.

    This handler can later be replaced or extended
    by other modules registering their own handlers.
    """

    return create_message(
        message_type="PONG",
        sender_id=NODE_ID,
        receiver_id=message["sender_id"],
        payload={
            "message": "TCP connection successful",
        },
    )


# Register the PING handler with MeshNode
node.register_handler("PING", handle_ping)


def create_error_response(receiver_id, error_message):
    """Create a standard ERROR response."""

    return create_message(
        message_type="ERROR",
        sender_id=NODE_ID,
        receiver_id=receiver_id,
        payload={
            "message": error_message,
        },
    )


async def handle_client(reader, writer):
    """Handle multiple messages from a TCP client."""

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

               # Delegate message processing to MeshNode
                response = await node.handle_message(message)

                # Attach original request ID to every response
                if response is not None:
                    response["payload"]["request_message_id"] = message["message_id"]

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


def create_ssl_context():
    """Create TLS context for the TCP server."""

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    ssl_context.load_cert_chain(
        certfile="certificates/server-cert.pem",
        keyfile="certificates/server-key.pem",
    )

    return ssl_context

async def main():
    ssl_context = create_ssl_context()

    server = await asyncio.start_server(
        handle_client,
        HOST,
        PORT,
        ssl=ssl_context,
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
