
import asyncio
import json

HOST = "127.0.0.1"
PORT = 25000


async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")
    print(f"TCP client connected: {peer}")

    try:
        while True:
            data = await reader.readline()

            if not data:
                break

            try:
                message = json.loads(data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                response = {
                    "type": "error",
                    "message": "Invalid JSON message",
                }
           
            else:
                if not isinstance(message, dict):
                    response = {
                        "type": "error",
                        "message": "Message must be a JSON object",
                    }
                else:
                    print(f"Received from {peer}: {message}")

                    if message.get("type") == "ping":
                        response = {
                            "type": "pong",
                            "message": "TCP connection successful",
                        }
                    else:
                        response = {
                            "type": "error",
                            "message": "Unsupported message type",
                        }


            writer.write(
                (json.dumps(response) + "\n").encode("utf-8")
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
