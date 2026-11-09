
import asyncio

from networking.protocol import create_message, encode_message, decode_message


SERVER_HOST = "127.0.0.1"
SERVER_PORT = 25000
TIMEOUT = 3

NODE_ID = "node_client"
SERVER_ID = "node_server"


async def main():
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(SERVER_HOST, SERVER_PORT),
            timeout=TIMEOUT,
        )

        try:
            # Create a standard protocol message
            request = create_message(
                message_type="PING",
                sender_id=NODE_ID,
                receiver_id=SERVER_ID,
            )

            # Encode the message into JSON
            encoded_request = encode_message(request)

            writer.write(
                (encoded_request + "\n").encode("utf-8")
            )
            await writer.drain()

            print(f"Sent request: {request}")

            # Wait for the server response
            data = await asyncio.wait_for(
                reader.readline(),
                timeout=TIMEOUT,
            )

            if not data:
                print("Server closed the connection without a response.")
                return

            # Decode and validate the response
            response = decode_message(data.decode("utf-8"))

            print(f"Received response: {response}")

            if response.get("type") == "PONG":
                print("Success: TCP communication completed.")
            elif response.get("type") == "ERROR":
                print(f"Server error: {response.get('payload')}")
            else:
                print("Unexpected response received.")

        finally:
            writer.close()
            await writer.wait_closed()

    except asyncio.TimeoutError:
        print("Timeout: TCP server did not respond in time.")

    except ConnectionRefusedError:
        print("Connection refused: start the TCP server first.")

    except (ConnectionError, UnicodeDecodeError, ValueError) as exc:
        print(f"Communication failed: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
