
import asyncio
import json

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 25000
TIMEOUT = 3


async def main():
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(SERVER_HOST, SERVER_PORT),
            timeout=TIMEOUT,
        )

        try:
            request = {"type": "ping"}

            writer.write(
                (json.dumps(request) + "\n").encode("utf-8")
            )
            await writer.drain()

            print(f"Sent request: {request}")

            data = await asyncio.wait_for(
                reader.readline(),
                timeout=TIMEOUT,
            )

            if not data:
                print("Server closed the connection without a response.")
                return

            response = json.loads(data.decode("utf-8"))
            print(f"Received response: {response}")

            if response.get("type") == "pong":
                print("Success: TCP communication completed.")
            else:
                print("The server returned an error response.")

        finally:
            writer.close()
            await writer.wait_closed()

    except asyncio.TimeoutError:
        print("Timeout: TCP server did not respond in time.")
    except ConnectionRefusedError:
        print("Connection refused: start the TCP server first.")
    except (ConnectionError, json.JSONDecodeError) as exc:
        print(f"Communication failed: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
