
import asyncio

from networking.protocol import (
    create_message,
    encode_message,
    decode_message,
)


SERVER_ADDRESS = ("127.0.0.1", 9999)
RESPONSE_TIMEOUT = 3


class UDPClient(asyncio.DatagramProtocol):

    def __init__(self, response_future):
        self.response_future = response_future
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

        request = create_message(
            "PING",
            "node_client",
            "node_server",
        )

        message = encode_message(request).encode("utf-8")

        self.transport.sendto(message, SERVER_ADDRESS)

        print(f"Sent PING to {SERVER_ADDRESS}")

    def datagram_received(self, data, addr):
        try:
            message = decode_message(data.decode("utf-8"))

            print(f"Received from {addr}: {message}")

            if not self.response_future.done():
                self.response_future.set_result(message)

        except (UnicodeDecodeError, ValueError) as exc:
            print(f"Invalid UDP response: {exc}")

    def error_received(self, exc):
        print(f"UDP client error: {exc}")

    def connection_lost(self, exc):
        if exc:
            print(f"UDP client connection lost: {exc}")


async def main():
    loop = asyncio.get_running_loop()
    response_future = loop.create_future()

    transport, _ = await loop.create_datagram_endpoint(
        lambda: UDPClient(response_future),
        local_addr=("127.0.0.1", 0),
    )

    try:
        response = await asyncio.wait_for(
            response_future,
            timeout=RESPONSE_TIMEOUT,
        )

        if (
            response["type"] == "PONG"
            and response["sender_id"] == "node_server"
            and response["receiver_id"] == "node_client"
        ):
            print("Success: UDP ping/pong completed.")

        elif response["type"] == "ERROR":
            print(f"UDP server error: {response['payload']}")

        else:
            print(f"Unexpected response: {response}")

    except asyncio.TimeoutError:
        print("Timeout: UDP server did not respond.")

    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
