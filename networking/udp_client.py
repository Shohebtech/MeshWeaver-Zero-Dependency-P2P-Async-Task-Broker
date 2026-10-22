
import asyncio


SERVER_ADDRESS = ("127.0.0.1", 9999)
RESPONSE_TIMEOUT = 3


class UDPClient(asyncio.DatagramProtocol):
    def __init__(self, response_future):
        self.response_future = response_future
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(b"PING", SERVER_ADDRESS)
        print(f"Sent PING to {SERVER_ADDRESS}")

    def datagram_received(self, data, addr):
        message = data.decode("utf-8")
        print(f"Received from {addr}: {message}")

        if not self.response_future.done():
            self.response_future.set_result(message)

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

        if response == "PONG":
            print("Success: UDP ping/pong completed.")
        else:
            print(f"Unexpected response: {response}")

    except asyncio.TimeoutError:
        print("Timeout: UDP server did not respond.")

    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
