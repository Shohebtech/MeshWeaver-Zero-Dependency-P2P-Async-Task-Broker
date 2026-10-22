
import asyncio


class UDPServer(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        print("UDP server is running on 127.0.0.1:9999")

    def datagram_received(self, data, addr):
        try:
            message = data.decode("utf-8").strip()
        except UnicodeDecodeError:
            print(f"Invalid UTF-8 message received from {addr}")
            self.transport.sendto(b"INVALID_MESSAGE", addr)
            return

        print(f"Received from {addr}: {message}")

        if message == "PING":
            response = b"PONG"
        else:
            response = b"UNKNOWN_MESSAGE"

        self.transport.sendto(response, addr)
        print(f"Sent {response.decode()} to {addr}")

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
