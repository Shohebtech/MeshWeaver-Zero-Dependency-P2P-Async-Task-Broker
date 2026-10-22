
import asyncio


class UDPServer(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        print("UDP server is running on 127.0.0.1:9999")

    def datagram_received(self, data, addr):
        message = data.decode("utf-8")
        print(f"Received from {addr}: {message}")

        if message == "PING":
            self.transport.sendto(b"PONG", addr)
            print(f"Sent PONG to {addr}")


async def main():
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        UDPServer,
        local_addr=("127.0.0.1", 9999)
    )

    try:
        await asyncio.Future()
    finally:
        transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nUDP server stopped")
