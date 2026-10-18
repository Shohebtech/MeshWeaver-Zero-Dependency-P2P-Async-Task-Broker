import asyncio
from typing import Callable, Optional, Tuple

Address = Tuple[str, int]
Receiver = Callable[[bytes, Address], None]


class _DatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_datagram: Receiver):
        self._on_datagram = on_datagram

    def datagram_received(self, data: bytes, addr) -> None:
        self._on_datagram(data, (addr[0], addr[1]))

    def error_received(self, exc: Exception) -> None:
        pass  # UDP errors (e.g. peer gone) are normal; timeouts handle them


class UDPTransport:

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port  # 0 = let the OS pick a free port
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._receiver: Optional[Receiver] = None

    @property
    def local_addr(self) -> Address:
        return (self.host, self.port)

    def set_receiver(self, callback: Receiver) -> None:
        self._receiver = callback

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _DatagramProtocol(self._handle),
            local_addr=(self.host, self.port),
        )
        self._transport = transport
        sockname = transport.get_extra_info("sockname")
        self.host, self.port = sockname[0], sockname[1]

    def _handle(self, data: bytes, addr: Address) -> None:
        if self._receiver is not None:
            self._receiver(data, addr)

    def send(self, data: bytes, addr: Address) -> None:
        if self._transport is None:
            raise RuntimeError("transport is not started")
        self._transport.sendto(data, addr)

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None