import asyncio
import socket
import sys
from typing import Callable, Optional, Tuple

Address = Tuple[str, int]
Receiver = Callable[[bytes, Address], None]


def _ignore_icmp_resets(sock: socket.socket) -> None:
    """Windows only: stop 'port unreachable' replies from breaking a UDP socket."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        from ctypes import wintypes

        ws2 = ctypes.WinDLL("ws2_32")
        ws2.WSAIoctl.argtypes = [
            ctypes.c_size_t, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD,
            ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD),
            ctypes.c_void_p, ctypes.c_void_p,
        ]
        ws2.WSAIoctl.restype = ctypes.c_int
        flag = wintypes.DWORD(0)
        returned = wintypes.DWORD(0)
        # 0x9800000C = SIO_UDP_CONNRESET, value 0 = switch the behaviour off
        ws2.WSAIoctl(sock.fileno(), 0x9800000C, ctypes.byref(flag), 4,
                     None, 0, ctypes.byref(returned), None, None)
    except Exception:
        pass  # best effort: never stop the node from starting


class _DatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_datagram: Receiver):
        self._on_datagram = on_datagram

    def datagram_received(self, data: bytes, addr) -> None:
        self._on_datagram(data, (addr[0], addr[1]))

    def error_received(self, exc: Exception) -> None:
        pass  # UDP errors (e.g. peer gone) are normal; timeouts handle them


class UDPTransport:
    """
    Stand-in for Module 1's UDP layer. The DHT only needs:
      start()               open the socket
      send(data, addr)      fire-and-forget a datagram
      set_receiver(cb)      cb(data, addr) is called for every datagram received
      local_addr            (host, port) we are listening on
      close()               shut down
    """

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
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setblocking(False)
            _ignore_icmp_resets(sock)
            sock.bind((self.host, self.port))  # raises OSError if the port is taken
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _DatagramProtocol(self._handle), sock=sock)
        except BaseException:
            sock.close()
            raise
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