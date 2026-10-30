import asyncio
import inspect
import struct
from typing import Any, Awaitable, Callable, Optional, Tuple, Union

Address = Tuple[str, int]
Handler = Callable[[bytes, Address], Union[bytes, Awaitable[bytes]]]

HEADER = struct.Struct(">I")        # 4 bytes: how long the payload is
MAX_FRAME_BYTES = 1024 * 1024       # 1 MB: refuse anything bigger
TASK_PORT_OFFSET = 1000             # task port = DHT port + 1000 (an assumption)


class TaskChannelError(Exception):
    """A task could not be sent, or the reply could not be read."""


def task_port(dht_port: int, offset: int = TASK_PORT_OFFSET) -> int:
    """The TCP port where a node with this DHT port receives tasks."""
    port = dht_port + offset
    if not 0 < port < 65536:
        raise ValueError("task port out of range")
    return port


def _frame(data: bytes) -> bytes:
    return HEADER.pack(len(data)) + data


async def _read_frame(reader: asyncio.StreamReader, max_bytes: int, timeout: float) -> bytes:
    header = await asyncio.wait_for(reader.readexactly(HEADER.size), timeout)
    (size,) = HEADER.unpack(header)
    if size == 0 or size > max_bytes:
        raise TaskChannelError(f"frame of {size} bytes refused")  # checked BEFORE reading it
    return await asyncio.wait_for(reader.readexactly(size), timeout)


class TaskServer:
    """
    Receives one message per connection, passes the bytes to handler(data, peer_addr)
    and sends back whatever bytes the handler returns. The handler may be a normal
    or an async function. If it fails, the connection is closed without a reply.
    """

    def __init__(self, handler: Handler, host: str = "127.0.0.1", port: int = 0,
                 max_bytes: int = MAX_FRAME_BYTES, read_timeout: float = 10.0):
        self._handler = handler
        self.host, self.port = host, port
        self.max_bytes = max_bytes
        self.read_timeout = read_timeout
        self._server: Optional[asyncio.AbstractServer] = None

    @property
    def address(self) -> Address:
        if self._server is None:
            raise RuntimeError("server is not started")
        sockname = self._server.sockets[0].getsockname() #type: ignore 
        return (sockname[0], sockname[1])

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("server is already started")
        self._server = await asyncio.start_server(self._serve, self.host, self.port)

    async def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()
            await server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        try:
            data = await _read_frame(reader, self.max_bytes, self.read_timeout)
            reply: Any = self._handler(data, (peer[0], peer[1]))
            if inspect.isawaitable(reply):
                reply = await reply
            if not isinstance(reply, bytes) or not 0 < len(reply) <= self.max_bytes:
                raise TaskChannelError("handler returned an unusable reply")
            writer.write(_frame(reply))
            await writer.drain()
        except Exception:
            pass  # bad frame, timeout, broken connection or failing handler: just hang up
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


async def send_task(host: str, port: int, data: bytes, timeout: float = 10.0,
                    max_bytes: int = MAX_FRAME_BYTES) -> bytes:
    """Send bytes to a TaskServer and return its reply. Raises TaskChannelError on any failure."""
    if not isinstance(data, bytes) or not 0 < len(data) <= max_bytes:
        raise ValueError(f"data must be 1 to {max_bytes} bytes")
    writer: Optional[asyncio.StreamWriter] = None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        writer.write(_frame(data))
        await writer.drain()
        return await _read_frame(reader, max_bytes, timeout)
    except (asyncio.TimeoutError, asyncio.IncompleteReadError, OSError) as e:
        raise TaskChannelError(f"sending to {host}:{port} failed ({type(e).__name__})") from e
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass