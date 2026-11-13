
import asyncio
import inspect
import socket
import ssl

from networking.protocol import (
    create_message,
    encode_message,
    decode_message,
)

DEFAULT_TIMEOUT = 3



class UDPResponseProtocol(asyncio.DatagramProtocol):
    """Receive and validate one UDP response."""

    def __init__(self, response_future, encoded_message, address):
        self.response_future = response_future
        self.encoded_message = encoded_message
        self.address = address
        self.transport = None

        # Decode the original request to identify the expected response.
        request = decode_message(encoded_message.decode("utf-8"))

        self.expected_peer_id = request["receiver_id"]
        self.expected_node_id = request["sender_id"]
        self.request_message_id = request["message_id"]
        self.request_type = request["type"]

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.encoded_message, self.address)

    def datagram_received(self, data, addr):
        """Accept only valid responses from the expected peer."""

        # 1. Validate source address.
        if addr[:2] != self.address[:2]:
            return

        try:
            response = decode_message(data.decode("utf-8"))

            # 2. Validate sender identity.
            if response["sender_id"] != self.expected_peer_id:
                raise ValueError("Unexpected response sender")

            # 3. Validate receiver identity.
            if response["receiver_id"] != self.expected_node_id:
                raise ValueError("Unexpected response receiver")

            # 4. Validate expected PING response type.
            if self.request_type == "PING":
                if response["type"] != "PONG":
                    raise ValueError("Expected PONG response")

            # 5. Validate request correlation for every response.
            if (
                response["payload"].get("request_message_id")
                != self.request_message_id
            ):
                raise ValueError("Response does not match the original request")

            if not self.response_future.done():
                self.response_future.set_result(response)

        except (UnicodeDecodeError, ValueError) as exc:
            if not self.response_future.done():
                self.response_future.set_exception(exc)

    def error_received(self, exc):
        # Windows may report a UDP port-unreachable error.
        # Ignore it and let the request timeout naturally.
        if isinstance(exc, ConnectionResetError):
            return

        if not self.response_future.done():
            self.response_future.set_exception(exc)


class MeshNode:
    """
    Common networking interface for MeshWeaver modules.

    Supports:
    - TCP and UDP server lifecycle
    - Message handler registration
    - Peer registration
    - TCP and UDP message sending
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        tcp_port: int = 0,
        udp_port: int = 0,
        ssl_context=None,
        client_ssl_context=None,
    ):
        self.node_id = node_id
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        # Optional TLS configuration
        self.ssl_context = ssl_context
        self.client_ssl_context = client_ssl_context

        self.handlers = {}
        self.peers = {}

        self.tcp_server = None
        self.udp_transport = None
        self.udp_protocol = None

        self.is_running = False

    # ---------------------------------
    # Lifecycle management
    # ---------------------------------

    async def start(self):
        """Start TCP and UDP servers."""

        if self.is_running:
            return

        try:
            self.tcp_server = await asyncio.start_server(
                self._handle_tcp_client,
                self.host,
                self.tcp_port,
                ssl=self.ssl_context,
            )

            self.udp_transport, self.udp_protocol = (
                await asyncio.get_running_loop().create_datagram_endpoint(
                    lambda: self._create_udp_protocol(),
                    local_addr=(self.host, self.udp_port),
                )
            )

            # Save actual ports when port 0 was requested.
            self.tcp_port = self.tcp_server.sockets[0].getsockname()[1]
            self.udp_port = self.udp_transport.get_extra_info("sockname")[1]

            self.is_running = True

        except Exception:
            await self.stop()
            raise

    async def stop(self):
        """Stop TCP and UDP servers."""

        if self.tcp_server is not None:
            self.tcp_server.close()
            await self.tcp_server.wait_closed()
            self.tcp_server = None

        if self.udp_protocol is not None:
            await self.udp_protocol.close()

        if self.udp_transport is not None:
            self.udp_transport.close()
            self.udp_transport = None
            self.udp_protocol = None

        self.is_running = False

    def _create_udp_protocol(self):
        """Create the UDP server protocol instance."""

        from networking.udp_server import UDPServer

        self.udp_protocol = UDPServer(node=self)
        return self.udp_protocol

    async def _handle_tcp_client(self, reader, writer):
        """Receive and dispatch TCP messages."""

        try:
            while True:
                data = await reader.readline()

                if not data:
                    break

                try:
                    message = decode_message(data.decode("utf-8"))
                    response = await self.handle_message(message)

                    if response is not None:
                        writer.write(
                            (encode_message(response) + "\n").encode("utf-8")
                        )
                        await writer.drain()

                except (UnicodeDecodeError, ValueError) as exc:
                    error = create_message(
                        message_type="ERROR",
                        sender_id=self.node_id,
                        receiver_id="unknown",
                        payload={"message": str(exc)},
                    )

                    writer.write(
                        (encode_message(error) + "\n").encode("utf-8")
                    )
                    await writer.drain()

        finally:
            writer.close()
            await writer.wait_closed()

    # ---------------------------------
    # Handler registration
    # ---------------------------------

    def register_handler(self, message_type: str, handler):
        if not callable(handler):
            raise TypeError("Handler must be callable")

        self.handlers[message_type] = handler

    def unregister_handler(self, message_type: str):
        self.handlers.pop(message_type, None)

    async def handle_message(self, message: dict):

        receiver_id = message.get("receiver_id")

        if receiver_id != self.node_id:
            return create_message(
                message_type="ERROR",
                sender_id=self.node_id,
                receiver_id=message.get("sender_id", "unknown"),
                payload={
                    "message": "Message is not intended for this node"
                },
            )

        message_type = message.get("type")
        handler = self.handlers.get(message_type)

        if handler is None:
            return create_message(
                message_type="ERROR",
                sender_id=self.node_id,
                receiver_id=message.get("sender_id", "unknown"),
                payload={"message": "Unsupported message type"},
            )

        try:
            result = handler(message)

            if inspect.isawaitable(result):
                result = await result

            return result

        except Exception as exc:
            return create_message(
                message_type="ERROR",
                sender_id=self.node_id,
                receiver_id=message.get("sender_id", "unknown"),
                payload={"message": f"Handler failed: {exc}"},
            )

    # ---------------------------------
    # Peer registration
    # ---------------------------------

    def register_peer(
        self,
        peer_id: str,
        host: str,
        tcp_port: int,
        udp_port: int,
    ):
        """Register a peer after validating its details."""

        if not isinstance(peer_id, str) or not peer_id.strip():
            raise ValueError("Peer ID must be a non-empty string")

        if not isinstance(host, str) or not host.strip():
            raise ValueError("Host must be a non-empty string")

        for name, port in (
            ("TCP", tcp_port),
            ("UDP", udp_port),
        ):
            if isinstance(port, bool) or not isinstance(port, int):
                raise TypeError(f"{name} port must be an integer")

            if not 1 <= port <= 65535:
                raise ValueError(f"{name} port must be between 1 and 65535")

        if peer_id == self.node_id:
            raise ValueError("A node cannot register itself as a peer")

        self.peers[peer_id] = {
            "host": host.strip(),
            "tcp_port": tcp_port,
            "udp_port": udp_port,
        }

    def unregister_peer(self, peer_id: str):
        """Remove a registered peer."""

        if peer_id not in self.peers:
            return False

        del self.peers[peer_id]
        return True

    def update_peer(
        self,
        peer_id: str,
        host=None,
        tcp_port=None,
        udp_port=None,
    ):
        """Update an existing peer after validating the new details."""

        if peer_id not in self.peers:
            return False

        # Validate host before changing anything.
        if host is not None:
            if not isinstance(host, str) or not host.strip():
                raise ValueError("Host must be a non-empty string")

        # Validate ports before changing anything.
        for name, port in (
            ("TCP", tcp_port),
            ("UDP", udp_port),
        ):
            if port is not None:
                if isinstance(port, bool) or not isinstance(port, int):
                    raise TypeError(f"{name} port must be an integer")

                if not 1 <= port <= 65535:
                    raise ValueError(
                        f"{name} port must be between 1 and 65535"
                    )

        # Apply updates only after all validations pass.
        if host is not None:
            self.peers[peer_id]["host"] = host.strip()

        if tcp_port is not None:
            self.peers[peer_id]["tcp_port"] = tcp_port

        if udp_port is not None:
            self.peers[peer_id]["udp_port"] = udp_port

        return True

    def get_peer(self, peer_id: str):
        """Return a copy of registered peer information."""

        peer = self.peers.get(peer_id)

        if peer is None:
            return None

        return peer.copy()

    # ---------------------------------
    # Common sending API
    # ---------------------------------

    async def send_message(
        self,
        receiver_id: str,
        message_type: str,
        payload=None,
        protocol: str = "tcp",
        timeout: float = DEFAULT_TIMEOUT,
    ):
        peer = self.peers.get(receiver_id)

        if peer is None:
            raise ValueError(
                f"Peer '{receiver_id}' is not registered"
            )

        message = create_message(
            message_type=message_type,
            sender_id=self.node_id,
            receiver_id=receiver_id,
            payload=payload if payload is not None else {},
        )

        encoded_message = encode_message(message)

        if protocol.lower() == "tcp":
            return await self._send_tcp(peer, encoded_message, timeout)

        if protocol.lower() == "udp":
            return await self._send_udp(peer, encoded_message, timeout)

        raise ValueError("Protocol must be either 'tcp' or 'udp'")

    async def broadcast(
        self,
        message_type: str,
        payload=None,
        protocol: str = "udp",
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        Send the same message to all registered peers.

        Returns a dictionary containing each peer's response
        or error.
        """

        if not self.peers:
            return {}

        async def send_to_peer(peer_id):
            try:
                response = await self.send_message(
                    receiver_id=peer_id,
                    message_type=message_type,
                    payload=payload,
                    protocol=protocol,
                    timeout=timeout,
                )

                return peer_id, {
                    "success": True,
                    "response": response,
                }

            except Exception as exc:
                return peer_id, {
                    "success": False,
                    "error": str(exc),
                }

        results = await asyncio.gather(
            *[
                send_to_peer(peer_id)
                for peer_id in self.peers
            ]
        )

        return dict(results)

    # ---------------------------------
    # TCP client transport
    # ---------------------------------

    async def _send_tcp(self, peer, encoded_message, timeout):
        reader = None
        writer = None

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    peer["host"],
                    peer["tcp_port"],
                    ssl=self.client_ssl_context,
                    server_hostname=(
                        peer["host"] if self.client_ssl_context else None
                    ),
                ),
                timeout=timeout,
            )

            writer.write((encoded_message + "\n").encode("utf-8"))

            await asyncio.wait_for(writer.drain(), timeout=timeout)

            data = await asyncio.wait_for(
                reader.readline(),
                timeout=timeout,
            )

            if not data:
                raise ConnectionError(
                    "Peer closed the connection without a response"
                )

            return decode_message(data.decode("utf-8"))

        finally:
            if writer is not None:
                writer.close()

                try:
                    await writer.wait_closed()
                except ConnectionError:
                    pass

    # ---------------------------------
    # UDP client transport
    # ---------------------------------

    async def _send_udp(self, peer, encoded_message, timeout):
        loop = asyncio.get_running_loop()
        response_future = loop.create_future()

        encoded_bytes = encoded_message.encode("utf-8")

        addresses = await loop.getaddrinfo(
            peer["host"],
            peer["udp_port"],
            family=socket.AF_INET,
            type=socket.SOCK_DGRAM,
        )

        expected_address = addresses[0][4][:2]

        transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPResponseProtocol(
                response_future,
                encoded_bytes,
                expected_address,
            ),
            local_addr=(self.host, 0),
        )

        try:
            return await asyncio.wait_for(
                response_future,
                timeout=timeout,
            )

        finally:
            transport.close()
