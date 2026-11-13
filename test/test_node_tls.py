import ssl
import unittest

from networking.node import MeshNode
from networking.protocol import create_message


class TestMeshNodeTLS(unittest.IsolatedAsyncioTestCase):

    async def test_tls_communication_between_nodes(self):

        # Server TLS configuration
        server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ssl.load_cert_chain(
            certfile="test/fixtures/test-cert.pem",
            keyfile="test/fixtures/test-key.pem",
        )

        # Client TLS configuration
        client_ssl = ssl.create_default_context(
            cafile="test/fixtures/test-cert.pem"
        )

        server = MeshNode(
            node_id="server",
            ssl_context=server_ssl,
        )

        client = MeshNode(
            node_id="client",
            client_ssl_context=client_ssl,
        )

        async def handle_ping(message):
            return create_message(
                message_type="PONG",
                sender_id="server",
                receiver_id=message["sender_id"],
                payload={"message": "TLS connection successful"},
            )

        server.register_handler("PING", handle_ping)

        try:
            await server.start()
            await client.start()

            client.register_peer(
                peer_id="server",
                host="localhost",
                tcp_port=server.tcp_port,
                udp_port=server.udp_port,
            )

            response = await client.send_message(
                receiver_id="server",
                message_type="PING",
                protocol="tcp",
            )

            self.assertEqual(response["type"], "PONG")
            self.assertEqual(response["sender_id"], "server")

        finally:
            await client.stop()
            await server.stop()


if __name__ == "__main__":
    unittest.main()
