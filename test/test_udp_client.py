
import asyncio
import unittest
from unittest.mock import patch

from networking.udp_client import main


class TestUDPClient(unittest.IsolatedAsyncioTestCase):

    async def test_ping_receives_pong(self):
        with (
            patch("networking.udp_client.asyncio.wait_for",
                  return_value="PONG"),
            patch("networking.udp_client.asyncio.get_running_loop") as mock_loop,
            patch("networking.udp_client.asyncio.wait_for",
                  return_value="PONG"),
            patch("builtins.print") as mock_print,
        ):
            loop = asyncio.get_running_loop()
            mock_loop.return_value = loop

            transport = unittest.mock.MagicMock()
            loop.create_datagram_endpoint = unittest.mock.AsyncMock(
                return_value=(transport, None)
            )

            await main()

            mock_print.assert_any_call(
                "Success: UDP ping/pong completed."
            )
            transport.close.assert_called_once()

    async def test_unexpected_response(self):
        with patch(
            "networking.udp_client.asyncio.wait_for",
            return_value="UNKNOWN_MESSAGE",
        ), patch("networking.udp_client.asyncio.get_running_loop") as mock_loop, \
             patch("builtins.print") as mock_print:

            loop = asyncio.get_running_loop()
            mock_loop.return_value = loop
            transport = unittest.mock.MagicMock()
            loop.create_datagram_endpoint = unittest.mock.AsyncMock(
                return_value=(transport, None)
            )

            await main()

            mock_print.assert_any_call(
                "Unexpected response: UNKNOWN_MESSAGE"
            )
            transport.close.assert_called_once()
    
    
    async def test_response_timeout(self):
        with patch(
            "networking.udp_client.asyncio.wait_for",
            side_effect=asyncio.TimeoutError,
        ), patch(
            "networking.udp_client.asyncio.get_running_loop"
        ) as mock_loop, patch(
            "builtins.print"
        ) as mock_print:

            loop = asyncio.get_running_loop()
            mock_loop.return_value = loop

            transport = unittest.mock.MagicMock()
            loop.create_datagram_endpoint = unittest.mock.AsyncMock(
                return_value=(transport, None)
            )

            await main()

            mock_print.assert_any_call(
                "Timeout: UDP server did not respond."
            )
            transport.close.assert_called_once()



if __name__ == "__main__":
    unittest.main()
