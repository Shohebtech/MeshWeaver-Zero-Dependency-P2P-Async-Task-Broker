
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from networking.udp_client import main


class TestUDPClient(unittest.IsolatedAsyncioTestCase):

    async def run_client_with_mocked_response(self, response):
        loop = asyncio.get_running_loop()
        transport = MagicMock()
        response_future = loop.create_future()
        response_future.set_result(response)

        with (
            patch(
                "networking.udp_client.asyncio.get_running_loop",
                return_value=loop,
            ),
            patch(
                "networking.udp_client.asyncio.wait_for",
                new=AsyncMock(return_value=response),
            ),
            patch("builtins.print") as mock_print,
        ):
            loop.create_datagram_endpoint = AsyncMock(
                return_value=(transport, None)
            )

            await main()

        transport.close.assert_called_once()
        return mock_print

    async def test_ping_receives_pong(self):
        mock_print = await self.run_client_with_mocked_response("PONG")

        mock_print.assert_any_call(
            "Success: UDP ping/pong completed."
        )

    async def test_unexpected_response(self):
        mock_print = await self.run_client_with_mocked_response(
            "UNKNOWN_MESSAGE"
        )

        mock_print.assert_any_call(
            "Unexpected response: UNKNOWN_MESSAGE"
        )

    async def test_response_timeout(self):
        loop = asyncio.get_running_loop()
        transport = MagicMock()

        with (
            patch(
                "networking.udp_client.asyncio.get_running_loop",
                return_value=loop,
            ),
            patch(
                "networking.udp_client.RESPONSE_TIMEOUT",
                0.01,
            ),
            patch("builtins.print") as mock_print,
        ):
            loop.create_datagram_endpoint = AsyncMock(
                return_value=(transport, None)
            )

            await main()

        mock_print.assert_any_call(
            "Timeout: UDP server did not respond."
        )
        transport.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
