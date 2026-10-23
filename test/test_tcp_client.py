
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from networking import tcp_client


class TestTCPClient(unittest.IsolatedAsyncioTestCase):

    @patch(
        "networking.tcp_client.asyncio.open_connection",
        new_callable=AsyncMock,
        side_effect=asyncio.TimeoutError,
    )
    async def test_connection_timeout(self, mock_connect):
        with patch("builtins.print") as mock_print:
            await tcp_client.main()

        mock_print.assert_any_call(
            "Timeout: TCP server did not respond in time."
        )

    @patch(
        "networking.tcp_client.asyncio.open_connection",
        new_callable=AsyncMock,
        side_effect=ConnectionRefusedError,
    )
    async def test_connection_refused(self, mock_connect):
        with patch("builtins.print") as mock_print:
            await tcp_client.main()

        mock_print.assert_any_call(
            "Connection refused: start the TCP server first."
        )


if __name__ == "__main__":
    unittest.main()
