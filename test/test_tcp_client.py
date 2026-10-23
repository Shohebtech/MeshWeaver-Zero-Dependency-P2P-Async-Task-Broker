
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

    
    
    @patch("networking.tcp_client.asyncio.open_connection")
    async def test_response_timeout(self, mock_connect):
        reader = asyncio.StreamReader()
        reader.readline = AsyncMock(
            side_effect=asyncio.TimeoutError
        )

        writer = unittest.mock.MagicMock()
        writer.drain = AsyncMock()
        writer.wait_closed = AsyncMock()

        mock_connect.return_value = (reader, writer)

        with patch("builtins.print") as mock_print:
            await tcp_client.main()

        mock_print.assert_any_call(
            "Timeout: TCP server did not respond in time."
        )
        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()




if __name__ == "__main__":
    unittest.main()
