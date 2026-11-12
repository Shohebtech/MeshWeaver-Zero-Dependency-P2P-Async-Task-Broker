
"""Tests for socket-based remote task execution."""

import socket
import threading

from meshweaver.remote import handle_client, submit_task


def add_numbers(a, b):
    return a + b


def test_remote_task_execution():
    """Test sending a task through a TCP socket."""

    # Create a temporary TCP server.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1)

    port = server.getsockname()[1]

    def worker():
        connection, _ = server.accept()

        with connection:
            handle_client(connection)

        server.close()

    worker_thread = threading.Thread(target=worker)
    worker_thread.start()

    # Submit task to the worker.
    response = submit_task(
        "127.0.0.1",
        port,
        add_numbers,
        args=(10, 20)
    )

    worker_thread.join(timeout=5)

    assert not worker_thread.is_alive()
    assert response["status"] == "completed"
    assert response["result"] == 30
    assert response["error"] is None

import pytest

from meshweaver.remote import (
    send_message,
    receive_message,
    receive_exact,
)


def test_send_and_receive_message():
    """Verify that a message is transferred correctly."""

    sender, receiver = socket.socketpair()

    try:
        message = b"Hello from MeshWeaver"

        send_message(sender, message)
        received = receive_message(receiver)

        assert received == message

    finally:
        sender.close()
        receiver.close()


def test_receive_message_with_empty_data():
    """Verify that an empty message can be transferred."""

    sender, receiver = socket.socketpair()

    try:
        send_message(sender, b"")
        received = receive_message(receiver)

        assert received == b""

    finally:
        sender.close()
        receiver.close()


def test_receive_exact_connection_closed():
    """Verify that an unexpected connection close is detected."""

    sender, receiver = socket.socketpair()

    try:
        sender.close()

        with pytest.raises(ConnectionError):
            receive_exact(receiver, 10)

    finally:
        receiver.close()


def test_send_message_rejects_oversized_data(monkeypatch):
    """Verify that messages exceeding the size limit are rejected."""

    import meshweaver.remote as remote

    sender, receiver = socket.socketpair()

    try:
        monkeypatch.setattr(remote, "MAX_MESSAGE_SIZE", 5)

        with pytest.raises(ValueError, match="too large"):
            send_message(sender, b"123456")

    finally:
        sender.close()
        receiver.close()


def test_receive_message_rejects_oversized_data(monkeypatch):
    """Verify that an oversized incoming message is rejected."""

    import meshweaver.remote as remote

    sender, receiver = socket.socketpair()

    try:
        monkeypatch.setattr(remote, "MAX_MESSAGE_SIZE", 5)

        # Send only the length header.
        header = (10).to_bytes(remote.HEADER_SIZE, "big")
        sender.sendall(header)

        with pytest.raises(ValueError, match="too large"):
            receive_message(receiver)

    finally:
        sender.close()
        receiver.close()

