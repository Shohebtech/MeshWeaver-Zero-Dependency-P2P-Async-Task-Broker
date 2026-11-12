
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
