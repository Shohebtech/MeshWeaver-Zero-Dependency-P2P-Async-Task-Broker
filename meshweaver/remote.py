
"""Socket-based remote task execution for MeshWeaver."""

import socket
import cloudpickle

from meshweaver.serializer import serialize_task
from meshweaver.executor import execute_serialized_task


HEADER_SIZE = 8
MAX_MESSAGE_SIZE = 64 * 1024 * 1024


def send_message(connection, data):
    """Send bytes with an 8-byte length header."""
    if not isinstance(data, bytes):
        raise TypeError("Message must be bytes")

    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError("Message is too large")

    header = len(data).to_bytes(HEADER_SIZE, "big")
    connection.sendall(header + data)


def receive_exact(connection, size):
    """Receive exactly the requested number of bytes."""
    chunks = []
    remaining = size

    while remaining > 0:
        chunk = connection.recv(remaining)

        if not chunk:
            raise ConnectionError("Connection closed unexpectedly")

        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


def receive_message(connection):
    """Receive a length-prefixed message."""
    header = receive_exact(connection, HEADER_SIZE)
    message_size = int.from_bytes(header, "big")

    if message_size > MAX_MESSAGE_SIZE:
        raise ValueError("Received message is too large")

    return receive_exact(connection, message_size)


def handle_client(connection):
    """Receive, execute, and return one task."""
    task_data = receive_message(connection)

    # Execute the received serialized task.
    response = execute_serialized_task(task_data)

    # Serialize the execution response.
    response_data = cloudpickle.dumps(response)

    send_message(connection, response_data)


def serve_once(host="127.0.0.1", port=5000):
    """Start a worker that handles one remote task."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(1)

        print(f"Worker listening on {host}:{port}")

        connection, address = server.accept()

        with connection:
            print("Connected by:", address)
            handle_client(connection)

        print("Task execution completed.")


def submit_task(host, port, func, args=(), kwargs=None):
    """Send a task to a worker and receive its result."""
    task_data = serialize_task(func, args, kwargs)

    with socket.create_connection((host, port), timeout=10) as connection:
        send_message(connection, task_data)
        response_data = receive_message(connection)

    return cloudpickle.loads(response_data)
