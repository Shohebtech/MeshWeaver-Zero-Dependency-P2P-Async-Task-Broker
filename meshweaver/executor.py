
"""Task execution helpers for MeshWeaver."""

from meshweaver.serializer import deserialize_task


def execute_serialized_task(data):
    """Execute a serialized task and return its result or error."""
    try:
        payload = deserialize_task(data)

        func = payload["function"]
        args = payload["args"]
        kwargs = payload["kwargs"]

        result = func(*args, **kwargs)

        return {
            "status": "completed",
            "result": result,
            "error": None,
        }

    except Exception as exc:
        return {
            "status": "failed",
            "result": None,
            "error": f"{type(exc).__name__}: {exc}",
        }
