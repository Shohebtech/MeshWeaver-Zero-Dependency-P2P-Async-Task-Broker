"""Task execution helpers for MeshWeaver."""

import time

from meshweaver.serializer import deserialize_task


def execute_serialized_task(data):
    """Deserialize and execute a task, recording its execution duration."""
    start_time = time.perf_counter()

    try:
        payload = deserialize_task(data)
    except Exception as exc:
        execution_time = time.perf_counter() - start_time

        return {
            "status": "failed",
            "result": None,
            "error": f"Deserialization error: {type(exc).__name__}: {exc}",
            "execution_time": execution_time,
        }

    try:
        func = payload["function"]
        args = payload["args"]
        kwargs = payload["kwargs"]

        result = func(*args, **kwargs)
        execution_time = time.perf_counter() - start_time

        return {
            "status": "completed",
            "result": result,
            "error": None,
            "execution_time": execution_time,
        }

    except Exception as exc:
        execution_time = time.perf_counter() - start_time

        return {
            "status": "failed",
            "result": None,
            "error": f"Execution error: {type(exc).__name__}: {exc}",
            "execution_time": execution_time,
        }