"""Task serialization helpers for MeshWeaver."""

import cloudpickle


def serialize_task(func, args=(), kwargs=None):
    """Serialize a Python function and its arguments."""
    if kwargs is None:
        kwargs = {}

    payload = {
        "function": func,
        "args": tuple(args),
        "kwargs": dict(kwargs),
    }

    return cloudpickle.dumps(payload)