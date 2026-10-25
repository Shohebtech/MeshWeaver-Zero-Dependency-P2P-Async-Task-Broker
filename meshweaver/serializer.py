"""Task serialization helpers for MeshWeaver."""

import cloudpickle


def serialize_task(func, args=(), kwargs=None):
    """Serialize a Python function and its arguments."""
    if not callable(func):
        raise TypeError("Task function must be callable")

    if not isinstance(args, (tuple, list)):
        raise TypeError("Task args must be a tuple or list")

    if kwargs is None:
        kwargs = {}

    if not isinstance(kwargs, dict):
        raise TypeError("Task kwargs must be a dictionary")

    payload = {
        "function": func,
        "args": tuple(args),
        "kwargs": dict(kwargs),
    }

    return cloudpickle.dumps(payload)


def deserialize_task(data):
    """Restore a serialized task from bytes."""
    if not isinstance(data, bytes):
        raise TypeError("Serialized task must be bytes")

    payload = cloudpickle.loads(data)

    if not isinstance(payload, dict):
        raise ValueError("Invalid task payload")

    func = payload.get("function")
    args = payload.get("args")
    kwargs = payload.get("kwargs")

    if not callable(func):
        raise ValueError("Task function must be callable")

    if not isinstance(args, tuple):
        raise ValueError("Task args must be a tuple")

    if not isinstance(kwargs, dict):
        raise ValueError("Task kwargs must be a dictionary")

    return payload
