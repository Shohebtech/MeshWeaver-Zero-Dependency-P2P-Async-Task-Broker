
from meshweaver.serializer import (
    serialize_task,
    deserialize_task,
)


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


def test_serialize_task_returns_bytes():
    data = serialize_task(add, (10, 20))

    assert isinstance(data, bytes)


def test_serialize_task_with_arguments():
    data = serialize_task(add, (10, 20))

    assert len(data) > 0


def test_serialize_task_with_keyword_arguments():
    data = serialize_task(
        multiply,
        args=(),
        kwargs={"a": 5, "b": 4},
    )

    assert isinstance(data, bytes)


def test_deserialize_task_restores_function():
    data = serialize_task(add, (10, 20))
    payload = deserialize_task(data)

    assert callable(payload["function"])
    assert payload["args"] == (10, 20)
    assert payload["kwargs"] == {}


def test_deserialize_task_restores_keyword_arguments():
    data = serialize_task(
        add,
        args=(),
        kwargs={"a": 10, "b": 20},
    )
    payload = deserialize_task(data)

    assert payload["kwargs"] == {"a": 10, "b": 20}


def test_deserialize_task_rejects_non_bytes():
    import pytest

    with pytest.raises(TypeError):
        deserialize_task("invalid data")


