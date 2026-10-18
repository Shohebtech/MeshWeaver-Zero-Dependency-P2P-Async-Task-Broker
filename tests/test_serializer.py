
import pytest

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
    with pytest.raises(TypeError):
        deserialize_task("invalid data")


# Commit 4: Additional test coverage

def test_round_trip_preserves_function_result():
    data = serialize_task(add, (15, 25))
    payload = deserialize_task(data)

    result = payload["function"](*payload["args"])

    assert result == 40


def test_round_trip_with_keyword_arguments():
    data = serialize_task(
        multiply,
        args=(),
        kwargs={"a": 6, "b": 7},
    )
    payload = deserialize_task(data)

    result = payload["function"](
        *payload["args"],
        **payload["kwargs"],
    )

    assert result == 42


def test_deserialize_rejects_invalid_payload():
    import cloudpickle

    invalid_data = cloudpickle.dumps(["invalid", "task"])

    with pytest.raises(ValueError):
        deserialize_task(invalid_data)


def test_round_trip_preserves_empty_arguments():
    def get_message():
        return "MeshWeaver ready"

    data = serialize_task(get_message)
    payload = deserialize_task(data)

    assert payload["args"] == ()
    assert payload["kwargs"] == {}
    assert payload["function"]() == "MeshWeaver ready"
