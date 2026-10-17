
from meshweaver.serializer import serialize_task


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
