
import pytest

from meshweaver.serializer import serialize_task
from meshweaver.executor import execute_serialized_task


def add(a, b):
    return a + b


def divide_by_zero():
    return 10 / 0


def test_execute_serialized_task_success():
    data = serialize_task(add, (10, 20))

    response = execute_serialized_task(data)

    assert response["status"] == "completed"
    assert response["result"] == 30
    assert response["error"] is None


def test_execute_serialized_task_failure():
    data = serialize_task(divide_by_zero)

    response = execute_serialized_task(data)

    assert response["status"] == "failed"
    assert response["result"] is None
    assert "ZeroDivisionError" in response["error"]


def test_execute_invalid_task():
    response = execute_serialized_task(b"invalid data")

    assert response["status"] == "failed"
    assert response["result"] is None
    assert response["error"] is not None

def test_deserialization_error_is_reported():
    response = execute_serialized_task(b"invalid data")

    assert response["status"] == "failed"
    assert response["result"] is None
    assert response["error"].startswith("Deserialization error:")


def test_execution_error_is_reported():
    data = serialize_task(divide_by_zero)
    response = execute_serialized_task(data)

    assert response["status"] == "failed"
    assert response["result"] is None
    assert response["error"].startswith("Execution error:")
    assert "ZeroDivisionError" in response["error"]
