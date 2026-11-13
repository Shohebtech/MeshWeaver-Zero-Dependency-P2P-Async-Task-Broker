"""End-to-end tests for MeshWeaver task serialization and execution."""

from meshweaver.serializer import serialize_task
from meshweaver.executor import execute_serialized_task


def add_numbers(a, b):
    return a + b


def create_greeting(name, greeting="Hello"):
    return f"{greeting}, {name}!"


def test_end_to_end_addition():
    """Serialize, transfer as bytes, and execute a task."""
    payload = serialize_task(add_numbers, (10, 20))

    assert isinstance(payload, bytes)

    response = execute_serialized_task(payload)

    assert response["status"] == "completed"
    assert response["result"] == 30
    assert response["error"] is None


def test_end_to_end_function_with_keyword_arguments():
    """Verify execution with keyword arguments."""
    payload = serialize_task(
        create_greeting,
        ("Harini",),
        {"greeting": "Welcome"},
    )

    response = execute_serialized_task(payload)

    assert response["status"] == "completed"
    assert response["result"] == "Welcome, Harini!"
    assert response["error"] is None


def test_end_to_end_complex_return_value():
    """Verify that a task can return a dictionary."""
    def build_result(number):
        return {
            "original": number,
            "squared": number ** 2,
            "is_even": number % 2 == 0,
        }

    payload = serialize_task(build_result, (6,))
    response = execute_serialized_task(payload)

    assert response["status"] == "completed"
    assert response["result"] == {
        "original": 6,
        "squared": 36,
        "is_even": True,
    }


def test_end_to_end_execution_failure():
    """Verify that task execution errors are returned safely."""
    def failing_task():
        raise ValueError("Intentional test error")

    payload = serialize_task(failing_task)
    response = execute_serialized_task(payload)

    assert response["status"] == "failed"
    assert response["result"] is None
    assert "ValueError" in response["error"]