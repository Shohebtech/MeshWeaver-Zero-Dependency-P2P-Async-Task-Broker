"""Tests for MeshWeaver task state tracking."""

import pytest

from meshweaver.task_state import TaskState, TaskStatus


def test_initial_task_state():
    """A new task should have submitted status."""
    task = TaskState("task-001")

    assert task.task_id == "task-001"
    assert task.status == TaskStatus.SUBMITTED
    assert task.result is None
    assert task.error is None


def test_mark_task_as_running():
    """A task can be marked as running."""
    task = TaskState("task-002")

    task.mark_running()

    assert task.status == TaskStatus.RUNNING


def test_mark_task_as_completed():
    """A completed task should store its result."""
    task = TaskState("task-003")

    task.mark_completed(30)

    assert task.status == TaskStatus.COMPLETED
    assert task.result == 30
    assert task.error is None


def test_mark_task_as_failed():
    """A failed task should store its error."""
    task = TaskState("task-004")

    task.mark_failed("ZeroDivisionError")

    assert task.status == TaskStatus.FAILED
    assert task.error == "ZeroDivisionError"
    assert task.result is None


def test_task_state_to_dict():
    """Task state should be converted to a dictionary."""
    task = TaskState("task-005")

    data = task.to_dict()

    assert data == {
        "task_id": "task-005",
        "status": "submitted",
        "result": None,
        "error": None,
    }


def test_task_can_transition_from_submitted_to_running():
    """Verify that the task status changes to running."""
    task = TaskState("task-006")

    assert task.status == TaskStatus.SUBMITTED

    task.mark_running()

    assert task.status == TaskStatus.RUNNING


def test_completed_task_stores_result():
    """Verify that a completed task stores its result."""
    task = TaskState("task-007")

    task.mark_completed({"answer": 42})

    assert task.status == TaskStatus.COMPLETED
    assert task.result == {"answer": 42}
    assert task.error is None


def test_failed_task_stores_error_message():
    """Verify that a failed task stores its error message."""
    task = TaskState("task-008")

    task.mark_failed("Task execution failed")

    assert task.status == TaskStatus.FAILED
    assert task.error == "Task execution failed"
    assert task.result is None


def test_task_state_dictionary_updates_after_failure():
    """Verify that the dictionary reflects the failed task state."""
    task = TaskState("task-009")

    task.mark_failed("Connection error")

    data = task.to_dict()

    assert data["task_id"] == "task-009"
    assert data["status"] == "failed"
    assert data["error"] == "Connection error"
    assert data["result"] is None


def test_task_state_accepts_valid_task_id():
    """A valid string task ID should be accepted."""
    task = TaskState("task-101")

    assert task.task_id == "task-101"
    assert task.status == TaskStatus.SUBMITTED


def test_task_state_rejects_non_string_task_id():
    """A non-string task ID should raise TypeError."""
    with pytest.raises(TypeError, match="Task ID must be a string"):
        TaskState(123)


def test_task_state_rejects_empty_task_id():
    """An empty task ID should raise ValueError."""
    with pytest.raises(ValueError, match="Task ID cannot be empty"):
        TaskState("")


def test_task_state_rejects_whitespace_task_id():
    """A whitespace-only task ID should raise ValueError."""
    with pytest.raises(ValueError, match="Task ID cannot be empty"):
        TaskState("   ")