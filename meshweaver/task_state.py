"""Task state tracking for MeshWeaver."""

from enum import Enum


class TaskStatus(str, Enum):
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskState:
    """Track the status and result of a task."""

    def __init__(self, task_id):
        self.task_id = task_id
        self.status = TaskStatus.SUBMITTED
        self.result = None
        self.error = None

    def mark_running(self):
        self.status = TaskStatus.RUNNING

    def mark_completed(self, result):
        self.status = TaskStatus.COMPLETED
        self.result = result
        self.error = None

    def mark_failed(self, error):
        self.status = TaskStatus.FAILED
        self.error = str(error)
        self.result = None

    def to_dict(self):
        """Return task information as a dictionary."""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
        }