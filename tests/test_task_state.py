from meshweaver.task_state import TaskState, TaskStatus


def test_initial_task_status():
    task = TaskState("task-001")

    assert task.status == TaskStatus.SUBMITTED
    assert task.result is None
    assert task.error is None


def test_mark_task_running():
    task = TaskState("task-002")

    task.mark_running()

    assert task.status == TaskStatus.RUNNING


def test_mark_task_completed():
    task = TaskState("task-003")

    task.mark_running()
    task.mark_completed(30)

    assert task.status == TaskStatus.COMPLETED
    assert task.result == 30
    assert task.error is None


def test_mark_task_failed():
    task = TaskState("task-004")

    task.mark_running()
    task.mark_failed("ZeroDivisionError")

    assert task.status == TaskStatus.FAILED
    assert task.result is None
    assert task.error == "ZeroDivisionError"


def test_task_state_to_dict():
    task = TaskState("task-005")
    task.mark_completed(100)

    data = task.to_dict()

    assert data["task_id"] == "task-005"
    assert data["status"] == "completed"
    assert data["result"] == 100
    assert data["error"] is None