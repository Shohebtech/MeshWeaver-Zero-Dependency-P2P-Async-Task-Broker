import asyncio
import pickle
import threading
import time

import pytest

from meshweaver.integration.task_runner import TaskRunner

# The functions below are standard-library ones, because a separate process can only
# run functions it can import by name.


def run(runner, *call, **options):
    return asyncio.run(runner.run(*call, **options))


def test_result_comes_back_as_pickled_bytes():
    async def scenario():
        return await TaskRunner(timeout=30).run(bytes.upper, b"abc")

    result = asyncio.run(scenario())
    assert result.status == "ok"
    assert pickle.loads(result.payload) == b"ABC" #type: ignore


def test_error_in_the_function_is_reported_not_raised():
    async def scenario():
        return await TaskRunner(timeout=30).run(int, "not a number")

    result = asyncio.run(scenario())
    assert result.status == "error"
    assert "ValueError" in result.error #type: ignore


def test_runaway_task_is_killed_on_timeout():
    async def scenario():
        started = time.monotonic()
        result = await TaskRunner(timeout=30).run(time.sleep, 60, timeout=1.5)
        return result, time.monotonic() - started

    result, elapsed = asyncio.run(scenario())
    assert result.status == "timeout"
    assert elapsed < 15                           # we did not wait for the 60 s sleep


def test_oversized_result_is_refused():
    async def scenario():
        return await TaskRunner(timeout=30, max_result_bytes=100).run(bytes.upper, b"a" * 1000)

    result = asyncio.run(scenario())
    assert result.status == "error"
    assert result.payload is None


def test_unpicklable_result_is_an_error():
    async def scenario():
        return await TaskRunner(timeout=30).run(threading.Lock)

    assert asyncio.run(scenario()).status == "error"


def test_bad_arguments_are_rejected():
    with pytest.raises(ValueError):
        TaskRunner(timeout=0)
    with pytest.raises(TypeError):
        asyncio.run(TaskRunner().run("not callable")) #type: ignore