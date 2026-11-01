import asyncio
import multiprocessing
import pickle
from dataclasses import dataclass
from typing import Any, Callable, Optional

DEFAULT_TIMEOUT = 30.0
MAX_RESULT_BYTES = 1024 * 1024        # 1 MB


@dataclass
class IsolatedResult:
    status: str                       # "ok", "error" or "timeout"
    payload: Optional[bytes] = None   # pickled return value; set only when status == "ok"
    error: Optional[str] = None


def _child(conn: Any, func: Callable, args: tuple) -> None:
    """Runs inside the separate process: call the function, send back bytes."""
    try:
        message = b"O" + pickle.dumps(func(*args))
    except BaseException as exc:
        message = b"E" + f"{type(exc).__name__}: {exc}".encode("utf-8", "replace")[:2000]
    try:
        conn.send_bytes(message)
    finally:
        conn.close()


async def _stop(proc: Any) -> None:
    """Kill the process if it is still running and wait until it is gone."""
    if proc.is_alive():
        proc.kill()
    for _ in range(250):              # wait up to 5 seconds
        if not proc.is_alive():
            break
        await asyncio.sleep(0.02)
    try:
        proc.close()
    except ValueError:
        pass                          # still running: nothing more we can do


class TaskRunner:
    """
    Runs func(*args) in a separate process and kills it if it takes too long.
    The result comes back as pickled BYTES that this side never unpickles, so a
    hostile task cannot use the reply to attack the node. Whoever needs the value
    calls pickle.loads on result.payload.
    """

    def __init__(self, timeout: float = DEFAULT_TIMEOUT, max_concurrent: int = 2,
                 max_result_bytes: int = MAX_RESULT_BYTES):
        if timeout <= 0 or max_concurrent < 1 or max_result_bytes < 1:
            raise ValueError("timeout, max_concurrent and max_result_bytes must be positive")
        self.timeout = timeout
        self.max_result_bytes = max_result_bytes
        self._slots = asyncio.Semaphore(max_concurrent)   # at most this many tasks at once

    async def run(self, func: Callable, *args: Any,
                  timeout: Optional[float] = None) -> IsolatedResult:
        if not callable(func):
            raise TypeError("func must be callable")
        wait = self.timeout if timeout is None else timeout
        if wait <= 0:
            raise ValueError("timeout must be positive")
        async with self._slots:
            return await self._run_in_process(func, args, wait)

    async def _run_in_process(self, func: Callable, args: tuple, wait: float) -> IsolatedResult:
        loop = asyncio.get_running_loop()
        ctx = multiprocessing.get_context("spawn")        # a clean process on every OS
        receiver, sender = ctx.Pipe(duplex=False)
        proc = ctx.Process(target=_child, args=(sender, func, args), daemon=True)
        proc.start()
        sender.close()                                    # only the child writes now
        deadline = loop.time() + wait                     # the start-up time counts too
        try:
            while not receiver.poll():
                if not proc.is_alive() and not receiver.poll():
                    return IsolatedResult("error", error=f"process exited ({proc.exitcode})")
                if loop.time() >= deadline:
                    return IsolatedResult("timeout", error=f"no result within {wait}s")
                await asyncio.sleep(0.02)
            try:
                raw = receiver.recv_bytes(self.max_result_bytes + 1)
            except (EOFError, OSError):
                return IsolatedResult("error", error="result missing or too large")
            if raw[:1] == b"O":
                return IsolatedResult("ok", payload=raw[1:])
            return IsolatedResult("error", error=raw[1:].decode("utf-8", "replace"))
        finally:
            receiver.close()
            await _stop(proc)