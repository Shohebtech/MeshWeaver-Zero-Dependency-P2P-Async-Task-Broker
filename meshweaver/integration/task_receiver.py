import io
import pickle
import time
from collections import OrderedDict
from typing import Any, Callable, Optional

from meshweaver.integration.signing import DEFAULT_MAX_AGE, sign, verify
from meshweaver.integration.task_manager import decode_request, encode_reply
from meshweaver.integration.task_runner import TaskRunner

REMEMBER_IDS = 10000                 # how many recent task IDs to remember
MAX_BODY_BYTES = 900_000             # keeps a reply inside the channel's 1 MB limit


def run_executor(execute: Callable, task_bytes: bytes) -> bytes:
    """Runs inside the child process. Returns b'O'+pickled result, or b'E'+error text."""
    outcome = execute(task_bytes)
    if outcome.get("status") == "completed":
        return b"O" + pickle.dumps(outcome.get("result"))
    return b"E" + str(outcome.get("error")).encode("utf-8", "replace")[:2000]


class _NoGlobals(pickle.Unpickler):
    """Reads plain data only. A pickle that tries to load any class or function is refused."""

    def find_class(self, module: str, name: str) -> Any:
        raise pickle.UnpicklingError("globals are not allowed")


def _unwrap(payload: bytes) -> bytes:
    value = _NoGlobals(io.BytesIO(payload)).load()
    if not isinstance(value, bytes) or not value:
        raise ValueError("unexpected result")
    return value


class TaskReceiver:
    """
    Use receiver.handle as the handler of a TaskServer.
    `execute` takes the task bytes and returns {"status", "result", "error"}
    (Module 4's execute_serialized_task has exactly this shape). It must be a
    function the child process can import by name.
    """

    def __init__(self, secret: bytes, execute: Callable, runner: Optional[TaskRunner] = None,
                 max_age: float = DEFAULT_MAX_AGE, remember: int = REMEMBER_IDS,
                 clock: Callable[[], float] = time.monotonic):
        self.secret, self.execute = secret, execute
        self.runner = runner or TaskRunner(max_result_bytes=MAX_BODY_BYTES)
        self.max_age, self.remember, self._clock = max_age, remember, clock
        self._seen: "OrderedDict[str, float]" = OrderedDict()

    def _is_replay(self, task_id: str) -> bool:
        now = self._clock()
        while self._seen and next(iter(self._seen.values())) < now - 2 * self.max_age:
            self._seen.popitem(last=False)          # too old to be replayed anyway
        if task_id in self._seen:
            return True
        if len(self._seen) >= self.remember:
            self._seen.popitem(last=False)
        self._seen[task_id] = now
        return False

    def _reply(self, task_id: str, ok: bool, body: bytes) -> bytes:
        return sign(encode_reply(task_id, ok, body), self.secret)

    async def handle(self, data: bytes, peer: Any = None) -> bytes:
        # A bad signature or request raises: the channel then hangs up without a reply.
        task_id, task_bytes = decode_request(verify(data, self.secret, self.max_age))
        if self._is_replay(task_id):
            return self._reply(task_id, False, b"duplicate task id")
        result = await self.runner.run(run_executor, self.execute, task_bytes)
        if result.status != "ok":
            return self._reply(task_id, False, f"{result.status}: {result.error}".encode()[:2000])
        try:
            raw = _unwrap(result.payload or b"")
        except Exception:
            return self._reply(task_id, False, b"result could not be read")
        return self._reply(task_id, raw[:1] == b"O", raw[1:])