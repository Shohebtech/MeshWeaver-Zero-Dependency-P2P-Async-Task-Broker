import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from meshweaver.integration.signing import SigningError, sign, verify
from meshweaver.integration.task_channel import TaskChannelError, send_task, task_port

ID_CHARS = 32                                   # a task ID is 32 lowercase hex characters
_HEX = set("0123456789abcdef")
NODE_ERRORS = (TaskChannelError, SigningError, ValueError)   # the node failed, not the task


# ---- wire format (the receiving side uses the same helpers) ----
def _check_id(task_id: str) -> str:
    if len(task_id) != ID_CHARS or any(c not in _HEX for c in task_id):
        raise ValueError("bad task id")
    return task_id


def encode_request(task_id: str, task_bytes: bytes) -> bytes:
    return _check_id(task_id).encode("ascii") + task_bytes


def decode_request(data: bytes) -> Tuple[str, bytes]:
    if len(data) <= ID_CHARS:
        raise ValueError("request too short")
    return _check_id(data[:ID_CHARS].decode("ascii", "replace")), data[ID_CHARS:]


def encode_reply(task_id: str, ok: bool, body: bytes) -> bytes:
    return _check_id(task_id).encode("ascii") + (b"O" if ok else b"E") + body


def decode_reply(data: bytes) -> Tuple[str, bool, bytes]:
    if len(data) <= ID_CHARS:
        raise ValueError("reply too short")
    flag = data[ID_CHARS:ID_CHARS + 1]
    if flag not in (b"O", b"E"):
        raise ValueError("bad reply flag")
    return _check_id(data[:ID_CHARS].decode("ascii", "replace")), flag == b"O", data[ID_CHARS + 1:]


@dataclass
class TaskRecord:
    task_id: str
    status: str = "submitted"                   # submitted, running, completed or failed
    node_id: Optional[str] = None               # the node of the latest attempt
    attempts: int = 0
    tried: List[str] = field(default_factory=list)
    result: Optional[bytes] = None              # pickled return value, as sent by the node
    error: Optional[str] = None


class TaskManager:
    """
    Sends tasks to the least busy node and re-sends them if that node fails.
    `engine` is a gossip engine (anything with lowest_cpu_node and on_peer_lost).
    `secret` is the shared signing key (bytes).
    """

    def __init__(self, engine: Any, secret: bytes, send: Callable = send_task,
                 port_for: Callable[[int], int] = task_port, max_attempts: int = 3,
                 timeout: float = 60.0, include_self: bool = True):
        if max_attempts < 1 or timeout <= 0:
            raise ValueError("max_attempts and timeout must be positive")
        self.engine, self.secret = engine, secret
        self._send, self._port_for = send, port_for
        self.max_attempts, self.timeout, self.include_self = max_attempts, timeout, include_self
        self.tasks: Dict[str, TaskRecord] = {}
        self._inflight: Dict[str, Tuple[str, "asyncio.Future[Any]"]] = {}
        self._lost: Set[str] = set()
        engine.on_peer_lost(self._peer_lost)

    async def submit(self, task_bytes: bytes) -> TaskRecord:
        """Send a serialized task and wait until it completes or fails for good."""
        if not isinstance(task_bytes, bytes) or not task_bytes:
            raise ValueError("task_bytes must be non-empty bytes")
        record = TaskRecord(uuid.uuid4().hex)
        self.tasks[record.task_id] = record
        await self._drive(record, task_bytes)
        return record

    def _peer_lost(self, node_id: str, entry: Any = None) -> None:
        """Gossip says a node went silent: stop waiting on tasks sent to it."""
        for task_id, (target, attempt) in list(self._inflight.items()):
            if target == node_id and not attempt.done():
                self._lost.add(task_id)
                attempt.cancel()

    async def _attempt(self, record: TaskRecord, entry: Any, task_bytes: bytes) -> Tuple[bool, bytes]:
        request = sign(encode_request(record.task_id, task_bytes), self.secret)
        reply = await self._send(entry.host, self._port_for(entry.port), request,
                                 timeout=self.timeout)
        task_id, ok, body = decode_reply(verify(reply, self.secret))
        if task_id != record.task_id:
            raise ValueError("reply belongs to a different task")
        return ok, body

    async def _drive(self, record: TaskRecord, task_bytes: bytes) -> None:
        while record.attempts < self.max_attempts:
            entry = self.engine.lowest_cpu_node(exclude=record.tried,
                                                include_self=self.include_self)
            if entry is None:
                break
            record.attempts += 1
            record.node_id = entry.node_id
            record.tried.append(entry.node_id)
            record.status = "running"
            attempt = asyncio.ensure_future(self._attempt(record, entry, task_bytes))
            self._inflight[record.task_id] = (entry.node_id, attempt)
            try:
                ok, body = await attempt
            except asyncio.CancelledError:
                if record.task_id not in self._lost:
                    attempt.cancel()
                    raise                        # we were cancelled ourselves
                self._lost.discard(record.task_id)
                record.error, record.status = f"node {entry.node_id} was lost", "submitted"
                continue
            except NODE_ERRORS as exc:
                record.error, record.status = f"{entry.node_id}: {exc}", "submitted"
                continue
            finally:
                self._inflight.pop(record.task_id, None)
            if ok:
                record.status, record.result, record.error = "completed", body, None
            else:                                # the task itself failed: do not retry
                record.status, record.error = "failed", body.decode("utf-8", "replace")
            return
        record.status = "failed"
        record.error = (f"gave up after {record.attempts} attempt(s): {record.error}"
                        if record.attempts else "no node available")