"""MOCK ADAPTER for MODULE 4 (Task Serializer & Remote Execution).

*** Placeholder until the teammate's real cloudpickle executor is integrated. ***
SIMULATED ONLY: nothing is serialised, sent or executed. It exists so the
dashboard can show the whole workflow: pick the least-loaded online node from
Module 3, "run" for a few seconds, and re-route if that node goes offline.
"""
from __future__ import annotations

import itertools
import threading
import time
from typing import Any, Callable, Dict, List, Optional

DEMO_TASKS = ["matrix_multiply(512)", "monte_carlo_pi(10**6)", "fibonacci(32)",
              "train_linear_model()", "word_count(corpus)"]


class MockTaskExecutor:
    mock = True
    implementation = "MockTaskExecutor (Module 4 placeholder; simulated, no code is executed)"

    def __init__(self, load_provider, run_seconds: float = 12.0,
                 clock: Callable[[], float] = time.time):
        self.load = load_provider
        self.run_seconds = run_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._tasks: List[Dict[str, Any]] = []
        self._ids = itertools.count(1)

    def submit(self, name: Optional[str] = None, **_: Any) -> Dict[str, Any]:
        with self._lock:
            n = next(self._ids)
            now = self._clock()
            task = {"task_id": f"T{n:04d}", "name": name or DEMO_TASKS[(n - 1) % len(DEMO_TASKS)],
                    "node_id": None, "node_name": None, "status": "submitted", "result": None,
                    "error": None, "created": now, "updated": now, "started": None,
                    "reroutes": [], "mock": True}
            self._assign(task, now)
            self._tasks.append(task)
            return dict(task)

    def _assign(self, task: Dict[str, Any], now: float, avoid: Optional[str] = None) -> None:
        pick = self.load.get_least_loaded(exclude=[avoid] if avoid else [])
        if pick is None:
            task.update(status="failed", error="no online node available", updated=now)
            return
        task.update(node_id=pick["node_id"], node_name=pick["name"], status="running",
                    started=now, updated=now)

    def advance(self) -> None:
        """Move simulated tasks forward; re-route tasks whose node went offline."""
        now = self._clock()
        with self._lock:
            for t in self._tasks:
                if t["status"] != "running":
                    continue
                if not self.load.is_online(t["node_id"]):
                    t["reroutes"].append({"from": t["node_name"], "at": now, "reason": "node offline"})
                    self._assign(t, now, avoid=t["node_id"])
                    if t["status"] == "failed":
                        t["error"] = "node offline and no replacement available"
                elif now - t["started"] >= self.run_seconds:
                    t.update(status="completed", updated=now,
                             result=f"MOCK result from {t['node_name']} (not executed)")

    def list_tasks(self) -> List[Dict[str, Any]]:
        self.advance()
        with self._lock:
            return [dict(t) for t in reversed(self._tasks)]

    def summary(self) -> Dict[str, Any]:
        tasks = self.list_tasks()
        count = lambda *s: sum(1 for t in tasks if t["status"] in s)
        return {"active": count("submitted", "running"), "completed": count("completed"),
                "failed": count("failed"), "rerouted": sum(1 for t in tasks if t["reroutes"]),
                "total": len(tasks), "mock": True}
