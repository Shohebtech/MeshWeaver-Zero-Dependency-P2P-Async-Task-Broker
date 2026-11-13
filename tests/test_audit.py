import subprocess
import sys
from pathlib import Path

from examples.audit import check_mesh #type: ignore 

ROOT = Path(__file__).resolve().parent.parent


def status(name, node_id, peers):
    return {"name": name, "port": 9000 + name, "node_id": node_id, "peers": peers}


def test_check_mesh_accepts_a_full_mesh():
    statuses = [status(1, "a", ["b", "c"]),
                status(2, "b", ["a", "c"]),
                status(3, "c", ["a", "b"])]
    assert check_mesh(statuses) == []


def test_check_mesh_reports_missing_peers():
    statuses = [status(1, "a", ["b"]),
                status(2, "b", ["a", "c"]),
                status(3, "c", ["a", "b"])]
    problems = check_mesh(statuses)
    assert len(problems) == 1
    assert "node 1" in problems[0]
    assert "missing 1" in problems[0]


def test_check_mesh_reports_unknown_peers():
    statuses = [status(1, "a", ["b", "ghost"]),
                status(2, "b", ["a"])]
    problems = check_mesh(statuses)
    assert len(problems) == 1
    assert "outside this audit" in problems[0]


def test_check_mesh_single_node_needs_no_peers():
    assert check_mesh([status(1, "a", [])]) == []


def test_ten_separate_processes_form_a_mesh():
    result = subprocess.run(
        [sys.executable, "-m", "examples.audit",
         "--nodes", "10", "--port-base", "47900", "--ports", "20"],
        cwd=ROOT, capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS" in result.stdout