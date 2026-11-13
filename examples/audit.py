import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent   # the repo folder


def read_status(path):
    """Read one node's status file. None if it is missing or half-written."""
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def check_mesh(statuses):
    """
    Every node must know every other node, and nobody unknown.
    Returns a list of problems; an empty list means the mesh is complete.
    """
    problems = []
    ids = {s["node_id"] for s in statuses}
    for s in statuses:
        peers = set(s["peers"])
        missing = ids - {s["node_id"]} - peers
        unknown = peers - ids
        if missing:
            problems.append(f"node {s['name']} (port {s['port']}) is missing "
                            f"{len(missing)} peer(s)")
        if unknown:
            problems.append(f"node {s['name']} (port {s['port']}) knows "
                            f"{len(unknown)} node(s) outside this audit")
    return problems


def wait_for_first_status(path, proc, timeout):
    """Wait until a freshly started node reports in. None if it died or is too slow."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            return None
        status = read_status(path)
        if status:
            return status
        time.sleep(0.1)
    return None


def stop_all(procs, directory):
    """Ask every node to stop (a STOP file), then force the stubborn ones."""
    Path(directory, "STOP").touch()
    for proc in procs:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()


def run_audit(args):
    directory = tempfile.mkdtemp(prefix="meshweaver-audit-")
    procs, paths = [], []
    started = time.monotonic()
    try:
        print(f"Starting {args.nodes} node processes "
              f"(no addresses are passed to any of them)...")
        for i in range(1, args.nodes + 1):
            path = os.path.join(directory, f"node-{i}.json")
            command = [sys.executable, "-m", "examples.node",
                       "--name", str(i), "--status-dir", directory,
                       "--port-base", str(args.port_base),
                       "--ports", str(args.ports), "--max-seconds", "300"]
            proc = subprocess.Popen(command, cwd=ROOT)
            procs.append(proc)
            paths.append(path)
            # One at a time: the next node starts only after this one has joined.
            status = wait_for_first_status(path, proc, args.startup_timeout)
            if status is None:
                print(f"  node {i}: failed to start")
                return 1
            how = "joined the mesh" if status["joined"] else "first node, nobody else found"
            print(f"  node {i}: pid {status['pid']}, port {status['port']}, {how}")

        print(f"\nChecking that every node knows all {args.nodes - 1} others...")
        deadline = time.monotonic() + args.timeout
        while True:
            statuses = [s for s in (read_status(p) for p in paths) if s]
            if len(statuses) == args.nodes:
                problems = check_mesh(statuses)
            else:
                problems = ["some nodes have not reported yet"]
            if not problems or time.monotonic() >= deadline:
                break
            time.sleep(0.5)

        print(f"\n{'Node':>5}  {'Port':>5}  {'Peers':>7}  Status")
        ids = {s["node_id"] for s in statuses}
        for s in sorted(statuses, key=lambda s: s["name"]):
            ok = set(s["peers"]) == ids - {s["node_id"]}
            known = f"{len(s['peers'])}/{args.nodes - 1}"
            print(f"{s['name']:>5}  {s['port']:>5}  {known:>7}  {'ok' if ok else 'INCOMPLETE'}")

        elapsed = time.monotonic() - started
        if problems:
            print("\nFAIL:")
            for problem in problems:
                print("  - " + problem)
            return 1
        print(f"\nPASS: all {args.nodes} nodes found each other with no hardcoded "
              f"addresses ({elapsed:.1f}s)")
        return 0
    finally:
        stop_all(procs, directory)
        shutil.rmtree(directory, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description="Start separate node processes and check that they form one mesh.")
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--port-base", type=int, default=9000)
    parser.add_argument("--ports", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=20,
                        help="seconds to wait for the mesh to complete")
    parser.add_argument("--startup-timeout", type=float, default=15,
                        help="seconds to wait for each node to start")
    args = parser.parse_args()
    if not 1 <= args.nodes <= args.ports:
        parser.error("--nodes must be between 1 and --ports")
    sys.exit(run_audit(args))


if __name__ == "__main__":
    main()