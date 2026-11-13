#!/usr/bin/env python3
"""Start several MeshWeaver nodes on this machine (separate OS processes).

    python scripts/launch_mesh.py --nodes 5
    python scripts/launch_mesh.py --nodes 10 --interval 5

Node i uses UDP 9000+i and dashboard http://127.0.0.1:8000+i. Every node except
the first gets node 0 as its only seed; the rest of the mesh is learned via gossip.
Press Ctrl+C to stop them all. To test failure detection, kill one node's process
and watch the others mark it offline after 3 missed intervals.
"""
import argparse
import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--base-udp", type=int, default=9000)
    ap.add_argument("--base-http", type=int, default=8000)
    ap.add_argument("--interval", type=float, default=None, help="gossip interval seconds (default 5)")
    ap.add_argument("--secret", default=None, help="shared HMAC secret for all nodes")
    a = ap.parse_args()

    procs = []
    for i in range(a.nodes):
        cmd = [sys.executable, os.path.join(ROOT, "app.py"), "--name", f"node-{i}",
               "--port", str(a.base_udp + i), "--http-port", str(a.base_http + i)]
        if i:
            cmd += ["--peers", f"127.0.0.1:{a.base_udp}"]
        if a.interval:
            cmd += ["--interval", str(a.interval)]
        if a.secret:
            cmd += ["--secret", a.secret]
        procs.append(subprocess.Popen(cmd, cwd=ROOT))
        time.sleep(0.4 if i == 0 else 0.1)
    print(f"\n{a.nodes} nodes started. Dashboards: http://127.0.0.1:{a.base_http}/ ... :{a.base_http + a.nodes - 1}/")
    print("Ctrl+C stops all nodes. PIDs:", ", ".join(f"node-{i}={p.pid}" for i, p in enumerate(procs)))

    def stop(*_):
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(3)
            except subprocess.TimeoutExpired:
                p.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    while True:
        time.sleep(1)
        dead = [i for i, p in enumerate(procs) if p.poll() is not None]
        if len(dead) == len(procs):
            break


if __name__ == "__main__":
    main()
