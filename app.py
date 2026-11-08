"""MeshWeaver node entrypoint: one gossip engine + REST API + web dashboard.

    python app.py --name node-1 --port 9000 --http-port 8000
    python app.py --name node-2 --port 9001 --http-port 8001 --peers 127.0.0.1:9000

Module imports for the backends are done lazily so the Flask factory can be
imported (and the page templates rendered) before every module exists.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import random
import threading
from typing import List, Optional, Tuple

from flask import Flask, render_template

from config import DEFAULT_GOSSIP_PORT, DEFAULT_HTTP_PORT, GossipConfig

BASE = os.path.dirname(os.path.abspath(__file__))
PAGES = [("overview", "/", "Overview"), ("mesh", "/mesh", "Mesh network"),
         ("gossip", "/gossip", "Gossip monitoring"), ("tasks", "/tasks", "Task management"),
         ("network", "/network", "Network monitoring")]


def create_app(engine=None, executor=None) -> Flask:
    app = Flask(__name__, template_folder=os.path.join(BASE, "frontend", "templates"),
                static_folder=os.path.join(BASE, "frontend", "static"))
    if engine is not None:
        from modules.gossip.gossip_api import create_blueprint as gossip_bp
        app.register_blueprint(gossip_bp(engine))
        if executor is not None:
            from dashboard_api import create_blueprint as dash_bp
            app.register_blueprint(dash_bp(engine, executor))

    def page(key: str, title: str):
        node = engine.identity if engine else None
        interval = engine.cfg.interval if engine else GossipConfig().interval
        return render_template(f"{key}.html", pages=PAGES, active=key, title=title,
                               node_name=getattr(node, "name", "no engine"),
                               node_port=getattr(node, "port", ""), refresh_ms=5000,
                               gossip_interval=interval)

    for key, path, title in PAGES:
        app.add_url_rule(path, endpoint=key, view_func=(lambda k=key, t=title: page(k, t)))
    return app


class EngineThread(threading.Thread):
    """Runs the asyncio gossip engine in its own thread so Flask can use the main one."""

    def __init__(self, engine):
        super().__init__(name="gossip-engine", daemon=True)
        self.engine = engine
        self.loop = asyncio.new_event_loop()
        self.started = threading.Event()
        self.error: Optional[BaseException] = None

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.engine.start())
        except BaseException as exc:  # e.g. port already in use
            self.error = exc
            self.started.set()
            return
        self.started.set()
        self.loop.run_forever()

    def shutdown(self) -> None:
        if self.loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(self.engine.stop(), self.loop)
            try:
                fut.result(timeout=3)
            except Exception:
                pass
            self.loop.call_soon_threadsafe(self.loop.stop)


def parse_peers(text: str) -> List[Tuple[str, int]]:
    out = []
    for item in filter(None, (t.strip() for t in text.split(","))):
        host, _, port = item.rpartition(":")
        out.append((host or "127.0.0.1", int(port)))
    return out


def attach_simulation(engine, count: int) -> None:
    """Demo mode: add clearly-labelled SIMULATED nodes that wander around."""
    rng = random.Random(7)
    state = {i: [rng.uniform(10, 70), rng.uniform(20, 70)] for i in range(count)}

    def hook(_engine):
        for i, v in state.items():
            v[0] = max(1.0, min(99.0, v[0] + rng.uniform(-8, 8)))
            v[1] = max(1.0, min(99.0, v[1] + rng.uniform(-3, 3)))
            _engine.table.upsert_simulated(f"sim-{i}", f"sim-{i}", v[0], v[1])
    engine.add_tick_hook(hook)


def build_runtime(args):
    from modules.dht.mock_directory import MockPeerDirectory
    from modules.gossip.engine import GossipEngine
    from modules.interfaces import NodeIdentity
    from modules.networking.mock_transport import MockUdpTransport
    from modules.task_execution.mock_executor import MockTaskExecutor

    cfg = GossipConfig.from_env()
    if args.interval:
        cfg.interval = args.interval
    if args.secret:
        cfg.secret = args.secret
    node_id = args.node_id or hashlib.sha1(f"{args.advertise_host}:{args.port}".encode()).hexdigest()[:16]
    ident = NodeIdentity(node_id, args.name or f"node-{args.port}", args.advertise_host, args.port, args.http_port)
    directory = MockPeerDirectory(parse_peers(args.peers), (args.advertise_host, args.port))
    engine = GossipEngine(ident, MockUdpTransport(args.host, args.port), directory, cfg)
    if args.simulate:
        attach_simulation(engine, args.simulate)
    return engine, MockTaskExecutor(engine)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="MeshWeaver node (Module 3 gossip + dashboard)")
    ap.add_argument("--name", help="display name (default node-<port>)")
    ap.add_argument("--node-id", help="stable id (default: hash of advertise-host:port)")
    ap.add_argument("--host", default="0.0.0.0", help="UDP bind address")
    ap.add_argument("--advertise-host", default="127.0.0.1", help="address other nodes use to reach this node")
    ap.add_argument("--port", type=int, default=DEFAULT_GOSSIP_PORT, help="UDP gossip port")
    ap.add_argument("--http-port", type=int, default=DEFAULT_HTTP_PORT, help="dashboard/REST port")
    ap.add_argument("--http-host", default="127.0.0.1", help="dashboard bind address (use 0.0.0.0 to share)")
    ap.add_argument("--peers", default="", help="comma list of seed peers host:port")
    ap.add_argument("--interval", type=float, help="gossip interval seconds (default 5)")
    ap.add_argument("--secret", help="shared secret: sign/verify gossip with HMAC-SHA256")
    ap.add_argument("--simulate", type=int, default=0, help="add N SIMULATED demo nodes (labelled in the UI)")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    engine, executor = build_runtime(args)
    thread = EngineThread(engine)
    thread.start()
    thread.started.wait(10)
    if thread.error:
        raise SystemExit(f"could not start gossip engine on UDP {args.port}: {thread.error}")

    app = create_app(engine, executor)
    print(f"MeshWeaver {engine.identity.name}: gossip UDP {args.advertise_host}:{engine.identity.port}, "
          f"dashboard http://{args.http_host}:{args.http_port}/  (interval {engine.cfg.interval:g}s)")
    try:
        app.run(host=args.http_host, port=args.http_port, threaded=True, use_reloader=False)
    finally:
        thread.shutdown()


if __name__ == "__main__":
    main()
