"""REST endpoints that feed dashboard pages 1, 2, 4 and 5.

Data from Module 3 is real. Anything coming from a mock adapter carries
``"mock": true`` so the UI can label it.
"""
from __future__ import annotations

from flask import Blueprint, jsonify, request

NETWORK_KINDS = ["heartbeat_sent", "heartbeat_recv", "gossip_sent", "gossip_recv",
                 "peer_discovered", "status_change", "auth_failed"]


def create_blueprint(engine, executor) -> Blueprint:
    bp = Blueprint("dashboard_api", __name__, url_prefix="/api")

    @bp.get("/overview")
    def overview():
        s = engine.table.summary()
        tasks = executor.summary()
        return jsonify({"server_time": engine._clock(), "gossip": s, "tasks": tasks,
                        "node": engine.health()["node"],
                        "recent_events": engine.events(8, ["status_change", "peer_discovered"])})

    @bp.get("/mesh")
    def mesh():
        rows = engine.get_table()
        for r in rows:                      # an offline node's old claims are not live connections
            if r["status"] == "offline":
                r["links"] = []
        ids = {r["node_id"] for r in rows if r["status"] == "online"}
        edges = sorted({tuple(sorted((r["node_id"], l))) for r in rows for l in r["links"] if l in ids})
        h = engine.health()
        return jsonify({"server_time": engine._clock(), "nodes": rows,
                        "edges": [list(e) for e in edges], "self_id": engine.identity.node_id,
                        "discovery": h["directory"], "transport": h["transport"]})

    @bp.get("/tasks")
    def tasks():
        return jsonify({"server_time": engine._clock(), "mock": True,
                        "implementation": executor.implementation,
                        "summary": executor.summary(), "tasks": executor.list_tasks()})

    @bp.post("/tasks/demo")
    def submit_demo():
        body = request.get_json(silent=True) or {}
        name = body.get("name") if isinstance(body.get("name"), str) else None
        return jsonify(executor.submit(name[:80] if name else None)), 201

    @bp.get("/network")
    def network():
        h = engine.health()
        rows = engine.get_table()
        events = engine.events(150, NETWORK_KINDS)
        failures = [e for e in engine.events(200, ["status_change", "auth_failed"])
                    if e["kind"] == "auth_failed" or "OFFLINE" in e["detail"] or "STALE" in e["detail"]]
        return jsonify({"server_time": engine._clock(), "health": h, "nodes": rows,
                        "events": events, "failures": failures[:50]})

    return bp
