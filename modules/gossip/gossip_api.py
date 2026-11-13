"""REST API for Module 3. Register with ``app.register_blueprint(create_blueprint(engine))``.

GET  /api/gossip/nodes          full load table
GET  /api/gossip/summary        counts, averages, least-loaded node
GET  /api/gossip/least-loaded   lowest-CPU online node (?exclude=id1,id2)
GET  /api/gossip/health         engine status, thresholds, transport/directory info
GET  /api/gossip/history        events (?limit=&kinds=a,b&node_id=) + CPU/RAM series
POST /api/gossip/heartbeat      report that a node is alive (JSON body, node_id required)
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from flask import Blueprint, jsonify, request


def create_blueprint(engine) -> Blueprint:
    bp = Blueprint("gossip_api", __name__, url_prefix="/api/gossip")

    def now() -> float:
        return engine._clock()

    @bp.get("/nodes")
    def nodes():
        rows = engine.get_table()
        return jsonify({"server_time": now(), "count": len(rows), "nodes": rows})

    @bp.get("/summary")
    def summary():
        return jsonify({"server_time": now(), **engine.table.summary()})

    @bp.get("/least-loaded")
    def least_loaded():
        exclude = [x for x in request.args.get("exclude", "").split(",") if x]
        node = engine.get_least_loaded(exclude)
        return jsonify({"server_time": now(), "node": node,
                        "criteria": "lowest CPU among online nodes with fresh data (tie: lower RAM)"})

    @bp.get("/health")
    def health():
        h = engine.health()
        h["ok"] = h["status"] == "running"
        return jsonify(h), (200 if h["ok"] else 503)

    @bp.get("/history")
    def history():
        try:
            limit = max(1, min(int(request.args.get("limit", 100)), 500))
        except ValueError:
            return jsonify({"error": "limit must be an integer"}), 400
        kinds = [k for k in request.args.get("kinds", "").split(",") if k] or None
        return jsonify({"server_time": now(),
                        "events": engine.events(limit, kinds, request.args.get("node_id")),
                        "series": engine.table.series()})

    @bp.post("/heartbeat")
    def heartbeat():
        data: Optional[Dict[str, Any]] = request.get_json(silent=True)
        if not isinstance(data, dict) or not isinstance(data.get("node_id"), str) or not data["node_id"].strip():
            return jsonify({"error": "JSON body with a non-empty string 'node_id' is required"}), 400
        if data["node_id"] == engine.identity.node_id:
            return jsonify({"error": "cannot report a heartbeat for this node itself"}), 400
        info = {k: data[k] for k in ("name", "host", "port", "http_port") if k in data}
        created = engine.record_heartbeat(data["node_id"][:128], info)
        return jsonify({"accepted": True, "new_node": created,
                        "node": engine.table.get(data["node_id"][:128])}), 202

    return bp
