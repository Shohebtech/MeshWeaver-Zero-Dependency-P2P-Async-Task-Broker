# REST API

Base URL: `http://127.0.0.1:<http-port>` (default 8000). All responses are JSON and include
`server_time` (unix seconds, this node's clock). Timestamps are unix seconds; ages are seconds.

## Module 3 endpoints

### GET /api/gossip/nodes
The load table. Row fields: `node_id, name, host, port (UDP), http_port, cpu, ram` (percent, or null
until the first reading), `status` (online|offline), `stale` (bool), `last_update`, `last_heartbeat`,
`update_age`, `heartbeat_age`, `seq`, `source` (real|reported|simulated), `is_self`, `links`.
```json
{"count": 2, "nodes": [{"node_id": "70bacfa8e44a0929", "name": "node-0", "cpu": 12.5, "ram": 33.1,
  "status": "online", "stale": false, "update_age": 1.2, "heartbeat_age": 0.4, "source": "real"}]}
```

### GET /api/gossip/summary
`total_nodes, online_nodes, offline_nodes, stale_nodes, avg_cpu, avg_ram` (online nodes with data),
`least_loaded` (row or null), `last_update, real_nodes, simulated_nodes, gossip_interval,
stale_after, offline_after`.

### GET /api/gossip/least-loaded[?exclude=id1,id2]
`{"node": <row or null>, "criteria": "..."}`. `null` means no online node has fresh data.

### GET /api/gossip/health
Engine status (`running`), identity, interval and thresholds, `last_tick`, `next_tick_at`,
`monitor_backend`, `signed_messages`, `stats` counters, and `transport` / `directory` info with
`mock` flags. HTTP 503 when the engine is not running.

### GET /api/gossip/history[?limit=100&kinds=a,b&node_id=...]
`events` (newest first; kinds: `tick, gossip_sent, gossip_recv, heartbeat_sent, heartbeat_recv,
peer_discovered, status_change, auth_failed`) and `series`: `{node_id: [{t, cpu, ram}, ...]}`.
`limit` must be an integer (400 otherwise); max 500.

### POST /api/gossip/heartbeat
Body: `{"node_id": "ext-1", "name": "optional", "host": "optional", "port": 9100}`.
Records that a node is alive (not load data). Returns 202 `{accepted, new_node, node}`;
400 if `node_id` is missing/empty/not a string or is this node's own id.
```bash
curl -X POST localhost:8000/api/gossip/heartbeat -H 'Content-Type: application/json' \
     -d '{"node_id":"ext-1","name":"external"}'
```

## Dashboard endpoints
`GET /api/overview`, `GET /api/mesh`, `GET /api/network`, `GET /api/tasks` (mock),
`POST /api/tasks/demo` (mock: submit a simulated task).

## Python interface for other modules
```python
from modules.interfaces import LoadProvider        # implemented by GossipEngine
engine.get_table()                  # list of rows, as in /nodes
engine.get_least_loaded(exclude=[]) # row or None (online + fresh only)
engine.is_online(node_id)           # bool
```
