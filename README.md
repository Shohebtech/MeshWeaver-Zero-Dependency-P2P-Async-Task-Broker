# MeshWeaver: Module 3 (Gossip Protocol Engine) + web dashboard

MeshWeaver is a peer-to-peer async task broker (Infotact Python internship project). This repository
is **Module 3: the gossip engine**, implemented for real, with a five-page web dashboard.
Modules 1 (networking), 2 (DHT) and 4 (task execution) belong to teammates and are **mock adapters**
here, labelled as such in code and in the UI. **The full decentralized system is not claimed to work
until the real modules are integrated** (see `docs/INTEGRATION.md`).

## What works (verified)
* CPU/RAM sampling (psutil, or pure-Python `/proc` fallback).
* Gossip every 5 s over real UDP between separate OS processes; one seed address is enough.
* Load table with per-node timestamps, stale detection, heartbeat monitoring, offline detection.
* Least-loaded online node selection, REST API, live dashboard (auto-refresh 5 s).
* 35 automated tests; manual runs with 5 and 10 local node processes including killing a node.

## Install and run
```bash
cd meshweaver
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# one node  ->  http://127.0.0.1:8000
python app.py --name node-0 --port 9000 --http-port 8000

# a second node, in another terminal
python app.py --name node-1 --port 9001 --http-port 8001 --peers 127.0.0.1:9000

# or start N nodes at once (dashboards on 8000..800N-1)
python scripts/launch_mesh.py --nodes 10
```
Try failure detection: kill one node process; after 15 s (3 intervals) the others mark it offline.
Demo without a mesh: `python app.py --simulate 4` adds labelled **simulated** nodes.
Shared secret (HMAC-signed gossip): add `--secret yoursecret` to every node.
Charts need Chart.js: loaded from a CDN, or place `chart.umd.min.js` in `frontend/static/js/vendor/` to work offline.

## Tests
```bash
python -m unittest discover -s tests -t . -v      # no extra packages needed
```
Unit: monitor, table, heartbeat, engine (in-memory network), REST API. Integration: 3 engines over real UDP.

## Dashboard
Overview, Mesh network, **Gossip monitoring** (live CPU/RAM, load table, countdown to next broadcast,
heartbeat status, stale/offline warnings, least-loaded node, CPU and RAM charts, gossip history),
Task management (mock), Network monitoring. Purple "mock" badge = placeholder module;
"real p2p" = received over UDP; "simulated" = demo rows.

## Layout
```
app.py  config.py  dashboard_api.py  requirements.txt
modules/interfaces.py              shared contracts for all four modules
modules/gossip/                    MODULE 3: engine, load_monitor, load_table, heartbeat, gossip_api
modules/networking|dht|task_execution/   mock adapters (Modules 1, 2, 4)
frontend/templates, frontend/static      dashboard
tests/  docs/  scripts/  tools/
```
Docs: `docs/ARCHITECTURE.md`, `docs/API.md`, `docs/INTEGRATION.md`, `docs/COMMIT_ROADMAP.md`.

## Known limits
Plaintext UDP unless `--secret` is used (TLS belongs to Module 1); tested on one machine only;
heartbeat fan-out is O(n) per node. The PDF's Rich CLI dashboard is replaced here by the web dashboard.
