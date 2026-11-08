# Architecture (Module 3: Gossip Protocol Engine)

MeshWeaver has four modules (see the project report): networking, DHT, gossip, task execution.
This repository contains **Module 3 as a real implementation**, plus a web dashboard, plus
clearly labelled **mock adapters** for the other three modules.

```
            +------------------------- one node (one OS process) -------------------------+
            |                                                                              |
 browser -> |  Flask: pages + /api/gossip/* + /api/{overview,mesh,tasks,network}           |
            |        |                                                                     |
            |        v                                  asyncio thread                     |
            |  GossipEngine  <-- LoadMonitor (psutil | /proc)                              |
            |   |   |   |                                                                  |
            |   |   |   +-- HeartbeatMonitor --+                                           |
            |   |   +------ LoadTable <--------+  (thread-safe, per-node timestamps)       |
            |   |                                                                          |
            |   +-- Transport   (Module 1)  <- MockUdpTransport  (real UDP, mock module)  |
            |   +-- PeerDirectory (Module 2) <- MockPeerDirectory (NOT Kademlia)           |
            |   +-- TaskExecutor (Module 4)  <- MockTaskExecutor  (simulated only)         |
            +------------------------------------------------------------------------------+
```

## Gossip round (every 5 s, `config.GossipConfig.interval`)
1. Sample CPU/RAM (`load_monitor.py`).
2. Update this node's row; its `seq` is `max(old + 1, unix_ms)` so it also survives restarts.
3. Push up to 20 table rows to `fanout` (3) random neighbours (`gossip` message).
4. Send a small `heartbeat` message to every known neighbour.
5. On receipt: merge rows (higher `seq` wins), learn new peers from sender info and row
   addresses, and record a heartbeat for the sender.

One seed address is enough for a mesh to form: neighbours learn about each other from the rows
and peer lists carried in gossip. (The PDF lists "first contact" as an open decision; a real DHT
may replace this.)

## Status rules (`load_table.py`)
| State   | Rule (interval = 5 s)                                          |
|---------|-----------------------------------------------------------------|
| online  | something heard from the node within 15 s (3 intervals)         |
| offline | nothing heard (update or heartbeat) for more than 15 s          |
| stale   | load reading older than 10 s (2 intervals); node may be alive   |

`least-loaded` only returns nodes that are **online and not stale**: lowest CPU, ties broken by
lower RAM, then node id.

## Real vs simulated vs mock
Every row has `source`: `real` (arrived over UDP gossip), `reported` (REST heartbeat only),
`simulated` (`--simulate N` demo rows, never gossiped). Mock *modules* are flagged `mock: true`
in `/api/gossip/health`, `/api/mesh`, `/api/tasks`, and shown with purple "mock" badges.
`MockUdpTransport` is a mock *module* but carries **real** UDP traffic.

## Limits (known)
* Gossip is plaintext JSON over UDP unless a shared secret is set (`--secret`, HMAC-SHA256 +
  timestamp window). TLS is Module 1's job and is **not** implemented here. No replay cache.
* Heartbeats go to every known peer: fine for tens of nodes, O(n) per node beyond that.
* Tested: 10 real local processes, unit tests, and an in-memory 4-node chain. Not tested across
  multiple machines, NAT, packet loss, or IPv6.
* The PDF asks for a Rich *CLI* dashboard; this project's dashboard is web-based as requested.
