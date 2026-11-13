# MeshWeaver Module 2: Kademlia DHT (peer discovery)

This module lets MeshWeaver nodes find each other **without a central server and without typed IP addresses**. It is a small, pure-Python implementation of the Kademlia distributed hash table, built on `asyncio` and UDP, with no third-party dependencies.

## What it does, in plain English

- Every node gets a random 160-bit ID. The "distance" between two nodes is the XOR of their IDs. This is only a number, not a physical distance.
- Each node keeps an address book (the routing table) of the peers it knows, grouped by distance.
- A new node knocks on a known range of ports (9000 to 9019 by default). The first node that answers becomes its entry point.
- The newcomer then searches the mesh for its own ID. This teaches it about its neighbours and teaches them about it.
- Nodes can also store small, expiring records (for example "node X exists") on the peers closest to a key.
- In the background, each node removes dead peers, refreshes its address book and republishes its records.

## Files

All source files are in `meshweaver/dht/`.

| File | What it does |
|---|---|
| `node_id.py` | Creates 160-bit IDs, hashes keys into the same space, XOR distance, bucket index |
| `contact.py` | `Contact`: a peer's ID, host and port, with dict conversion |
| `kbucket.py` | `KBucket`: up to 20 peers, ordered by when they were last seen; can split |
| `routing_table.py` | `RoutingTable`: all buckets, bucket splitting, `find_closest(target, n)` |
| `messages.py` | The four messages (PING, FIND_NODE, STORE, FIND_VALUE), JSON encoding, strict validation of incoming bytes |
| `rpc.py` | Tracks requests that await an answer (by request ID) and times them out |
| `transport.py` | Stand-in UDP socket layer: `start`, `send`, `set_receiver`, `local_addr`, `close` (includes a Windows fix) |
| `protocol.py` | `DHTProtocol`: answers the four message types and sends its own requests |
| `lookup.py` | Iterative search that asks 3 peers at a time (alpha = 3) |
| `bootstrap.py` | Port-scan bootstrap, manual `host:port` fallback, `join_mesh`, `start_node` |
| `storage.py` | Key/value store where every value expires; size and count limits |
| `network_store.py` | `put_value` and `get_value` across the closest peers |
| `maintenance.py` | Background jobs: dead-peer removal, bucket refresh, expired-value sweep, republishing |
| `node.py` | `DHTNode`: the single public entry point for other modules |

Tests are in `tests/` (one test file per source file, plus `test_audit.py`). Demo scripts are in `examples/`.

## Running the tests

From the repository root:

```
pip install pytest
python -m pytest -v
```

All tests should pass (123 at the time of writing).

## Mid-review demo: 10 separate processes, no hardcoded addresses

The audit starts 10 independent Python processes. No address is passed to any of them. It then checks that every node knows exactly the other nine.

```
python -m examples.audit
```

Expected result: a table with `9/9` and `ok` for every node, then `PASS: all 10 nodes found each other with no hardcoded addresses`.

Ports 9000 to 9019 must be free. Close any old node terminals first, because a leftover node makes the audit report nodes "outside this audit".

### Manual demo in separate terminals

Open 3 terminals in the repository root and run this in each, one after another:

```
python -m examples.node --name 1 --max-seconds 0
python -m examples.node --name 2 --max-seconds 0
python -m examples.node --name 3 --max-seconds 0
```

Each terminal prints how many peers it knows. The numbers rise to 2 as the others start. Press Ctrl+C to stop a node.

## Public API (for other modules)

Other modules should use only `DHTNode`.

```python
from meshweaver.dht.node import DHTNode

node = DHTNode()                       # defaults: 127.0.0.1, ports 9000-9019
node.on_peer_joined(lambda c: print("joined", c.port))
node.on_peer_left(lambda c: print("left", c.port))
await node.start()                     # open a port, join the mesh, start background jobs
...
await node.stop()
```

It also works as `async with DHTNode() as node:`.

| Call | Returns | Notes |
|---|---|---|
| `await node.start()` | `JoinResult` | `joined=False` means this node is the first one |
| `await node.stop()` | | Safe to call twice |
| `node.get_neighbors(n=None)` | list of `Contact` | Known peers nearest to this node, nearest first. No network traffic |
| `node.closest_peers(target, n=None)` | list of `Contact` | Known peers nearest to an ID or key (`int`, `str` or `bytes`) |
| `node.peers()` / `node.peer_count` | list / int | Every known peer |
| `node.knows(node_id)` | bool | |
| `await node.lookup_peers(target, n)` | list of `Contact` | Searches the live mesh (slower, fresher) |
| `await node.ping(contact)` | bool | |
| `await node.publish(key, value, ttl)` | int | Stores a small JSON-safe record on the closest nodes and keeps republishing it. Returns how many nodes accepted it |
| `node.unpublish(key)` | bool | Stops republishing |
| `await node.get(key)` | `(found, value)` | Reads a record from anywhere in the mesh |
| `node.on_peer_joined(fn)` / `node.on_peer_left(fn)` | unsubscribe function | `fn(contact)` may be normal or `async`. Register before `start()` to also see peers found while joining |

A `Contact` has `.node_id` (int), `.host`, `.port` and `.addr` (a `(host, port)` pair).

## Design decisions

- **Messages are JSON, never pickle.** Bytes from an unknown peer are treated as hostile: every field is validated and anything odd is dropped.
- **The sender's real source address is used,** not the address written inside the message.
- **Messages are capped at 8 KB** and stored values at 4 KB. The DHT holds small network-state records only. Task payloads must travel by another route.
- **Every stored value expires** (default 1 hour, maximum 24 hours), so records about dead nodes disappear on their own.
- **Windows:** `transport.py` switches off the ICMP "port unreachable" reset on UDP sockets. Without it, a node that scans empty ports can stop receiving on Windows.

## Known limitations

- Automatic discovery scans a fixed port range on one machine. On separate machines (for example Raspberry Pis), pass a manual entry point with `bootstrap=[("host", port)]`. LAN broadcast discovery is not implemented.
- One machine can host at most 20 nodes with the default port range.
- Messages are not yet signed or encrypted. TLS and signed task requests are planned for the security stage.
- `transport.py` is a stand-in for the shared UDP layer. Integration with the other modules is planned after the mid-review.
- Two nodes started at the exact same instant might each believe they are first. The periodic bucket refresh repairs this.
- A node that restarts on the same port with a new ID can leave its old ID in other nodes' tables until the liveness check removes it.