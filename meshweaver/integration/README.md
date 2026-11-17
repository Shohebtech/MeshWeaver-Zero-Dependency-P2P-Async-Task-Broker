# MeshWeaver integration layer

This folder connects the four modules into one working node:

| Module | Where it lives | Role in a node |
|---|---|---|
| 2. Kademlia DHT | `meshweaver/dht/` | Finds the other nodes, no central server and no typed IPs |
| 3. Gossip | `modules/gossip/` | Shares CPU/RAM load and detects silent nodes |
| 4. Tasks | `meshweaver/serializer.py`, `meshweaver/executor.py` | Packages a function (cloudpickle) and runs it |
| Glue | `meshweaver/integration/` | This folder |

## How a task travels

1. `submit` serializes a function with Module 4's `serialize_task`.
2. The task manager asks the gossip engine for the least busy node.
3. The task is signed (HMAC, with a timestamp and task ID) and sent over TCP to that node. Task port = DHT port + 1000.
4. The receiving node checks the signature first, rejects repeated task IDs, and runs the task in a separate process with a timeout.
5. The result comes back signed. If the node cannot be reached, or gossip reports it offline, the task is sent to the next best node (3 attempts at most).

The DHT and gossip share one UDP socket. The dispatcher sorts messages by their `"type"` field: `PING`, `FIND_NODE`, `STORE` and `FIND_VALUE` go to the DHT, and `gossip` and `heartbeat` go to the gossip engine.

## Files

| File | What it does |
|---|---|
| `dispatch.py` | One UDP socket shared by the DHT and gossip, routed by message type |
| `gossip_bridge.py` | Gives the gossip engine its transport and peer list from the DHT; adds lowest-CPU lookup and a peer-lost callback |
| `gossip_adapter.py` | Older adapter for a previous gossip layout; still used for gossip ID conversion |
| `task_channel.py` | TCP server and client with length-prefixed framing and size limits |
| `signing.py` | HMAC signing and verification, checked before anything is unpickled |
| `task_runner.py` | Runs each task in a separate process and kills it on timeout |
| `task_receiver.py` | Verifies, de-duplicates, runs a task and signs the reply |
| `task_manager.py` | Picks the target node, sends the task, re-sends it if the node fails |
| `node_runner.py` | `MeshNode`: starts everything for one node |
| `cli.py` | `run` and `submit` commands |
| `tls.py` | Optional TLS helpers for the task channel (not wired in, not tested end to end) |

## Running it

From the repository root, with the dependencies installed (`pip install -r requirements.txt`).

Every terminal needs the same secret, at least 16 characters:

```
set MESHWEAVER_SECRET=a-long-enough-secret
```

(PowerShell: `$env:MESHWEAVER_SECRET="a-long-enough-secret"`)

Start two nodes in two terminals, then send a test task from a third:

```
python -m meshweaver.integration.cli run
python -m meshweaver.integration.cli run
python -m meshweaver.integration.cli submit
```

Expected output of `submit`: `completed on node xxxxxxxx: 2432902008176640000` (20!), run on one of the other nodes.

`submit --slow 25` sends a task that sleeps for 25 seconds, so you can stop its node and watch the task move.

Ports: DHT and gossip use UDP 9000 to 9019, and tasks use TCP 10000 to 10019. Stop `app.py` first if it is running, because it uses UDP 9000.

## Tested

- Unit tests for each piece: `python -m pytest tests -q`
- Three terminals on one machine: nodes found each other, shared load, and a task ran on a different node with the correct result.
- Four terminals on one machine: a 25-second task was sent to a node, that node was stopped, and the task completed on a different node.

## Not yet verified

- The dashboard showing these real nodes (it runs on simulated nodes).
- TLS on the task channel.
- Separate machines or Raspberry Pis: discovery scans a port range on one machine, and other machines need `--bootstrap host:port`.

## Known limitations

- Shared-secret signing keeps strangers out. It does not protect against a node that already holds the secret, and tasks are not sandboxed.
- A task may run twice if a node is slow or dies after finishing but before replying, so tasks should be safe to repeat.
- Module 4's `remote.py` is a separate blocking demo and is not used by the mesh.
- The loose gossip files in the repository root (`engine.py`, `heartbeat.py`, `table.py` and so on) are an older copy. The mesh uses `modules/gossip/`.