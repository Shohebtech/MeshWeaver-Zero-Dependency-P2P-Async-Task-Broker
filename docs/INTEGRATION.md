# Integrating the other modules

Module 3 depends on two contracts and offers one. All are in `modules/interfaces.py`.
**Nothing from teammates is included here. The adapters below are mocks.**

| Module | You provide | Replace this mock | Wire it in |
|---|---|---|---|
| 1 Networking | class satisfying `Transport` | `modules/networking/mock_transport.py` | `app.build_runtime()` |
| 2 DHT | class satisfying `PeerDirectory` | `modules/dht/mock_directory.py` | `app.build_runtime()` |
| 4 Task execution | consumes `LoadProvider`, implements `TaskExecutor` | `modules/task_execution/mock_executor.py` | `app.build_runtime()` |

## Module 1: Transport
```python
async def start(handler)      # handler(message: dict, addr: (host, port)) is called per datagram
async def send(addr, message) # message is a JSON-serialisable dict; return True if handed to the socket
async def stop()
def info() -> dict            # must include "implementation" and "mock"; stats are shown on /network
attributes: mock: bool, implementation: str, port: int (actual bound UDP port)
```
Message format is documented at the top of `modules/interfaces.py`. If Module 1 wraps traffic in
TLS or its own envelope, keep the dict that reaches `handler` identical to what `send` received.

## Module 2: PeerDirectory
```python
get_peers() -> list[(host, udp_port)]   # neighbours to gossip with (UDP gossip port)
add_peer(addr, node_id=None) -> bool    # gossip tells you about peers it learned; True if new
remove_peer(addr); info() -> dict       # info must include "implementation" and "mock"
```
Gossip calls `add_peer` for every sender and every row address it sees, so a DHT can use gossip
as an extra discovery hint, or ignore it and return `False`.

## Module 4: using load data
```python
pick = engine.get_least_loaded(exclude=[failed_node_id])   # None if nobody qualifies
if not engine.is_online(task.node_id): ...                 # failed -> re-route
```
Poll `is_online` (or `GET /api/gossip/least-loaded`) when a task fails; `MockTaskExecutor` shows
the exact pattern. A real executor must only execute **signed** requests from trusted peers.

## Swap-in checklist
1. Edit `build_runtime()` in `app.py`: replace `MockUdpTransport`, `MockPeerDirectory`, `MockTaskExecutor`.
2. `python -m unittest discover -s tests -t .` (the engine tests use the in-memory transport and should still pass).
3. `python scripts/launch_mesh.py --nodes 10`; confirm on `/mesh` that the `mock` badges are gone.
4. Only after the real modules replace all three mocks and the end-to-end flow passes (discover, gossip,
   route, execute, result) can the system be called integrated. Until then this repo is Module 3 plus mocks.
