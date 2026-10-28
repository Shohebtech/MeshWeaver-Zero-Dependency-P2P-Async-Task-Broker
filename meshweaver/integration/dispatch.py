import json
from typing import Callable, Dict, List, Optional

from meshweaver.dht.messages import MESSAGE_TYPES
from meshweaver.dht.transport import Address, Receiver


class Dispatcher:
    """
    One UDP socket shared by several modules.

    Wrap an already-started transport, then:
      - give the Dispatcher to DHTProtocol in place of the transport
        (DHT traffic arrives through the default receiver), and
      - claim other message types with add_route(), for example "LOAD_UPDATE".

    Every handler is a plain function: handler(data: bytes, addr: (host, port)).
    """

    def __init__(self, transport):
        self._transport = transport
        self._routes: Dict[str, Receiver] = {}
        self._default: Optional[Receiver] = None
        transport.set_receiver(self._on_datagram)

    # ---- the same surface DHTProtocol expects from a transport ----
    @property
    def local_addr(self) -> Address:
        return self._transport.local_addr

    def send(self, data: bytes, addr: Address) -> None:
        self._transport.send(data, addr)

    def set_receiver(self, callback: Receiver) -> None:
        """Receives everything that no route claims (this is where the DHT sits)."""
        self._default = callback

    def close(self) -> None:
        self._transport.close()

    # ---- routing ----
    def add_route(self, msg_type: str, handler: Receiver) -> None:
        if isinstance(msg_type, bool) or not isinstance(msg_type, str) or not msg_type:
            raise ValueError("msg_type must be a non-empty string")
        if msg_type in MESSAGE_TYPES:
            raise ValueError(f"{msg_type!r} belongs to the DHT and cannot be claimed")
        if msg_type in self._routes:
            raise ValueError(f"{msg_type!r} is already routed")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._routes[msg_type] = handler

    def remove_route(self, msg_type: str) -> bool:
        return self._routes.pop(msg_type, None) is not None

    @property
    def routes(self) -> List[str]:
        return sorted(self._routes)

    # ---- receiving ----
    def _pick(self, data: bytes) -> Optional[Receiver]:
        try:
            obj = json.loads(data)
        except (ValueError, RecursionError):
            return self._default          # unreadable: the default receiver ignores junk
        if isinstance(obj, dict):
            msg_type = obj.get("type")
            if isinstance(msg_type, str):
                route = self._routes.get(msg_type)
                if route is not None:
                    return route
        return self._default

    def _on_datagram(self, data: bytes, addr: Address) -> None:
        callback = self._pick(data)
        if callback is None:
            return                        # nobody wants it: drop silently
        try:
            callback(data, addr)
        except Exception:
            pass                          # one broken handler must not stop the socket