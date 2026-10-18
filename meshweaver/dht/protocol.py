from typing import Any, Dict, List, Optional, Tuple

from .contact import Contact
from .kbucket import K
from .messages import (
    FIND_NODE, FIND_VALUE, PING, STORE,
    Message, MessageError,
    contacts_from_payload, contacts_to_payload,
    decode, encode, hex_to_id, id_to_hex,
    make_request, make_response,
)
from .routing_table import RoutingTable
from .rpc import DEFAULT_TIMEOUT, PendingRequests, RPCTimeout


def _read_id(payload: Dict[str, Any], name: str) -> int:
    """Pull a hex ID out of a payload; any problem becomes a MessageError."""
    try:
        return hex_to_id(payload[name])
    except (KeyError, TypeError, ValueError) as e:
        raise MessageError(f"missing or bad {name!r}") from e


class DHTProtocol:
    """One DHT node: listens for requests, answers them, and sends its own."""

    def __init__(self, node_id: int, transport, k: int = K,
                 timeout: float = DEFAULT_TIMEOUT):
        self.node_id = node_id
        self.transport = transport
        self.k = k
        self.routing_table = RoutingTable(node_id, k)
        self.pending = PendingRequests(timeout)
        self.storage: Dict[int, Any] = {}  
        transport.set_receiver(self.datagram_received)

    @property
    def contact(self) -> Contact:
        host, port = self.transport.local_addr
        return Contact(self.node_id, host, port)

    def close(self) -> None:
        self.transport.close()

    
    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            msg = decode(data)
        except MessageError:
            return  # junk from the network: ignore silently
        if msg.sender.node_id == self.node_id:
            return  # a message "from ourselves" is never legitimate

        # Trust the real source address, not the one the message claims.
        sender = Contact(msg.sender.node_id, addr[0], addr[1])
        self.routing_table.add_contact(sender)  # we just heard from them: they're alive

        if msg.is_response:
            self.pending.resolve(msg)
            return
        try:
            reply = self._handle_request(msg, sender)
            self.transport.send(encode(reply), sender.addr)
        except MessageError:
            return  # bad request or reply too large: stay quiet

    def _handle_request(self, msg: Message, sender: Contact) -> Message:
        if msg.type == PING:
            return make_response(msg, self.contact)

        if msg.type == FIND_NODE:
            target = _read_id(msg.payload, "target")
            return make_response(msg, self.contact,
                                 {"contacts": self._closest(target, sender)})

        if msg.type == STORE:
            key = _read_id(msg.payload, "key")
            if "value" not in msg.payload:
                raise MessageError("missing value")
            self.storage[key] = msg.payload["value"]
            return make_response(msg, self.contact, {"stored": True})

        if msg.type == FIND_VALUE:
            key = _read_id(msg.payload, "key")
            if key in self.storage:
                return make_response(msg, self.contact,
                                     {"found": True, "value": self.storage[key]})
            return make_response(msg, self.contact,
                                 {"found": False, "contacts": self._closest(key, sender)})

        raise MessageError("unhandled message type")

    def _closest(self, target: int, asker: Contact) -> List[dict]:
        found = self.routing_table.find_closest(target, self.k, exclude={asker.node_id})
        return contacts_to_payload(found)

    
    async def _call(self, contact: Contact, msg_type: str,
                    payload: Optional[Dict[str, Any]] = None) -> Optional[Message]:
        request = make_request(msg_type, self.contact, payload)
        try:
            return await self.pending.request(
                request, lambda data: self.transport.send(data, contact.addr))
        except RPCTimeout:
            return None  # no answer

    async def ping(self, contact: Contact) -> bool:
        return await self._call(contact, PING) is not None

    async def find_node(self, contact: Contact, target: int) -> Optional[List[Contact]]:
        """Contacts the peer knows closest to target, or None if it did not answer."""
        resp = await self._call(contact, FIND_NODE, {"target": id_to_hex(target)})
        if resp is None:
            return None
        try:
            return contacts_from_payload(resp.payload.get("contacts", []))
        except MessageError:
            return None

    async def store(self, contact: Contact, key: int, value: Any) -> bool:
        resp = await self._call(contact, STORE, {"key": id_to_hex(key), "value": value})
        return resp is not None and resp.payload.get("stored") is True

    async def find_value(self, contact: Contact,
                         key: int) -> Optional[Tuple[bool, Any, List[Contact]]]:
        
        resp = await self._call(contact, FIND_VALUE, {"key": id_to_hex(key)})
        if resp is None:
            return None
        if resp.payload.get("found") is True and "value" in resp.payload:
            return (True, resp.payload["value"], [])
        try:
            return (False, None, contacts_from_payload(resp.payload.get("contacts", [])))
        except MessageError:
            return None