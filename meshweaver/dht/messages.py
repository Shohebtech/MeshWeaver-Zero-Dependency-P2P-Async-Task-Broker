import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional 

from .contact import Contact
from .node_id import ID_BITS

PROTOCOL_VERSION = 1
MAX_MESSAGE_SIZE = 8192  

PING = "PING"
FIND_NODE = "FIND_NODE"
STORE = "STORE"
FIND_VALUE = "FIND_VALUE"
MESSAGE_TYPES = (PING, FIND_NODE, STORE, FIND_VALUE)


class MessageError(ValueError):
    """Raised when a message cannot be built, encoded or decoded."""

@dataclass
class Message:
    type: str                      
    msg_id: str                    
    sender: Contact                
    payload: Dict[str, Any] = field(default_factory=dict)
    is_response: bool = False


def id_to_hex(node_id: int) -> str:
    return format(node_id, "040x")


def hex_to_id(text: str) -> int:
    value = int(text, 16)
    if not 0 <= value < 2 ** ID_BITS:
        raise MessageError("ID out of range")
    return value


def new_msg_id() -> str:
    return os.urandom(16).hex()



def make_request(msg_type: str, sender: Contact, payload: Optional[Dict[str, Any]] = None) -> Message:
    if msg_type not in MESSAGE_TYPES:
        raise MessageError(f"unknown message type: {msg_type!r}")
    return Message(msg_type, new_msg_id(), sender, dict(payload or {}), False)


def make_response(request: Message, sender: Contact, payload: Optional[Dict[str, Any]] = None) -> Message:

    return Message(request.type, request.msg_id, sender, dict(payload or {}), True)


def contacts_to_payload(contacts: Iterable[Contact]) -> List[dict]:
    return [c.to_dict() for c in contacts]


def contacts_from_payload(items) -> List[Contact]:
    if not isinstance(items, list):
        raise MessageError("contacts must be a list")
    return [_parse_contact(item) for item in items]


def _parse_contact(d) -> Contact:
    if not isinstance(d, dict):
        raise MessageError("contact must be an object")
    try:
        node_id = hex_to_id(d["id"])
        host, port = d["host"], d["port"]
    except (KeyError, TypeError, ValueError) as e:
        raise MessageError("malformed contact") from e
    if not isinstance(host, str) or not host:
        raise MessageError("bad host")
    if not isinstance(port, int) or isinstance(port, bool) or not 0 < port < 65536:
        raise MessageError("bad port")
    return Contact(node_id, host, port)


def encode(message: Message) -> bytes:
    data = json.dumps(
        {
            "v": PROTOCOL_VERSION,
            "type": message.type,
            "id": message.msg_id,
            "resp": message.is_response,
            "sender": message.sender.to_dict(),
            "data": message.payload,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    if len(data) > MAX_MESSAGE_SIZE:
        raise MessageError("message too large")
    return data


def decode(data: bytes) -> Message:
    
    if len(data) > MAX_MESSAGE_SIZE:
        raise MessageError("message too large")
    try:
        obj = json.loads(data.decode("utf-8"))
    except ValueError as e:  # covers bad UTF-8 and bad JSON
        raise MessageError("not valid JSON") from e
    if not isinstance(obj, dict):
        raise MessageError("message must be an object")
    if obj.get("v") != PROTOCOL_VERSION:
        raise MessageError("unsupported protocol version")
    msg_type = obj.get("type")
    if msg_type not in MESSAGE_TYPES:
        raise MessageError("unknown message type")
    msg_id = obj.get("id")
    if not isinstance(msg_id, str) or not 0 < len(msg_id) <= 64:
        raise MessageError("bad message id")
    is_response = obj.get("resp")
    if not isinstance(is_response, bool):
        raise MessageError("bad response flag")
    payload = obj.get("data")
    if not isinstance(payload, dict):
        raise MessageError("bad payload")
    sender = _parse_contact(obj.get("sender"))
    return Message(msg_type, msg_id, sender, payload, is_response)