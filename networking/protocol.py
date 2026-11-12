
import json
from typing import Any
import uuid

# Supported message types
# Supported message types
MESSAGE_TYPES = {
    # Networking
    "PING",
    "PONG",
    "ERROR",

    # Task broker
    "TASK",
    "RESULT",

    # Peer discovery
    "DISCOVERY",
    "DISCOVERY_RESPONSE",

    # Load management
    "LOAD_UPDATE",
}


def create_message(
    message_type: str,
    sender_id: str,
    receiver_id: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a standard message for communication between peers.
    """

    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"Unsupported message type: {message_type}")

    if not sender_id:
        raise ValueError("sender_id cannot be empty")

    if not receiver_id:
        raise ValueError("receiver_id cannot be empty")

    return {
        "message_id": str(uuid.uuid4()),
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "type": message_type,
        "payload": payload or {},
    }


def encode_message(message: dict[str, Any]) -> str:
    """
    Convert a Python message dictionary into a JSON string.
    """

    validate_message(message)

    return json.dumps(message)


def decode_message(data: str) -> dict[str, Any]:
    """
    Convert a JSON string into a Python message dictionary.
    """

    try:
        message = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON message") from exc

    validate_message(message)

    return message


def validate_message(message: Any) -> None:
    """
    Validate the structure of a communication message.
    """

    if not isinstance(message, dict):
        raise ValueError("Message must be a dictionary")

    required_fields = {
        "message_id",
        "sender_id",
        "receiver_id",
        "type",
        "payload",
    }

    missing_fields = required_fields - message.keys()

    if missing_fields:
        raise ValueError(
            f"Missing required fields: {', '.join(sorted(missing_fields))}"
        )

    if message["type"] not in MESSAGE_TYPES:
        raise ValueError(
            f"Unsupported message type: {message['type']}"
        )

    if not isinstance(message["message_id"], str) or not message["message_id"]:
        raise ValueError("message_id must be a non-empty string")

    if not isinstance(message["sender_id"], str) or not message["sender_id"]:
        raise ValueError("sender_id must be a non-empty string")

    if not isinstance(message["receiver_id"], str) or not message["receiver_id"]:
        raise ValueError("receiver_id must be a non-empty string")

    if not isinstance(message["payload"], dict):
        raise ValueError("payload must be a dictionary")
