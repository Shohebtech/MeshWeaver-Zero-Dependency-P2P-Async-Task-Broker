import hashlib
import hmac
import os
import struct
import time
from typing import Optional

TAG_BYTES = 32                       # length of an HMAC-SHA256 tag
STAMP = struct.Struct(">Q")          # 8 bytes: when the message was signed (unix seconds)
HEADER_BYTES = TAG_BYTES + STAMP.size
DEFAULT_MAX_AGE = 300.0              # seconds a signed message stays valid
MIN_SECRET_CHARS = 16
SECRET_VARIABLE = "MESHWEAVER_SECRET"


class SigningError(Exception):
    """The secret is unusable, or a message failed verification."""


def load_secret(variable: str = SECRET_VARIABLE) -> bytes:
    """Read the shared secret from an environment variable."""
    value = os.environ.get(variable, "")
    if len(value) < MIN_SECRET_CHARS:
        raise SigningError(
            f"set the {variable} environment variable to a secret of at least "
            f"{MIN_SECRET_CHARS} characters")
    return value.encode("utf-8")


def _tag(secret: bytes, body: bytes) -> bytes:
    return hmac.new(secret, body, hashlib.sha256).digest()


def sign(data: bytes, secret: bytes, now: Optional[float] = None) -> bytes:
    """Return tag + timestamp + data. Send this instead of the bare data."""
    if not isinstance(data, bytes) or not data:
        raise ValueError("data must be non-empty bytes")
    if not isinstance(secret, bytes) or not secret:
        raise ValueError("secret must be non-empty bytes")
    stamp = int(time.time() if now is None else now)
    body = STAMP.pack(stamp) + data
    return _tag(secret, body) + body


def verify(envelope: bytes, secret: bytes, max_age: Optional[float] = DEFAULT_MAX_AGE,
           now: Optional[float] = None) -> bytes:
    """
    Check the signature and the age, then return the original data.
    Raises SigningError for anything wrong. The signature is checked FIRST,
    before the timestamp or the data are looked at.
    """
    if not isinstance(envelope, bytes) or len(envelope) <= HEADER_BYTES:
        raise SigningError("message too short")
    tag, body = envelope[:TAG_BYTES], envelope[TAG_BYTES:]
    if not hmac.compare_digest(tag, _tag(secret, body)):   # constant-time comparison
        raise SigningError("bad signature")
    (stamp,) = STAMP.unpack(body[:STAMP.size])
    current = time.time() if now is None else now
    if max_age is not None and abs(current - stamp) > max_age:
        raise SigningError("message expired")
    return body[STAMP.size:]