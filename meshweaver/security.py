
"""Cryptographic signing and verification for MeshWeaver tasks."""

import hashlib
import hmac


def generate_signature(data, secret_key):
    """Generate an HMAC-SHA256 signature for task bytes."""

    if not isinstance(data, bytes):
        raise TypeError("Task data must be bytes")

    if not isinstance(secret_key, bytes) or not secret_key:
        raise ValueError("A non-empty bytes secret key is required")

    return hmac.new(
        secret_key,
        data,
        hashlib.sha256
    ).digest()


def verify_signature(data, signature, secret_key):
    """Verify the authenticity and integrity of task bytes."""

    if not isinstance(signature, bytes):
        return False

    expected_signature = generate_signature(data, secret_key)

    return hmac.compare_digest(
        expected_signature,
        signature
    )
