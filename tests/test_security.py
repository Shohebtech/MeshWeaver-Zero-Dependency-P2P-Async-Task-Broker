
"""Tests for MeshWeaver task signature verification."""

import pytest

from meshweaver.security import (
    generate_signature,
    verify_signature,
)


SECRET = b"meshweaver-test-secret"


def test_generate_signature():
    data = b"sample task data"

    signature = generate_signature(data, SECRET)

    assert isinstance(signature, bytes)
    assert len(signature) == 32


def test_valid_signature():
    data = b"sample task data"

    signature = generate_signature(data, SECRET)

    assert verify_signature(data, signature, SECRET) is True


def test_modified_data_rejected():
    data = b"sample task data"
    signature = generate_signature(data, SECRET)

    modified_data = b"modified task data"

    assert verify_signature(
        modified_data, signature, SECRET
    ) is False


def test_wrong_secret_rejected():
    data = b"sample task data"
    signature = generate_signature(data, SECRET)

    assert verify_signature(
        data, signature, b"wrong-secret"
    ) is False


def test_invalid_signature_rejected():
    data = b"sample task data"

    assert verify_signature(
        data, b"invalid-signature", SECRET
    ) is False


def test_empty_secret_rejected():
    with pytest.raises(ValueError):
        generate_signature(b"sample task data", b"")


def test_non_bytes_data_rejected():
    with pytest.raises(TypeError):
        generate_signature("sample task data", SECRET)
