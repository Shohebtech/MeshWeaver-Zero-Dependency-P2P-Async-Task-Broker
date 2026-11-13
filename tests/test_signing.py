import pytest

from meshweaver.integration.signing import SigningError, load_secret, sign, verify

SECRET = b"a-shared-secret-for-tests"


def test_signed_message_verifies_and_returns_the_original_bytes():
    data = bytes(range(256)) * 20                  # binary data, like a pickled task
    assert verify(sign(data, SECRET, now=1000), SECRET, now=1010) == data


def test_changing_any_part_of_the_message_is_detected():
    envelope = sign(b"run task", SECRET, now=1000)
    for index in (0, 32, 40, len(envelope) - 1):   # tag, timestamp, payload start, payload end
        broken = bytearray(envelope)
        broken[index] ^= 1
        with pytest.raises(SigningError):
            verify(bytes(broken), SECRET, now=1000)


def test_wrong_secret_is_rejected():
    envelope = sign(b"run task", SECRET, now=1000)
    with pytest.raises(SigningError):
        verify(envelope, b"some-other-secret-value", now=1000)


def test_old_and_future_messages_are_rejected():
    envelope = sign(b"run task", SECRET, now=1000)
    assert verify(envelope, SECRET, max_age=300, now=1299) == b"run task"
    for now in (1301, 699):                        # too old, and too far in the future
        with pytest.raises(SigningError):
            verify(envelope, SECRET, max_age=300, now=now)


def test_too_short_or_wrong_type_is_rejected():
    for bad in (b"", b"x" * 40, None, "text"):
        with pytest.raises(SigningError):
            verify(bad, SECRET)                    # type: ignore[arg-type]


def test_load_secret_needs_a_long_enough_environment_variable(monkeypatch):
    monkeypatch.delenv("MESHWEAVER_SECRET", raising=False)
    with pytest.raises(SigningError):
        load_secret()
    monkeypatch.setenv("MESHWEAVER_SECRET", "too-short")
    with pytest.raises(SigningError):
        load_secret()
    monkeypatch.setenv("MESHWEAVER_SECRET", "a-long-enough-secret")
    assert load_secret() == b"a-long-enough-secret"