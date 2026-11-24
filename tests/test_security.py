import hashlib
import hmac
import time

import pytest

from streamforge.security import SlidingWindowLimiter, verify_hmac_signature


def make_signature(body: bytes, timestamp: int, secret: str) -> str:
    signed = f"{timestamp}.".encode() + body
    return "sha256=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


def test_valid_signature() -> None:
    body = b'{"topic":"orders"}'
    timestamp = int(time.time())
    verify_hmac_signature(body, make_signature(body, timestamp, "secret"), str(timestamp), "secret")


@pytest.mark.parametrize(
    "body, secret",
    [(b'{"topic":"changed"}', "secret"), (b'{"topic":"orders"}', "wrong")],
)
def test_tampered_body_and_wrong_secret_are_rejected(body: bytes, secret: str) -> None:
    timestamp = int(time.time())
    signature = make_signature(b'{"topic":"orders"}', timestamp, "secret")
    with pytest.raises(ValueError):
        verify_hmac_signature(body, signature, str(timestamp), secret)


def test_replay_drift_is_rejected() -> None:
    timestamp = int(time.time()) - 301
    body = b"{}"
    with pytest.raises(ValueError):
        verify_hmac_signature(body, make_signature(body, timestamp, "secret"), str(timestamp), "secret")


def test_sliding_window_cutoff_and_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = iter([100.0, 100.1, 100.2, 101.1])
    monkeypatch.setattr("streamforge.security.time.monotonic", lambda: next(clock))
    limiter = SlidingWindowLimiter(max_requests=2, window_sec=1.0)
    assert limiter.allow("127.0.0.1")
    assert limiter.allow("127.0.0.1")
    assert not limiter.allow("127.0.0.1")
    assert limiter.allow("127.0.0.1")

