from __future__ import annotations

import hashlib
import hmac
import time
from collections import defaultdict, deque


def verify_hmac_signature(
    raw_body: bytes,
    signature_header: str | None,
    timestamp_header: str | None,
    secret: str,
    max_drift_sec: int = 300,
) -> None:
    if not signature_header or not timestamp_header:
        raise ValueError("signature and timestamp headers are required")
    if not signature_header.startswith("sha256="):
        raise ValueError("unsupported signature format")

    supplied_signature = signature_header[7:]
    if len(supplied_signature) != hashlib.sha256().digest_size * 2:
        raise ValueError("invalid signature length")
    try:
        int(supplied_signature, 16)
        timestamp = int(timestamp_header)
    except ValueError as exc:
        raise ValueError("invalid signature headers") from exc

    if abs(time.time() - timestamp) > max_drift_sec:
        raise ValueError("signature timestamp is outside the allowed drift")

    signed_body = f"{timestamp}.".encode() + raw_body
    expected_signature = hmac.new(secret.encode(), signed_body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_signature, supplied_signature):
        raise ValueError("signature mismatch")


class SlidingWindowLimiter:
    def __init__(self, max_requests: int, window_sec: float = 60.0) -> None:
        if max_requests <= 0 or window_sec <= 0:
            raise ValueError("limiter bounds must be greater than zero")
        self.max_requests = max_requests
        self.window_sec = window_sec
        self._requests: defaultdict[str, deque[float]] = defaultdict(deque)

    def allow(self, client_id: str) -> bool:
        now = time.monotonic()
        request_times = self._requests[client_id]
        cutoff = now - self.window_sec
        while request_times and request_times[0] <= cutoff:
            request_times.popleft()
        if len(request_times) >= self.max_requests:
            return False
        request_times.append(now)
        return True

