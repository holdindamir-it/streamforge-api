import asyncio
import hashlib
import hmac
import json
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from streamforge.app import create_app
from streamforge.config import Settings


def signed_headers(body: bytes, secret: str) -> dict[str, str]:
    timestamp = int(time.time())
    signature = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return {
        "content-type": "application/json",
        "x-streamforge-signature": f"sha256={signature}",
        "x-streamforge-timestamp": str(timestamp),
    }


def event_body(event_id: str = "event-1", topic: str = "orders") -> bytes:
    return json.dumps(
        {
            "event_id": event_id,
            "topic": topic,
            "source": "tests",
            "payload": {"value": 1},
            "timestamp": "2026-01-01T00:00:00Z",
        },
        separators=(",", ":"),
    ).encode()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(hmac_secret="test-secret", db_path=str(tmp_path / "events.db"), batch_size=2, flush_interval=0.05)


@pytest.mark.asyncio
async def test_ingestion_batch_persistence_and_idempotency(settings: Settings) -> None:
    api = create_app(settings)
    transport = httpx.ASGITransport(app=api)
    async with api.router.lifespan_context(api), httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        body = event_body()
        response = await client.post("/v1/events", content=body, headers=signed_headers(body, settings.hmac_secret))
        assert response.status_code == 202
        duplicate = await client.post("/v1/events", content=body, headers=signed_headers(body, settings.hmac_secret))
        assert duplicate.status_code == 202
        await asyncio.sleep(0.15)
        stats = await client.get("/v1/stats")
        assert stats.json()["persisted_total"] == 1
        health = await client.get("/health")
        assert health.status_code == 200


@pytest.mark.asyncio
async def test_queue_overflow_returns_503(tmp_path: Path) -> None:
    settings = Settings(hmac_secret="test-secret", db_path=str(tmp_path / "events.db"), queue_size=1, flush_interval=10)
    api = create_app(settings)
    transport = httpx.ASGITransport(app=api)
    async with api.router.lifespan_context(api), httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        body = event_body()
        headers = signed_headers(body, settings.hmac_secret)
        assert (await client.post("/v1/events", content=body, headers=headers)).status_code == 202
        overflow_body = event_body("event-2")
        overflow = await client.post("/v1/events", content=overflow_body, headers=signed_headers(overflow_body, settings.hmac_secret))
        assert overflow.status_code == 503
        assert overflow.headers["retry-after"] == "1"


def test_websocket_topic_delivery(settings: Settings) -> None:
    api = create_app(settings)
    with TestClient(api) as client:
        with client.websocket_connect("/v1/stream/orders") as websocket:
            body = event_body(topic="orders")
            response = client.post("/v1/events", content=body, headers=signed_headers(body, settings.hmac_secret))
            assert response.status_code == 202
            assert websocket.receive_json()["topic"] == "orders"

            other_body = event_body("event-2", topic="billing")
            response = client.post("/v1/events", content=other_body, headers=signed_headers(other_body, settings.hmac_secret))
            assert response.status_code == 202
            other_response = client.get("/v1/stats")
            assert other_response.json()["persisted_total"] == 2

