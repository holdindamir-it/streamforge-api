from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class IngestEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex, min_length=1, max_length=128)
    topic: str = Field(pattern=r"^[a-z0-9._-]{2,64}$")
    source: str = Field(min_length=1, max_length=256)
    payload: dict[str, Any]
    timestamp: datetime

    @field_validator("payload")
    @classmethod
    def validate_payload_size(cls, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            payload_size = len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise ValueError("payload must be JSON serializable") from exc
        if payload_size > 32 * 1024:
            raise ValueError("payload exceeds 32 KiB")
        return payload

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("timestamp must include a UTC offset")
        return timestamp.astimezone(timezone.utc)


class IngestAck(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    accepted: bool
    event_id: str
    queue_depth: int


class BrokerStats(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    queued: int
    persisted_total: int
    dropped_backpressure: int
    active_subscribers: int

