from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    hmac_secret: str = "change-me"
    db_path: str = "streamforge.db"
    queue_size: int = 1000
    batch_size: int = 50
    flush_interval: float = 0.5
    rate_limit: int = 120
    rate_window_sec: float = 60.0
    max_timestamp_drift_sec: int = 300

    @classmethod
    def from_env(cls) -> "Settings":
        defaults = cls()
        return cls(
            hmac_secret=os.getenv("STREAMFORGE_HMAC_SECRET", defaults.hmac_secret),
            db_path=os.getenv("STREAMFORGE_DB_PATH", defaults.db_path),
            queue_size=_env_int("STREAMFORGE_QUEUE_SIZE", defaults.queue_size),
            batch_size=_env_int("STREAMFORGE_BATCH_SIZE", defaults.batch_size),
            flush_interval=_env_float("STREAMFORGE_FLUSH_INTERVAL", defaults.flush_interval),
            rate_limit=_env_int("STREAMFORGE_RATE_LIMIT", defaults.rate_limit),
        )


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed

