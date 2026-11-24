from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict

import aiosqlite
from fastapi import WebSocket, WebSocketDisconnect

from .schemas import BrokerStats, IngestEvent


class QueueBackpressureError(RuntimeError):
    """Raised when the bounded event queue has no available slot."""


class EventBroker:
    def __init__(self, db_path: str, queue_size: int = 1000, batch_size: int = 50, flush_interval: float = 0.5) -> None:
        if queue_size <= 0 or batch_size <= 0 or flush_interval <= 0:
            raise ValueError("broker bounds must be greater than zero")
        self.db_path = db_path
        self.queue: asyncio.Queue[IngestEvent] = asyncio.Queue(maxsize=queue_size)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._db: aiosqlite.Connection | None = None
        self._flush_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._persisted_total = 0
        self._dropped_backpressure = 0
        self._subscribers: DefaultDict[str, set[WebSocket]] = defaultdict(set)

    async def start(self) -> None:
        if self._flush_task is not None:
            return
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                source TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
            """
        )
        await self._db.commit()
        self._stop_event.clear()
        self._flush_task = asyncio.create_task(self._flush_loop(), name="streamforge-flush")

    async def stop(self) -> None:
        if self._flush_task is None:
            return
        self._stop_event.set()
        await self._flush_task
        self._flush_task = None
        if self._db is not None:
            await self._db.close()
            self._db = None

    def enqueue(self, event: IngestEvent) -> None:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull as exc:
            self._dropped_backpressure += 1
            raise QueueBackpressureError from exc

    async def subscribe(self, topic: str, websocket: WebSocket) -> None:
        self._subscribers[topic].add(websocket)

    async def unsubscribe(self, topic: str, websocket: WebSocket) -> None:
        subscribers = self._subscribers.get(topic)
        if subscribers is None:
            return
        subscribers.discard(websocket)
        if not subscribers:
            self._subscribers.pop(topic, None)

    async def stats(self) -> BrokerStats:
        return BrokerStats(
            queued=self.queue.qsize(),
            persisted_total=self._persisted_total,
            dropped_backpressure=self._dropped_backpressure,
            active_subscribers=sum(len(subscribers) for subscribers in self._subscribers.values()),
        )

    async def check_database(self) -> bool:
        if self._db is None:
            return False
        try:
            async with self._db.execute("SELECT 1") as cursor:
                await cursor.fetchone()
        except aiosqlite.Error:
            return False
        return True

    async def _flush_loop(self) -> None:
        while not self._stop_event.is_set() or not self.queue.empty():
            try:
                first_event = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                continue

            batch = [first_event]
            while len(batch) < self.batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await self._persist_batch(batch)
            for _ in batch:
                self.queue.task_done()

    async def _persist_batch(self, batch: list[IngestEvent]) -> None:
        if self._db is None:
            return
        rows = [
            (
                event.event_id,
                event.topic,
                event.source,
                json.dumps(event.payload, separators=(",", ":"), ensure_ascii=False),
                event.timestamp.isoformat(),
            )
            for event in batch
        ]
        try:
            cursor = await self._db.executemany(
                "INSERT OR IGNORE INTO events (event_id, topic, source, payload, timestamp) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
            await self._db.commit()
        except aiosqlite.Error:
            await self._db.rollback()
            raise

        self._persisted_total += max(cursor.rowcount, 0)
        for event in batch:
            await self._publish(event)

    async def _publish(self, event: IngestEvent) -> None:
        subscribers = set(self._subscribers.get(event.topic, ()))
        subscribers.update(self._subscribers.get("*", ()))
        if not subscribers:
            return
        broken: set[WebSocket] = set()
        for websocket in subscribers:
            try:
                await websocket.send_json(event.model_dump(mode="json"))
            except (WebSocketDisconnect, RuntimeError, OSError, ConnectionError):
                broken.add(websocket)
        for websocket in broken:
            for topic_subscribers in self._subscribers.values():
                topic_subscribers.discard(websocket)

