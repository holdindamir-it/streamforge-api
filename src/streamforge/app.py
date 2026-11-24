from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from starlette.responses import JSONResponse

from .broker import EventBroker, QueueBackpressureError
from .config import Settings
from .schemas import BrokerStats, IngestAck, IngestEvent
from .security import SlidingWindowLimiter, verify_hmac_signature


def create_app(settings: Settings | None = None) -> FastAPI:
    service_settings = settings or Settings.from_env()
    broker = EventBroker(
        db_path=service_settings.db_path,
        queue_size=service_settings.queue_size,
        batch_size=service_settings.batch_size,
        flush_interval=service_settings.flush_interval,
    )
    limiter = SlidingWindowLimiter(service_settings.rate_limit, service_settings.rate_window_sec)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await broker.start()
        try:
            yield
        finally:
            await broker.stop()

    api = FastAPI(title="streamforge-api", version="0.1.0", lifespan=lifespan)
    api.state.broker = broker
    api.state.settings = service_settings

    @api.post("/v1/events", response_model=IngestAck, status_code=202)
    async def ingest_event(request: Request) -> IngestAck:
        client_id = request.client.host if request.client else "unknown"
        if not limiter.allow(client_id):
            raise HTTPException(status_code=429, detail="rate limit exceeded")

        raw_body = await request.body()
        signature_header = _first_header(request, "x-streamforge-signature", "x-signature", "x-webhook-signature")
        timestamp_header = _first_header(request, "x-streamforge-timestamp", "x-timestamp", "x-webhook-timestamp")
        try:
            verify_hmac_signature(
                raw_body,
                signature_header,
                timestamp_header,
                service_settings.hmac_secret,
                service_settings.max_timestamp_drift_sec,
            )
        except ValueError as exc:
            raise HTTPException(status_code=401, detail="invalid webhook signature") from exc

        try:
            event = IngestEvent.model_validate_json(raw_body)
        except ValidationError as exc:
            detail = [{"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]} for error in exc.errors()]
            return JSONResponse(status_code=422, content={"detail": detail})

        try:
            broker.enqueue(event)
        except QueueBackpressureError as exc:
            raise HTTPException(
                status_code=503,
                detail="event queue is full",
                headers={"Retry-After": "1"},
            ) from exc
        return IngestAck(accepted=True, event_id=event.event_id, queue_depth=broker.queue.qsize())

    @api.websocket("/v1/stream/{topic}")
    async def stream_events(websocket: WebSocket, topic: str) -> None:
        await websocket.accept()
        await broker.subscribe(topic, websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            await broker.unsubscribe(topic, websocket)

    @api.get("/v1/stats", response_model=BrokerStats)
    async def get_stats() -> BrokerStats:
        return await broker.stats()

    @api.get("/health")
    async def health() -> dict[str, str]:
        if not await broker.check_database():
            return JSONResponse(status_code=503, content={"status": "error", "database": "unavailable"})
        return {"status": "ok", "database": "ok"}

    return api


def _first_header(request: Request, *names: str) -> str | None:
    for name in names:
        value = request.headers.get(name)
        if value is not None:
            return value
    return None


app = create_app()

