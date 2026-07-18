import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.persistence.init_db import init_db
from app.routes.debug_replay import router as debug_replay_router
from app.routes.ask import router as ask_router

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler (replaces deprecated @app.on_event).
    Startup: init DB tables, create reports dir, optionally start WS consumer.
    Shutdown: stop WS consumer gracefully.
    """
    # --- Startup ---
    logger.info("Instant Strike Execution Engine starting up...")

    # Create reports directory for latency output
    os.makedirs("reports", exist_ok=True)

    # Initialize database tables
    await init_db()
    logger.info("Database initialized")

    # Start WebSocket consumer if configured
    ws_task = None
    dhan_token = os.getenv("DHAN_ACCESS_TOKEN", "")
    if dhan_token:
        from app.pipeline.ws_ingestion import start_ws_consumer
        ws_task = asyncio.create_task(start_ws_consumer())
        logger.info("WebSocket consumer task started")
    else:
        logger.info("DHAN_ACCESS_TOKEN not set — WebSocket ingestion disabled (use /debug/replay)")

    yield

    # --- Shutdown ---
    logger.info("Shutting down...")

    if ws_task is not None:
        from app.pipeline.ws_ingestion import stop_ws_consumer
        await stop_ws_consumer()
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
        logger.info("WebSocket consumer stopped")


app = FastAPI(
    title="Instant Strike Execution Engine",
    version="1.0.0",
    description="Real-time NIFTY spike detection and simulated options execution engine.",
    lifespan=lifespan,
)

app.include_router(debug_replay_router)
app.include_router(ask_router)


@app.get("/healthz", tags=["system"])
async def healthz():
    """Health check endpoint."""
    health = {"status": "ok"}

    # Include WS consumer health if applicable
    try:
        from app.pipeline.ws_ingestion import is_healthy, _is_running
        if _is_running:
            health["ws_consumer"] = "healthy" if is_healthy() else "stale"
    except Exception:
        pass

    return health
