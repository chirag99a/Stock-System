"""
DhanHQ WebSocket ingestion consumer.

Connects to the DhanHQ Market Feed WebSocket and pushes each tick
through the shared process_tick_pipeline, ensuring the same code path
as /debug/replay.

Features:
- Exponential backoff reconnection (must not silently stop)
- Health monitoring via last_tick_at timestamp
- Graceful shutdown
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Configuration
WS_URL = os.getenv("DHAN_WS_URL", "wss://api-feed.dhan.co/ws")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "")
RECONNECT_BASE_DELAY = float(os.getenv("WS_RECONNECT_BASE_DELAY", "1.0"))
RECONNECT_MAX_DELAY = float(os.getenv("WS_RECONNECT_MAX_DELAY", "60.0"))
HEALTH_STALE_SECONDS = int(os.getenv("WS_HEALTH_STALE_SECONDS", "30"))

# Monitored state
_last_tick_at: Optional[float] = None
_is_running = False
_consecutive_failures = 0


def is_healthy() -> bool:
    """Check if the WS consumer is receiving ticks recently."""
    if not _is_running:
        return False
    if _last_tick_at is None:
        return True  # Just started, no ticks expected yet
    return (time.monotonic() - _last_tick_at) < HEALTH_STALE_SECONDS


async def start_ws_consumer():
    """
    Main entry point for the WebSocket consumer.
    Runs indefinitely with automatic reconnection.
    Must not silently stop — logs all disconnections and retries.
    """
    global _is_running, _consecutive_failures

    if not DHAN_ACCESS_TOKEN:
        logger.warning(
            "DHAN_ACCESS_TOKEN not set — WebSocket ingestion disabled. "
            "Use /debug/replay for tick processing."
        )
        return

    _is_running = True
    logger.info("Starting DhanHQ WebSocket consumer")

    while _is_running:
        try:
            await _connect_and_consume()
            _consecutive_failures = 0
        except asyncio.CancelledError:
            logger.info("WebSocket consumer cancelled")
            break
        except Exception as e:
            _consecutive_failures += 1
            delay = min(
                RECONNECT_BASE_DELAY * (2 ** _consecutive_failures),
                RECONNECT_MAX_DELAY,
            )
            logger.error(
                "WebSocket disconnected (failure #%d): %s. Reconnecting in %.1fs...",
                _consecutive_failures, e, delay,
            )
            await asyncio.sleep(delay)

    _is_running = False
    logger.info("WebSocket consumer stopped")


async def stop_ws_consumer():
    """Signal the consumer to stop."""
    global _is_running
    _is_running = False
    logger.info("WebSocket consumer stop requested")


async def _connect_and_consume():
    """
    Single connection lifecycle:
    1. Connect to DhanHQ WebSocket
    2. Subscribe to instruments
    3. Process incoming tick messages through shared pipeline
    """
    import websockets

    global _last_tick_at

    logger.info("Connecting to DhanHQ WebSocket: %s", WS_URL)

    async with websockets.connect(
        WS_URL,
        extra_headers={"Authorization": f"Bearer {DHAN_ACCESS_TOKEN}"},
        ping_interval=20,
        ping_timeout=10,
    ) as ws:
        logger.info("WebSocket connected successfully")
        _last_tick_at = time.monotonic()

        # Subscribe to NIFTY index feed (security_id: 13 for NIFTY 50)
        subscribe_msg = json.dumps({
            "type": "subscribe",
            "instruments": [{"security_id": "13", "exchange_segment": "IDX_I"}],
        })
        await ws.send(subscribe_msg)
        logger.info("Subscribed to instruments")

        # Consume messages
        async for raw_message in ws:
            if not _is_running:
                break

            try:
                tick = _parse_ws_message(raw_message)
                if tick is None:
                    continue

                _last_tick_at = time.monotonic()

                from app.pipeline.tick_pipeline import process_tick_pipeline
                await process_tick_pipeline(
                    security_id=tick["security_id"],
                    ltp=tick["ltp"],
                    ts=tick["ts"],
                    source="websocket",
                )
            except Exception as e:
                logger.error("Error processing WebSocket tick: %s (raw: %s)", e, raw_message[:200] if isinstance(raw_message, str) else "<binary>")


def _parse_ws_message(raw: str | bytes) -> Optional[dict]:
    """
    Parse a DhanHQ WebSocket message into a tick dict.

    Expected format (simplified for NIFTY index):
    {
        "type": "ticker",
        "security_id": "13",
        "ltp": 22450.5,
        "exchange_timestamp": "2026-07-10T09:31:04.221Z"
    }

    Returns None for non-tick messages (heartbeats, acks, etc).
    """
    if isinstance(raw, bytes):
        # Binary frame — DhanHQ uses binary protocol for market data
        # In production, this would decode the binary protocol
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            logger.debug("Binary WebSocket frame received (needs binary decoder)")
            return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.debug("Non-JSON WebSocket frame: %s", raw[:100])
        return None

    msg_type = data.get("type", "")

    # Skip non-tick messages
    if msg_type in ("connected", "subscribed", "heartbeat", "ack"):
        return None

    security_id = data.get("security_id")
    ltp = data.get("ltp")

    if security_id is None or ltp is None:
        return None

    # Use exchange timestamp if available, otherwise now
    ts = data.get("exchange_timestamp") or data.get("ts")
    if not ts:
        ts = datetime.now(timezone.utc).isoformat()

    return {
        "security_id": str(security_id),
        "ltp": float(ltp),
        "ts": ts,
    }
