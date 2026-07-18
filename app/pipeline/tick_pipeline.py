import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.persistence.db import get_engine, get_sessionmaker
from app.metrics.latency import record_tick_to_signal_latency
from app.quant.option_chain import select_atm_strike, build_option_instrument
from app.quant.premium_source import get_option_premium
from app.spikes.redis_window import RedisPriceWindow
from app.persistence.models import Trade

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TickDecision:
    side: str  # "LONG" or "SHORT"
    strike: int
    option_type: str  # "CE" or "PE"
    entry_price: float
    signal_reason: str


def _parse_ts(ts: str) -> datetime:
    # Expected ISO-8601 Z format
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def process_tick_pipeline(security_id: str, ltp: float, ts: str, source: str) -> None:
    """
    Single shared pipeline for both live WS and /debug/replay.
    Measures tick-to-signal latency up to the point of spike decision (excluded Postgres/Celery).
    """
    # Monotonic start — captured at ingestion entry point.
    start_ns = time.perf_counter_ns()

    tick_dt = _parse_ts(ts)

    # Redis rolling 60s window for the specific instrument.
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    try:
        window = RedisPriceWindow(redis_client=r, key_prefix="instant-strike:prices", window_seconds=60)
        await window.append_tick(security_id=str(security_id), ts=tick_dt, ltp=float(ltp))

        p_now = float(ltp)
        p_60 = await window.fetch_price_at_or_before_shift(security_id=str(security_id), ts=tick_dt, shift_seconds=60)

        if p_60 is None or p_60 <= 0:
            return

        # Spike detection
        pct = (p_now - p_60) / p_60
        if pct >= 0.05:
            side = "LONG"
            option_type = "CE"
            signal_reason = f"+{pct * 100:.2f}% Spike"
        elif pct <= -0.05:
            side = "SHORT"
            option_type = "PE"
            signal_reason = f"{pct * 100:.2f}% Spike"
        else:
            return

        # Measure tick-to-signal latency (spike detector emitted decision here)
        await record_tick_to_signal_latency(start_ns=start_ns)

        # Quant: ATM strike and premium
        atm_spot = float(p_now)
        strike = select_atm_strike(atm_spot=atm_spot)

        instrument = build_option_instrument(security_id=str(security_id), strike=strike, option_type=option_type)

        # Fetch/mocked premium
        entry_price = float(await get_option_premium(instrument=instrument, side=side, ts=tick_dt))

        trade_id = str(uuid.uuid4())
        trade = Trade(
            id=trade_id,
            instrument=str(security_id),
            strike=int(strike),
            option_type=str(option_type),
            side=str(side),
            entry_price=float(entry_price),
            pnl=0.0,
            signal_reason=str(signal_reason),
            created_at=tick_dt,
            notification_status="pending",
            notification_dedup_key=f"{trade_id}:{option_type}:{side}",
        )

        # Persist trade (async transaction safety inside db layer)
        engine = await get_engine()
        sessionmaker: async_sessionmaker = await get_sessionmaker(engine)

        async with sessionmaker() as session:
            async with session.begin():
                session.add(trade)

        logger.info(
            "Trade persisted: id=%s side=%s strike=%d option=%s reason=%s",
            trade_id, side, strike, option_type, signal_reason,
        )

        # Enqueue notification async (after successful commit).
        # Import lazily to avoid startup errors before celery module exists.
        try:
            from app.notifications.tasks import enqueue_notification_for_trade

            await enqueue_notification_for_trade(
                trade_id=trade_id,
                dedup_key=trade.notification_dedup_key,
                message_template=trade,
            )
        except Exception as e:
            # Celery broker might be unreachable; trade is persisted with pending status.
            # Reconciliation beat will pick it up later.
            logger.warning("Failed to enqueue notification for trade %s: %s", trade_id, e)

    finally:
        await r.aclose()


async def parse_debug_tick_file_ndjson(payload: bytes) -> list[dict]:
    """
    Utility for future endpoints/tests.
    """
    ticks = []
    if not payload:
        return ticks
    lines = payload.decode("utf-8").splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        ticks.append(json.loads(line))
    return ticks
