import logging
import time

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.pipeline.tick_pipeline import process_tick_pipeline
from app.metrics.latency import flush_latency_report, reset_latency_metrics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/debug", tags=["debug"])


class DebugReplayTick(BaseModel):
    security_id: str
    ltp: float
    ts: str


@router.post("/replay")
async def debug_replay(request: Request):
    """
    Accepts newline-delimited JSON (NDJSON) ticks:
    {"security_id":"13","ltp":22450.5,"ts":"2026-07-10T09:31:04.221Z"}

    After processing all ticks through the shared pipeline, flushes the
    latency report and returns it along with the tick count.
    """
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty body")

    # One JSON object per line
    lines = body.decode("utf-8").splitlines()
    ticks = []
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        try:
            ticks.append(DebugReplayTick.model_validate_json(line))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON on line {i + 1}: {e}")

    if not ticks:
        raise HTTPException(status_code=400, detail="No valid ticks found")

    # Reset latency metrics for this replay run
    reset_latency_metrics()

    replay_start = time.monotonic()

    # Push through the exact same pipeline as live WS.
    processed = 0
    errors = 0
    for t in ticks:
        try:
            await process_tick_pipeline(
                security_id=t.security_id,
                ltp=t.ltp,
                ts=t.ts,
                source="replay",
            )
            processed += 1
        except Exception as e:
            logger.error("Error processing tick %d: %s", processed + 1, e)
            errors += 1

    replay_elapsed_ms = (time.monotonic() - replay_start) * 1000

    # Flush latency report after replay completes (required deliverable)
    latency_report = flush_latency_report()

    return {
        "accepted": len(ticks),
        "processed": processed,
        "errors": errors,
        "replay_elapsed_ms": round(replay_elapsed_ms, 2),
        "latency_report": latency_report,
    }
