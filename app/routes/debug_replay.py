from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.pipeline.tick_pipeline import process_tick_pipeline  # will be added next

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

    # Push through the exact same pipeline as live WS.
    for t in ticks:
        await process_tick_pipeline(
            security_id=t.security_id,
            ltp=t.ltp,
            ts=t.ts,
            source="replay",
        )

    return {"accepted": len(ticks)}
