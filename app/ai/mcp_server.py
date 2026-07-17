import os
from typing import Any, Dict, List

from fastmcp import FastMCP

from app.persistence.db import get_engine, get_sessionmaker
from app.persistence.models import Trade

mcp = FastMCP("instant-strike-mcp")


@mcp.tool()
async def get_last_trade() -> Dict[str, Any]:
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        # Simple ORM ordering
        q = await session.execute(
            __import__("sqlalchemy").select(Trade).order_by(Trade.created_at.desc()).limit(1)
        )
        trade = q.scalars().first()
        if not trade:
            return {}
        return {
            "id": trade.id,
            "instrument": trade.instrument,
            "strike": trade.strike,
            "option_type": trade.option_type,
            "side": trade.side,
            "entry_price": trade.entry_price,
            "pnl": trade.pnl,
            "signal_reason": trade.signal_reason,
            "created_at": trade.created_at.isoformat() if trade.created_at else None,
        }


@mcp.tool()
async def get_open_positions() -> List[Dict[str, Any]]:
    # Scaffold: positions not computed in this evaluation scaffold.
    return []


@mcp.tool()
async def get_pnl_summary() -> Dict[str, Any]:
    # Scaffold: PnL not computed in this scaffold.
    return {"total_pnl": 0.0}


@mcp.tool()
async def get_spike_events() -> List[Dict[str, Any]]:
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        q = await session.execute(
            __import__("sqlalchemy").select(Trade).order_by(Trade.created_at.desc()).limit(100)
        )
        trades = list(q.scalars().all())
        return [
            {
                "id": t.id,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "side": t.side,
                "strike": t.strike,
                "option_type": t.option_type,
                "signal_reason": t.signal_reason,
            }
            for t in trades
        ]


@mcp.tool()
async def get_best_strike_accuracy() -> Dict[str, Any]:
    # Scaffold metric: not enough data to compute.
    return {"best_strike": None, "accuracy": None}


@mcp.tool()
async def generate_trade_chart() -> Dict[str, Any]:
    # Scaffold: chart generation later
    return {"chart": None}


def run_mcp():
    """
    Entry point for starting MCP server. In docker-compose we can run this separately if needed.
    """
    import uvicorn
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8001"))

    # FastMCP provides its own ASGI app; FastAPI/uvicorn for simplicity.
    uvicorn.run(mcp.app, host=host, port=port)
