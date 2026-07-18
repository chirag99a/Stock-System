import io
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import sqlalchemy as sa
from fastmcp import FastMCP

from app.persistence.db import get_engine, get_sessionmaker
from app.persistence.models import Trade

logger = logging.getLogger(__name__)

mcp = FastMCP("instant-strike-mcp")


@mcp.tool()
async def get_last_trade() -> Dict[str, Any]:
    """Get the most recent trade from the database."""
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        q = await session.execute(
            sa.select(Trade).order_by(Trade.created_at.desc()).limit(1)
        )
        trade = q.scalars().first()
        if not trade:
            return {"message": "No trades found"}
        return _trade_to_dict(trade)


@mcp.tool()
async def get_open_positions() -> List[Dict[str, Any]]:
    """
    Get open positions — latest trade per instrument showing current side.
    In this engine, each spike creates a new simulated trade.
    Positions are inferred from the most recent trade per instrument.
    """
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        # Get the latest trade per instrument using a subquery
        subq = (
            sa.select(
                Trade.instrument,
                sa.func.max(Trade.created_at).label("latest"),
            )
            .group_by(Trade.instrument)
            .subquery()
        )

        q = await session.execute(
            sa.select(Trade)
            .join(
                subq,
                sa.and_(
                    Trade.instrument == subq.c.instrument,
                    Trade.created_at == subq.c.latest,
                ),
            )
            .order_by(Trade.created_at.desc())
        )
        trades = list(q.scalars().all())

        return [
            {
                "instrument": t.instrument,
                "side": t.side,
                "strike": t.strike,
                "option_type": t.option_type,
                "entry_price": t.entry_price,
                "pnl": t.pnl,
                "opened_at": t.created_at.isoformat() if t.created_at else None,
            }
            for t in trades
        ]


@mcp.tool()
async def get_pnl_summary() -> Dict[str, Any]:
    """
    Compute aggregate PnL summary from all trades.
    Breaks down by side (LONG/SHORT) and option type (CE/PE).
    """
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        # Overall stats
        result = await session.execute(
            sa.select(
                sa.func.count(Trade.id).label("total_trades"),
                sa.func.coalesce(sa.func.sum(Trade.pnl), 0.0).label("total_pnl"),
                sa.func.coalesce(sa.func.avg(Trade.pnl), 0.0).label("avg_pnl"),
            )
        )
        row = result.one()

        # Breakdown by side
        side_result = await session.execute(
            sa.select(
                Trade.side,
                sa.func.count(Trade.id).label("count"),
                sa.func.coalesce(sa.func.sum(Trade.pnl), 0.0).label("pnl"),
            )
            .group_by(Trade.side)
        )
        side_breakdown = {r.side: {"count": r.count, "pnl": float(r.pnl)} for r in side_result}

        # Breakdown by option type
        option_result = await session.execute(
            sa.select(
                Trade.option_type,
                sa.func.count(Trade.id).label("count"),
                sa.func.coalesce(sa.func.sum(Trade.pnl), 0.0).label("pnl"),
            )
            .group_by(Trade.option_type)
        )
        option_breakdown = {r.option_type: {"count": r.count, "pnl": float(r.pnl)} for r in option_result}

        return {
            "total_trades": row.total_trades,
            "total_pnl": float(row.total_pnl),
            "avg_pnl": float(row.avg_pnl),
            "by_side": side_breakdown,
            "by_option_type": option_breakdown,
        }


@mcp.tool()
async def get_spike_events(limit: int = 100) -> List[Dict[str, Any]]:
    """Get recent spike events (trades triggered by spike detection)."""
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        q = await session.execute(
            sa.select(Trade).order_by(Trade.created_at.desc()).limit(limit)
        )
        trades = list(q.scalars().all())
        return [_trade_to_dict(t) for t in trades]


@mcp.tool()
async def get_best_strike_accuracy() -> Dict[str, Any]:
    """
    Compute which strike had the most trades and highest success rate.
    In this engine, accuracy is approximated by the PnL distribution per strike.
    """
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        result = await session.execute(
            sa.select(
                Trade.strike,
                sa.func.count(Trade.id).label("trade_count"),
                sa.func.coalesce(sa.func.sum(Trade.pnl), 0.0).label("total_pnl"),
                sa.func.coalesce(sa.func.avg(Trade.pnl), 0.0).label("avg_pnl"),
            )
            .group_by(Trade.strike)
            .order_by(sa.func.count(Trade.id).desc())
        )
        rows = result.all()
        if not rows:
            return {"best_strike": None, "accuracy": None, "strikes": []}

        strikes = [
            {
                "strike": r.strike,
                "trade_count": r.trade_count,
                "total_pnl": float(r.total_pnl),
                "avg_pnl": float(r.avg_pnl),
            }
            for r in rows
        ]

        best = strikes[0]
        return {
            "best_strike": best["strike"],
            "trade_count": best["trade_count"],
            "avg_pnl": best["avg_pnl"],
            "strikes": strikes,
        }


@mcp.tool()
async def generate_trade_chart() -> Dict[str, Any]:
    """
    Generate a trade summary chart using matplotlib.
    Saves to reports/trade_chart.png and returns the path.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        return {"chart": None, "error": "matplotlib not available"}

    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        q = await session.execute(
            sa.select(Trade).order_by(Trade.created_at.asc()).limit(500)
        )
        trades = list(q.scalars().all())

    if not trades:
        return {"chart": None, "message": "No trades to chart"}

    # Create the chart
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), tight_layout=True)

    # Chart 1: Trade timeline (entry prices over time)
    times = [t.created_at for t in trades]
    prices = [t.entry_price for t in trades]
    sides = [t.side for t in trades]
    colors = ["#2ecc71" if s == "LONG" else "#e74c3c" for s in sides]

    axes[0].scatter(times, prices, c=colors, alpha=0.7, s=30)
    axes[0].set_title("Trade Entry Prices Over Time", fontweight="bold")
    axes[0].set_xlabel("Time")
    axes[0].set_ylabel("Entry Price")
    axes[0].grid(True, alpha=0.3)
    if times:
        axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    # Chart 2: Strike distribution
    strike_counts = {}
    for t in trades:
        key = f"{t.strike} {t.option_type}"
        strike_counts[key] = strike_counts.get(key, 0) + 1

    labels = list(strike_counts.keys())
    counts = list(strike_counts.values())
    bar_colors = ["#3498db" if "CE" in l else "#e67e22" for l in labels]

    axes[1].barh(labels, counts, color=bar_colors, alpha=0.8)
    axes[1].set_title("Trades by Strike & Option Type", fontweight="bold")
    axes[1].set_xlabel("Number of Trades")
    axes[1].grid(True, alpha=0.3, axis="x")

    chart_path = "reports/trade_chart.png"
    os.makedirs(os.path.dirname(chart_path), exist_ok=True)
    fig.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    logger.info("Trade chart saved to %s", chart_path)
    return {"chart": chart_path, "trade_count": len(trades)}


def _trade_to_dict(trade: Trade) -> Dict[str, Any]:
    """Convert a Trade ORM instance to a dictionary."""
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
        "notification_status": trade.notification_status,
    }


def run_mcp():
    """
    Entry point for starting MCP server. In docker-compose we can run this separately if needed.
    """
    import uvicorn
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8001"))

    # FastMCP provides its own ASGI app; FastAPI/uvicorn for simplicity.
    uvicorn.run(mcp.app, host=host, port=port)
