import logging
import os
from typing import Any, Dict

import sqlalchemy as sa

from app.persistence.db import get_engine, get_sessionmaker
from app.persistence.models import Trade

logger = logging.getLogger(__name__)


async def handle_ask(question: str) -> Dict[str, Any]:
    """
    Natural language endpoint handler for POST /ask.
    Uses OpenAI if OPENAI_API_KEY is set; otherwise falls back to
    a DB-backed rules-based responder that returns actual trade data.
    """
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if not openai_key:
        return await _fallback_answer(question)

    # Fetch context from DB for the LLM
    context = await _build_context()

    from openai import OpenAI
    client = OpenAI(api_key=openai_key)

    prompt = (
        "You are an options execution analytics assistant for the Instant Strike engine.\n"
        "Answer concisely based on the following trade data:\n\n"
        f"{context}\n\n"
        f"User question: {question}\n"
    )

    resp = await _openai_chat(client, prompt=prompt)
    return {"answer": resp}


async def _build_context() -> str:
    """Build a context string from recent trades for the LLM."""
    try:
        engine = await get_engine()
        sessionmaker = await get_sessionmaker(engine)
        async with sessionmaker() as session:
            result = await session.execute(
                sa.select(Trade).order_by(Trade.created_at.desc()).limit(20)
            )
            trades = list(result.scalars().all())

        if not trades:
            return "No trades recorded yet."

        # Aggregate stats
        total = len(trades)
        total_pnl = sum(t.pnl for t in trades)
        long_count = sum(1 for t in trades if t.side == "LONG")
        short_count = sum(1 for t in trades if t.side == "SHORT")
        ce_count = sum(1 for t in trades if t.option_type == "CE")
        pe_count = sum(1 for t in trades if t.option_type == "PE")

        lines = [
            f"Total recent trades: {total}",
            f"LONG: {long_count}, SHORT: {short_count}",
            f"CE: {ce_count}, PE: {pe_count}",
            f"Total PnL: {total_pnl:.2f}",
            "",
            "Last 5 trades:",
        ]
        for t in trades[:5]:
            lines.append(
                f"  - {t.side} NIFTY {t.strike} {t.option_type} @ {t.entry_price:.2f} "
                f"PnL={t.pnl:.2f} ({t.signal_reason}) at {t.created_at}"
            )

        return "\n".join(lines)
    except Exception as e:
        logger.warning("Failed to build context: %s", e)
        return "Trade data unavailable."


async def _openai_chat(client: Any, prompt: str) -> str:
    # openai python client is sync; run in thread to avoid blocking FastAPI
    import asyncio

    def _call():
        return client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": "You are a concise options trading analytics assistant. Give short, data-driven answers."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )

    completion = await asyncio.to_thread(_call)
    return completion.choices[0].message.content or ""


async def _fallback_answer(question: str) -> Dict[str, Any]:
    """
    DB-backed fallback responder when no LLM key is configured.
    Queries actual trade data to answer common questions.
    """
    q = question.lower()

    try:
        engine = await get_engine()
        sessionmaker = await get_sessionmaker(engine)

        async with sessionmaker() as session:
            if "last trade" in q or "latest trade" in q or "recent trade" in q:
                result = await session.execute(
                    sa.select(Trade).order_by(Trade.created_at.desc()).limit(1)
                )
                trade = result.scalars().first()
                if not trade:
                    return {"answer": "No trades have been recorded yet."}
                return {
                    "answer": (
                        f"Last trade: {trade.side} NIFTY {trade.strike} {trade.option_type} "
                        f"entered at {trade.entry_price:.2f}. "
                        f"Reason: {trade.signal_reason}. "
                        f"Time: {trade.created_at.isoformat() if trade.created_at else 'N/A'}."
                    )
                }

            if "losing" in q or "loss" in q:
                result = await session.execute(
                    sa.select(Trade)
                    .where(Trade.pnl < 0)
                    .order_by(Trade.pnl.asc())
                    .limit(5)
                )
                trades = list(result.scalars().all())
                if not trades:
                    return {"answer": "No losing trades found (all PnL >= 0)."}
                lines = [f"Top {len(trades)} losing trades:"]
                for t in trades:
                    lines.append(
                        f"  - {t.side} NIFTY {t.strike} {t.option_type}: PnL={t.pnl:.2f} ({t.signal_reason})"
                    )
                return {"answer": "\n".join(lines)}

            if "best strike" in q or "strike performed" in q or "top strike" in q:
                result = await session.execute(
                    sa.select(
                        Trade.strike,
                        sa.func.count(Trade.id).label("cnt"),
                        sa.func.coalesce(sa.func.sum(Trade.pnl), 0.0).label("total_pnl"),
                    )
                    .group_by(Trade.strike)
                    .order_by(sa.func.count(Trade.id).desc())
                    .limit(5)
                )
                rows = result.all()
                if not rows:
                    return {"answer": "No trade data available for strike analysis."}
                lines = ["Strike performance (by trade count):"]
                for r in rows:
                    lines.append(f"  - Strike {r.strike}: {r.cnt} trades, Total PnL: {float(r.total_pnl):.2f}")
                return {"answer": "\n".join(lines)}

            if ("ce" in q and "pe" in q) or "option type" in q:
                result = await session.execute(
                    sa.select(
                        Trade.option_type,
                        sa.func.count(Trade.id).label("cnt"),
                        sa.func.coalesce(sa.func.sum(Trade.pnl), 0.0).label("total_pnl"),
                    )
                    .group_by(Trade.option_type)
                )
                rows = result.all()
                if not rows:
                    return {"answer": "No trade data available for CE vs PE analysis."}
                lines = ["CE vs PE comparison:"]
                for r in rows:
                    lines.append(f"  - {r.option_type}: {r.cnt} trades, Total PnL: {float(r.total_pnl):.2f}")
                return {"answer": "\n".join(lines)}

            if "how many" in q or "count" in q or "total" in q:
                result = await session.execute(
                    sa.select(sa.func.count(Trade.id))
                )
                count = result.scalar() or 0
                return {"answer": f"Total trades recorded: {count}"}

            # Generic: return a summary
            result = await session.execute(
                sa.select(
                    sa.func.count(Trade.id).label("total"),
                    sa.func.coalesce(sa.func.sum(Trade.pnl), 0.0).label("pnl"),
                )
            )
            row = result.one()
            return {
                "answer": (
                    f"Instant Strike engine summary: {row.total} trades recorded, "
                    f"total PnL: {float(row.pnl):.2f}. "
                    f"Ask about specific trades, strikes, or CE/PE performance for details."
                )
            }

    except Exception as e:
        logger.error("Fallback answer DB error: %s", e)
        return {"answer": f"Unable to query trade data: {e}. LLM not configured; set OPENAI_API_KEY for full NL support."}
