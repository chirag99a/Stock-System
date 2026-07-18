import asyncio
import logging
import os
from typing import Any

from app.persistence.db import get_engine, get_sessionmaker
from app.persistence.models import Trade

logger = logging.getLogger(__name__)


async def _mark_notification_sent(trade_id: str) -> bool:
    """
    Idempotency gate:
    - returns True if we transitioned notification_status -> success
    - returns False if trade not found or already success
    """
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    async with sessionmaker() as session:
        async with session.begin():
            trade = await session.get(Trade, trade_id)
            if not trade:
                return False
            if trade.notification_status == "success":
                return False
            trade.notification_status = "success"
    return True


def _send_notification_http(message: dict) -> None:
    """
    Mockable notification sender.

    Delivery semantics:
    - In this scaffold we POST to a configurable webhook URL if provided.
    - If webhook is not configured, we treat it as a successful no-op delivery.
    """
    import requests

    webhook = os.getenv("NOTIFICATION_WEBHOOK_URL", "")
    if not webhook:
        logger.info("Notification delivered (no webhook configured): %s", message)
        return

    r = requests.post(webhook, json=message, timeout=5)
    r.raise_for_status()


def _format_message(message: dict) -> dict:
    return {"message": message.get("message") or message}


async def _send_notification_idempotent(trade_id: str, dedup_key: str, message: dict) -> None:
    # Gate: mark notification success only once.
    can_mark = await _mark_notification_sent(trade_id)
    if not can_mark:
        logger.debug("Notification already sent for trade %s (dedup: %s)", trade_id, dedup_key)
        return
    _send_notification_http(message)


async def enqueue_notification_for_trade(trade_id: str, dedup_key: str, message_template: Any) -> None:
    """
    Enqueue Celery notification after DB commit.
    """
    from app.notifications.celery_app import celery_app

    payload = {
        "trade_id": trade_id,
        "dedup_key": dedup_key,
        "message": None,
    }

    if isinstance(message_template, Trade):
        payload["message"] = {
            "message": f"Trade Alert! {message_template.side} NIFTY {message_template.strike} {message_template.option_type} entered. Reason: {message_template.signal_reason}",
        }
    else:
        payload["message"] = {"message": str(message_template)}

    if celery_app.conf.task_always_eager:
        send_trade_notification.delay(**payload)
    else:
        celery_app.send_task(
            "app.notifications.tasks.send_trade_notification",
            kwargs=payload,
        )


def _run_async(coro):
    """
    Run an async coroutine from a sync Celery worker context.
    Safely handles both dedicated Celery worker threads (no running event loop)
    and eager in-memory execution during async tests/endpoints where a loop is already running.
    """
    try:
        asyncio.get_running_loop()
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        return asyncio.run(coro)


from app.notifications.celery_app import celery_app  # noqa: E402


@celery_app.task(
    name="app.notifications.tasks.send_trade_notification",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    max_retries=int(os.getenv("NOTIFICATION_MAX_RETRIES", "5")),
)
def send_trade_notification(self, trade_id: str, dedup_key: str, message: dict) -> None:
    """
    Worker-side delivery:
    - uses DB transition as idempotency guard
    - retries on transient failures with exponential backoff
    """
    logger.info("Processing notification for trade %s", trade_id)
    _run_async(_send_notification_idempotent(trade_id=trade_id, dedup_key=dedup_key, message=message))
