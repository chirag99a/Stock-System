import asyncio
import os
from datetime import datetime

from app.notifications.celery_app import celery_app
from app.persistence.db import get_engine, get_sessionmaker
from app.persistence.models import Trade


async def _enqueue_missing_notifications_async() -> int:
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)

    missing = []
    async with sessionmaker() as session:
        async with session.begin():
            # pending/failed notifications are those not yet success
            # (we keep only trades that are not success)
            result = await session.execute(
                # SQLAlchemy 2.0: use ORM select
                __import__("sqlalchemy").select(Trade).where(Trade.notification_status != "success").limit(500)
            )
            missing = list(result.scalars().all())

    enqueued = 0
    from app.notifications.tasks import enqueue_notification_for_trade

    for trade in missing:
        await enqueue_notification_for_trade(
            trade_id=trade.id,
            dedup_key=trade.notification_dedup_key,
            message_template=trade,
        )
        enqueued += 1

    return enqueued


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    interval_seconds = int(os.getenv("RECONCILIATION_INTERVAL_SECONDS", "60"))
    sender.add_periodic_task(
        interval_seconds,
        reconcile_notifications.s(),
        name="reconcile-notifications-every-60s",
    )


@celery_app.task(name="app.reconciliation.reconcile_notifications", bind=True)
def reconcile_notifications(self):
    """
    Every 60 seconds:
    - find trades whose notifications are not success
    - enqueue notifications again (idempotent on worker side)
    """
    return asyncio.run(_enqueue_missing_notifications_async())
