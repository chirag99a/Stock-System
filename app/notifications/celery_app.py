import os

from celery import Celery

# Broker & backend
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/1")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/2")

celery_app = Celery(
    "instant_strike",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Late ack improves redelivery semantics on crashes.
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

# Ensure Celery Beat schedules are registered.
# This import wires `@celery_app.on_after_configure` handlers.
try:
    import app.reconciliation.beat  # noqa: F401
except Exception:
    # Don't crash app startup if beat wiring fails during partial environments.
    pass
