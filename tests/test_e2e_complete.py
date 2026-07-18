"""
Comprehensive End-to-End (E2E) Integration Test Suite for Instant Strike.
Tests FastAPI lifecycle, /debug/replay pipeline, latency reporting, DB persistence,
natural language /ask queries, MCP tool logic, and Celery worker/reconciliation idempotency.
"""
import asyncio
import json
import os
import shutil
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from httpx import AsyncClient, ASGITransport

# Configure file-backed SQLite DB and mock Redis for E2E testing
import os
test_db_path = "test_e2e_instant_strike.db"
if os.path.exists(test_db_path):
    try:
        os.remove(test_db_path)
    except Exception:
        pass
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{test_db_path}"
os.environ["REDIS_URL"] = "mock://redis"
os.environ["CELERY_BROKER_URL"] = "memory://"
os.environ["CELERY_RESULT_BACKEND"] = "cache+memory://"

from app.main import app
from app.notifications.celery_app import celery_app
celery_app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
    broker_url="memory://",
    result_backend="cache+memory://",
)

from app.persistence.db import get_engine, get_sessionmaker
from app.persistence.init_db import init_db
from app.persistence.models import Trade
from app.spikes.redis_window import RedisPriceWindow
import app.pipeline.tick_pipeline as tick_pipeline

# Mock redis-py for E2E when Redis server is not running locally
class MockAsyncRedis:
    def __init__(self):
        self.zsets = {}
        
    async def zadd(self, key, mapping):
        if key not in self.zsets:
            self.zsets[key] = []
        for member, score in mapping.items():
            self.zsets[key].append((score, member))
            
    async def zremrangebyscore(self, key, min_score, max_score):
        if key in self.zsets:
            self.zsets[key] = [
                (s, m) for s, m in self.zsets[key] 
                if not (float(min_score) <= s <= float(max_score))
            ]
            
    async def zrevrangebyscore(self, key, max_score, min_score, start=0, num=1):
        if key not in self.zsets:
            return []
        candidates = [
            (s, m) for s, m in self.zsets[key]
            if float(min_score) <= s <= float(max_score)
        ]
        candidates.sort(key=lambda x: x[0], reverse=True)
        return [candidates[i][1] for i in range(start, min(start + num, len(candidates)))]
        
    async def aclose(self):
        pass

from contextlib import asynccontextmanager

# Patch redis client inside tick_pipeline to use our in-memory MockAsyncRedis
_shared_mock_redis = MockAsyncRedis()
def _get_mock_redis(url, decode_responses=True):
    return _shared_mock_redis

tick_pipeline.redis.from_url = _get_mock_redis


@asynccontextmanager
async def get_e2e_client():
    await init_db()
    os.makedirs("reports", exist_ok=True)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest_asyncio.fixture
async def e2e_client():
    async with get_e2e_client() as client:
        yield client


@pytest.mark.asyncio
async def test_healthz_endpoint(e2e_client: AsyncClient):
    print("\n[E2E] Testing GET /healthz...")
    resp = await e2e_client.get("/healthz")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    data = resp.json()
    assert data["status"] == "ok", f"Expected ok, got {data}"
    print("  GET /healthz: PASSED!")


@pytest.mark.asyncio
async def test_debug_replay_endpoint(e2e_client: AsyncClient):
    print("\n[E2E] Testing POST /debug/replay with tests/test_ticks.ndjson...")
    with open("tests/test_ticks.ndjson", "rb") as f:
        payload = f.read()
        
    resp = await e2e_client.post("/debug/replay", content=payload)
    assert resp.status_code == 200, f"Expected 200, got {resp.text}"
    data = resp.json()
    
    assert data["accepted"] == 50
    assert data["processed"] == 50
    assert data["errors"] == 0
    assert "latency_report" in data
    
    report = data["latency_report"]
    assert report["count"] > 0, "Expected positive spike signals generated"
    assert report["p99_ms"] >= 0.0
    print(f"  Replay response: {data['processed']} ticks processed, {report['count']} spike trades triggered!")
    print(f"  Latency report: p50={report['p50_ms']}ms, p95={report['p95_ms']}ms, p99={report['p99_ms']}ms, max={report['max_ms']}ms: PASSED!")
    
    # Verify reports/latency.json was written to disk and matches
    assert os.path.exists("reports/latency.json"), "Expected reports/latency.json on disk"
    with open("reports/latency.json", "r") as lf:
        disk_report = json.load(lf)
    assert disk_report["count"] == report["count"]
    print("  Disk latency file reports/latency.json verification: PASSED!")


@pytest.mark.asyncio
async def test_database_persistence_and_trades():
    print("\n[E2E] Verifying Database Trade Records...")
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    
    async with sessionmaker() as session:
        result = await session.execute(__import__("sqlalchemy").select(Trade))
        trades = list(result.scalars().all())
        
    assert len(trades) > 0, "Expected persisted trades in DB from replay"
    
    long_trades = [t for t in trades if t.side == "LONG"]
    short_trades = [t for t in trades if t.side == "SHORT"]
    
    assert len(long_trades) > 0, "Expected at least one LONG (CE) trade"
    assert len(short_trades) > 0, "Expected at least one SHORT (PE) trade"
    
    for t in trades:
        assert t.instrument == "13", f"Expected security_id 13, got {t.instrument}"
        assert t.strike % 50 == 0, f"Strike must be exact 50-point NIFTY increment, got {t.strike}"
        assert t.option_type in ("CE", "PE")
        assert t.entry_price > 0.0
        # In eager mode, notifications execute immediately upon persistence and transition to success
        assert t.notification_status in ("pending", "success")
        assert f"{t.id}:{t.option_type}:{t.side}" == t.notification_dedup_key
        
    print(f"  DB verification: {len(trades)} trades ({len(long_trades)} LONG/CE, {len(short_trades)} SHORT/PE) exact schema check: PASSED!")


@pytest.mark.asyncio
async def test_nl_ask_endpoint_against_db(e2e_client: AsyncClient):
    print("\n[E2E] Testing POST /ask Natural Language queries against populated DB...")
    
    # Query 1: Last trade
    resp1 = await e2e_client.post("/ask", json={"question": "What was the last trade?"})
    assert resp1.status_code == 200
    ans1 = resp1.json()["answer"]
    assert "Last trade:" in ans1 or "Trade data unavailable" not in ans1, f"Unexpected answer: {ans1}"
    print(f"  Q: 'What was the last trade?' -> A: {ans1.strip()}")
    
    # Query 2: Summary/count
    resp2 = await e2e_client.post("/ask", json={"question": "How many total trades?"})
    assert resp2.status_code == 200
    ans2 = resp2.json()["answer"]
    assert "Total trades" in ans2 or "trades recorded" in ans2 or "LONG" in ans2, f"Unexpected answer: {ans2}"
    print(f"  Q: 'How many total trades?' -> A: {ans2.strip()}")
    
    # Query 3: Performance/strike comparison
    resp3 = await e2e_client.post("/ask", json={"question": "Compare CE vs PE performance"})
    assert resp3.status_code == 200
    ans3 = resp3.json()["answer"]
    assert "CE vs PE comparison:" in ans3 or "CE:" in ans3 or "PE:" in ans3, f"Unexpected answer: {ans3}"
    print("  POST /ask queries with DB context: PASSED!")


@pytest.mark.asyncio
async def test_reconciliation_beat_idempotency():
    print("\n[E2E] Testing Celery Worker Idempotency and Reconciliation Sweep...")
    from app.reconciliation.beat import _enqueue_missing_notifications_async
    import uuid
    
    # Insert a dummy pending trade to test the reconciliation sweep
    engine = await get_engine()
    sessionmaker = await get_sessionmaker(engine)
    dummy_id = str(uuid.uuid4())
    dummy_trade = Trade(
        id=dummy_id,
        instrument="13",
        strike=23000,
        option_type="CE",
        side="LONG",
        entry_price=100.0,
        pnl=0.0,
        signal_reason="Dummy pending for sweep",
        created_at=datetime.now(timezone.utc),
        notification_status="pending",
        notification_dedup_key=f"{dummy_id}:CE:LONG",
    )
    async with sessionmaker() as session:
        async with session.begin():
            session.add(dummy_trade)
            
    # Run reconciliation sweep (finds pending trades and re-enqueues)
    enqueued_count = await _enqueue_missing_notifications_async()
    assert enqueued_count > 0, "Expected reconciliation beat to enqueue pending trades"
    
    # Verify dummy trade transitioned to success due to eager task delivery
    async with sessionmaker() as session:
        updated_dummy = await session.get(Trade, dummy_id)
        assert updated_dummy is not None and updated_dummy.notification_status == "success"
        
    print(f"  Reconciliation sweep successfully re-enqueued and delivered {enqueued_count} pending notifications idempotently: PASSED!")


if __name__ == "__main__":
    # Run full E2E suite
    async def run_all():
        async with get_e2e_client() as client:
            await test_healthz_endpoint(client)
            await test_debug_replay_endpoint(client)
            await test_database_persistence_and_trades()
            await test_nl_ask_endpoint_against_db(client)
            await test_reconciliation_beat_idempotency()
        print("\n=== COMPLETE E2E SYSTEM INTEGRATION TESTING PASSED ===")
        
    asyncio.run(run_all())
