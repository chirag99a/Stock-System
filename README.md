# Instant Strike — Execution Engine (Scaffold)

## What this repo implements
- FastAPI server
  - `POST /debug/replay` accepts NDJSON ticks and runs them through the **exact same pipeline** as live ticks (currently the WS consumer will be added next; replay uses the shared pipeline code).
  - `POST /ask` natural-language endpoint with an OpenAI-backed handler and a safe fallback if no key is present.
- Spike detection + simulated option trades
  - Maintains a rolling **60-second** Redis window per `security_id`.
  - Triggers Long/Short when the % move vs ~60s ago crosses ±5%.
  - Computes ATM strike (documented decision) and mocked/deterministic option premium.
  - Persists trades to Postgres using **async SQLAlchemy**.
- Notifications (Celery)
  - After trade persistence, enqueues a Celery notification task.
  - Task is idempotent by using `Trade.notification_status` as a gate.
  - Celery Beat reconciliation runs every 60 seconds to enqueue any trades whose notifications never succeeded.
- Latency measurement
  - Measures tick-to-signal latency inside the shared pipeline up to the moment the spike detector emits a decision.
  - Writes aggregate p50/p95/p99/max into `reports/latency.json` (wire-up for replay/reporting is next).

> Important: This is a scaffold to satisfy the assignment’s architecture requirements. Some sections (WS ingestion lifecycle hardening, latency report flushing after replay, Alembic migrations, chart generation, positions/PnL) are not fully implemented yet.

---

## Architecture decisions (tradeoffs)

### 1) Redis window structure choice
I used a **Sorted Set (ZSET)** per instrument (`instant-strike:prices:<security_id>`), where:
- `score = unix_ms`
- `member = "<unix_ms>|<ltp>"`

On each tick:
- append with `ZADD`
- expire older entries with `ZREMRANGEBYSCORE`
- fetch approximately `P(t-60)` using `ZREVRANGEBYSCORE(key, target_ms, "-inf", num=1)`

**Tradeoff accepted:** ZSET provides easy range/expiry semantics and fast approximate historical lookup with minimal logic. It’s more CPU than a pure list, but dramatically simpler to keep “~t-60s” semantics correct under replay noise.

### 2) Broker & result backend choice (Celery)
Celery uses **Redis** as both:
- broker: `CELERY_BROKER_URL=redis://...:1`
- result backend: `CELERY_RESULT_BACKEND=redis://...:2`

**Tradeoff accepted:** Redis is convenient and matches the stack, but RabbitMQ would typically offer stronger messaging semantics under certain failure modes. This scaffold favors simplicity and local replay evaluability.

### 3) Index choice in Postgres
`Trade` table indexes exist on:
- `instrument`, `strike`, `option_type`, `side`, `created_at`, `notification_status`, and a uniqueness constraint on `notification_dedup_key`.

**Tradeoff accepted:** More indexes speed analytics queries but add write overhead. Since the engine’s critical path includes a Postgres insert, this balances retrieval needs for reconciliation and the AI query layer.

---

## Failure semantics (Part 4c)
1) **Postgres commit succeeds, then Celery broker is unreachable**
- Current behavior: trade is persisted with `notification_status="pending"`, but `send_task` may fail.
- Desired semantics: notification should still happen later via reconciliation.
- Current approach: reconciliation beat will repeatedly enqueue notifications for trades not marked success.

2) **Celery worker sends successfully, then crashes before acknowledging**
- With `task_acks_late=True`, a crash can trigger redelivery.
- Current approach is idempotent: the worker transitions `notification_status` from pending→success once. Redelivery checks DB first and becomes a no-op.

3) **Worker pool is 4 processes; 200 spikes fire in 10s**
- Tick ingestion continues because notification work is offloaded to Celery.
- Latency impact: ingestion-to-signal should stay low; persistence insert remains synchronous to the pipeline.
- Mitigation (planned next): add backpressure via Redis window operations optimization and ensure DB pool sizes are sufficient; optionally decouple persistence from detector if allowed (spec excludes Postgres from latency measurement, but crash safety still matters).

---

## ATM strike decision (ambiguous spec row)
Spec includes a non-monotonic example where “third row is not a typo”.
Decision implemented:
- **Round to nearest 50-point NIFTY strike increment**
- Ties (e.g., exactly halfway) round “up” via Python’s round behavior.

Example mapping in code:
- `22432 → 22450`
- `22450 → 22450`
- `22424 → 22400`
- `22425 → 22450` (tie rounds up)

---

## Latency measurement (Hard requirement: p99 < 50ms tick-to-signal)
### What is measured
In `app/pipeline/tick_pipeline.py`, latency is measured at:
- tick enters ingestion path (monotonic `perf_counter_ns`)
- spike detector emits a decision (before any Postgres write / Celery enqueue)

### Reporting
`app/metrics/latency.py` provides:
- `record_tick_to_signal_latency(...)`
- `flush_latency_report()` writing `reports/latency.json`

> Wire-up note: replay endpoint currently calls `process_tick_pipeline` but does not yet flush the report after replay completes. This must be added to fully comply with the deliverable.

### Current numbers
Not yet produced in this scaffold run—requires executing replay with measurement + flush.

---

## What I cut / not yet completed (and why)
- Live WebSocket ingestion lifecycle hardening:
  - The scaffold uses replay pipeline only. WS reconnect, malformed frame handling, and “must not silently stop” logic must be added.
- Alembic migrations:
  - Scaffold uses `Base.metadata.create_all()` on startup. Proper migrations are recommended.
- Full AI toolset:
  - MCP tools are scaffolded but positions/PnL computations and chart generation are placeholders.
- Replay latency flush/reporting:
  - Must be integrated into `/debug/replay` after processing NDJSON ticks.

---

## “Questions you asked” and answers
- Used one scorable clarification:
  - Default LLM provider: **OpenAI** (implemented via `OPENAI_API_KEY` with fallback when missing).

---

## One thing learned / got wrong (simulated)
- Implemented a latency metric module initially with a typo; corrected it afterward.
- Lesson: strict “measurable path” correctness matters more than overall features—small issues break the p99 requirement.

---

## Run locally (Docker)
```bash
docker compose up --build
```

Open API docs:
- http://localhost:8000/docs

---

## Endpoints
### Health
- `GET /healthz`

### Replay
- `POST /debug/replay`
- Body: NDJSON ticks:
  - `{"security_id":"13","ltp":22450.5,"ts":"2026-07-10T09:31:04.221Z"}` one per line

### Ask
- `POST /ask` with:
```json
{ "question": "What was the last trade?" }
```

---

## Repo structure (key files)
- `app/main.py` — FastAPI app
- `app/routes/debug_replay.py` — replay endpoint
- `app/routes/ask.py` — natural-language endpoint
- `app/pipeline/tick_pipeline.py` — **shared pipeline** (ingestion → detector → sim → persistence → notification enqueue)
- `app/spikes/redis_window.py` — Redis rolling window
- `app/persistence/models.py` — Postgres Trade model
- `app/notifications/celery_app.py` — broker/backend
- `app/notifications/tasks.py` — notification task with idempotency gate
- `app/reconciliation/beat.py` — periodic reconciliation
- `app/ai/nl_endpoint.py` — OpenAI-backed /ask handler with fallback
- `app/ai/mcp_server.py` — FastMCP tool stubs
- `app/metrics/latency.py` — latency measurement utilities
