# Instant Strike — Execution Engine

## What this repo implements
- **FastAPI server** with three endpoints:
  - `POST /debug/replay` — accepts NDJSON ticks and runs them through the **exact same pipeline** as live ticks via the shared `process_tick_pipeline`. Returns latency report after processing.
  - `POST /ask` — natural-language endpoint with an OpenAI-backed handler and a DB-backed fallback that returns actual trade data when no API key is present.
  - `GET /healthz` — health check including WebSocket consumer status.
- **Live WebSocket ingestion** via DhanHQ Market Feed:
  - Connects with exponential backoff reconnection (must not silently stop).
  - Health monitoring via last tick timestamp.
  - Shares the same `process_tick_pipeline` code path as replay.
  - Disabled by default; enabled by setting `DHAN_ACCESS_TOKEN`.
- **Spike detection + simulated option trades**:
  - Maintains a rolling **60-second** Redis ZSET window per `security_id`.
  - Triggers Long/Short when the % move vs ~60s ago crosses ±5%.
  - Computes ATM strike (nearest 50-point NIFTY increment, documented below).
  - Deterministic/mocked option premium for replay reproducibility.
  - Persists trades to Postgres using **async SQLAlchemy**.
- **Notifications (Celery)**:
  - After trade persistence, enqueues a Celery notification task.
  - Task is idempotent by using `Trade.notification_status` as a gate.
  - Retry with exponential backoff (configurable max retries).
  - Celery Beat reconciliation runs every 60 seconds to enqueue any trades whose notifications never succeeded.
- **Latency measurement** (p99 < 50ms target):
  - Measures tick-to-signal latency inside the shared pipeline using monotonic `perf_counter_ns`.
  - Measurement starts at pipeline entry and ends when spike detector emits a decision (before Postgres/Celery).
  - Writes aggregate p50/p95/p99/max into `reports/latency.json` after replay completes.
- **AI / MCP layer**:
  - FastMCP server with tools: `get_last_trade`, `get_open_positions`, `get_pnl_summary`, `get_spike_events`, `get_best_strike_accuracy`, `generate_trade_chart`.
  - `/ask` endpoint uses OpenAI with trade context, or falls back to DB-backed answers.

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

**Tradeoff accepted:** ZSET provides easy range/expiry semantics and fast approximate historical lookup with minimal logic. It's more CPU than a pure list, but dramatically simpler to keep "~t-60s" semantics correct under replay noise.

### 2) Broker & result backend choice (Celery)
Celery uses **Redis** as both:
- broker: `CELERY_BROKER_URL=redis://...:1`
- result backend: `CELERY_RESULT_BACKEND=redis://...:2`

**Tradeoff accepted:** Redis is convenient and matches the stack, but RabbitMQ would typically offer stronger messaging semantics under certain failure modes. This scaffold favors simplicity and local replay evaluability.

### 3) Index choice in Postgres
`Trade` table indexes exist on:
- `instrument`, `strike`, `option_type`, `side`, `created_at`, `notification_status`, and a uniqueness constraint on `notification_dedup_key`.

**Tradeoff accepted:** More indexes speed analytics queries but add write overhead. Since the engine's critical path includes a Postgres insert, this balances retrieval needs for reconciliation and the AI query layer.

---

## Failure semantics (Part 4c)
1) **Postgres commit succeeds, then Celery broker is unreachable**
   - Current behavior: trade is persisted with `notification_status="pending"`, but `send_task` may fail.
   - The pipeline wraps notification enqueue in try/except — trade is safely persisted regardless.
   - Recovery: reconciliation beat will repeatedly enqueue notifications for trades not marked success.

2) **Celery worker sends successfully, then crashes before acknowledging**
   - With `task_acks_late=True`, a crash can trigger redelivery.
   - The worker transitions `notification_status` from pending→success once. Redelivery checks DB first and becomes a no-op (idempotent).

3) **Worker pool is 4 processes; 200 spikes fire in 10s**
   - Tick ingestion continues because notification work is offloaded to Celery.
   - Latency impact: ingestion-to-signal stays low; persistence insert remains synchronous to the pipeline.
   - Mitigation: DB pool sizes are configurable (`DB_POOL_SIZE`, `DB_MAX_OVERFLOW`). Celery `worker_prefetch_multiplier=1` ensures fair distribution.

---

## ATM strike decision (ambiguous spec row)
Spec includes a non-monotonic example where "third row is not a typo".
Decision implemented:
- **Round to nearest 50-point NIFTY strike increment**
- Ties (e.g., exactly halfway) round "up" via Python's round behavior.

Example mapping in code:
- `22432 → 22450`
- `22450 → 22450`
- `22424 → 22400`
- `22425 → 22450` (tie rounds up)

---

## Latency measurement (Hard requirement: p99 < 50ms tick-to-signal)
### What is measured
In `app/pipeline/tick_pipeline.py`, latency is measured at:
- tick enters pipeline function (monotonic `perf_counter_ns()`)
- spike detector emits a decision (before any Postgres write / Celery enqueue)

### Reporting
`app/metrics/latency.py` provides:
- `record_tick_to_signal_latency(start_ns)` — records individual measurement
- `flush_latency_report()` — writes `reports/latency.json` with p50/p95/p99/max

### Wire-up
The `/debug/replay` endpoint:
1. Resets latency metrics before each replay run
2. Processes all ticks through the shared pipeline
3. Flushes the latency report and includes it in the response

---

## "Questions you asked" and answers
- Used one scorable clarification:
  - Default LLM provider: **OpenAI** (implemented via `OPENAI_API_KEY` with fallback when missing).

---

## One thing learned / got wrong
- Initially placed latency measurement at incorrect positions (inside `get_tick_start_ns()` indirection rather than directly at pipeline entry). Corrected to use `time.perf_counter_ns()` directly at the top of `process_tick_pipeline`.
- Lesson: strict "measurable path" correctness matters more than overall features—small issues in measurement methodology break the p99 requirement.

---

## Run locally (Docker)
```bash
docker compose up --build
```

Open API docs:
- http://localhost:8000/docs

### Test replay with sample data
```bash
# From the project root:
curl -X POST http://localhost:8000/debug/replay --data-binary @tests/test_ticks.ndjson
```

### Test the /ask endpoint
```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What was the last trade?"}'
```

---

## Endpoints
### Health
- `GET /healthz`

### Replay
- `POST /debug/replay`
- Body: NDJSON ticks:
  - `{"security_id":"13","ltp":22450.5,"ts":"2026-07-10T09:31:04.221Z"}` one per line
- Response includes: accepted count, processed count, errors, replay elapsed time, and latency report (p50/p95/p99/max).

### Ask
- `POST /ask` with:
```json
{ "question": "What was the last trade?" }
```

---

## Repo structure (key files)
- `Dockerfile` — Production Python 3.12 container
- `docker-compose.yml` — Full stack: API + Worker + Beat + Redis + Postgres
- `app/main.py` — FastAPI app with lifespan handler
- `app/routes/debug_replay.py` — replay endpoint with latency flush
- `app/routes/ask.py` — natural-language endpoint
- `app/pipeline/tick_pipeline.py` — **shared pipeline** (ingestion → detector → sim → persistence → notification enqueue)
- `app/pipeline/ws_ingestion.py` — DhanHQ WebSocket consumer with reconnection
- `app/spikes/redis_window.py` — Redis ZSET rolling window
- `app/quant/option_chain.py` — ATM strike selection logic
- `app/quant/premium_source.py` — Deterministic/mocked option premium
- `app/persistence/models.py` — Postgres Trade model with indexes
- `app/persistence/db.py` — Async SQLAlchemy engine/session management
- `app/persistence/init_db.py` — Table creation on startup
- `app/notifications/celery_app.py` — broker/backend configuration
- `app/notifications/tasks.py` — notification task with idempotency + retry
- `app/reconciliation/beat.py` — periodic reconciliation every 60s
- `app/ai/nl_endpoint.py` — OpenAI-backed /ask handler with DB fallback
- `app/ai/mcp_server.py` — FastMCP tools (positions, PnL, charts, etc.)
- `app/metrics/latency.py` — latency measurement + reporting utilities
- `tests/test_ticks.ndjson` — sample replay tick data
