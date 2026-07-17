# Instant Strike — TODO

## Step 1: Project scaffold
- [ ] Create FastAPI app skeleton (routes: `/debug/replay`, `/ask`)
- [ ] Add ingestion (DhanHQ WS consumer + shared pipeline)
- [ ] Add replay endpoint that uses the exact same pipeline code

## Step 2: Redis rolling 60s window + spike detector
- [ ] Implement Redis window (append, expire, fetch approx P(t-60))
- [ ] Implement spike detection logic with tick-to-signal latency instrumentation

## Step 3: Quant logic (ATM + premium mock/fetch) + trade payload
- [ ] Implement ATM strike selection (document decision)
- [ ] Implement option premium source (env-based, with deterministic mock fallback)
- [ ] Persist trade payload to Postgres (async)

## Step 4: Celery notifications
- [ ] Configure Celery (Redis broker/backend)
- [ ] Implement notification task with retry/backoff + idempotency
- [ ] Implement Celery Beat reconciliation every 60s

## Step 5: MCP AI layer
- [ ] Implement MCP server tools
- [ ] Implement `/ask` endpoint using OpenAI with safe fallback if no key

## Step 6: Performance measurement (p99 < 50ms for tick-to-signal)
- [ ] Implement replay-time latency reporting (p50/p95/p99/max)
- [ ] Add measurement method to README

## Step 7: DevOps
- [ ] Add `docker-compose.yml` + `.env.example`
- [ ] Add README (architecture decisions, failure semantics, latency numbers, indexes)
- [ ] Add API docs

## Step 8: Validation
- [ ] Run `docker compose up --build`
- [ ] POST `/debug/replay` with a small NDJSON tick file
- [ ] Verify trades in Postgres and notification idempotency + reconciliation
