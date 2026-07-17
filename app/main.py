from fastapi import FastAPI

from app.persistence.init_db import init_db
from .routes.debug_replay import router as debug_replay_router
from .routes.ask import router as ask_router

app = FastAPI(title="Instant Strike Execution Engine", version="0.1.0")

app.include_router(debug_replay_router)
app.include_router(ask_router)


@app.on_event("startup")
async def _startup():
    await init_db()


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
