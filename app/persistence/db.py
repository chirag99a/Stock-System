import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

_engine = None


def _db_url() -> str:
    # e.g. postgresql+asyncpg://user:pass@postgres:5432/dbname
    return os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@postgres:5432/instant_strike",
    )


async def get_engine():
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            _db_url(),
            pool_pre_ping=True,
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
        )
    return _engine


async def get_sessionmaker(engine=None):
    engine = engine or await get_engine()
    return async_sessionmaker(bind=engine, expire_on_commit=False)
