from sqlalchemy import text

from app.persistence.db import get_engine
from app.persistence.models import Base


async def init_db() -> None:
    """
    Best-effort table creation for the scaffold (so docker-compose runs without Alembic wiring).
    For production, use Alembic migrations.
    """
    engine = await get_engine()
    async with engine.begin() as conn:
        # Create all declared ORM tables.
        await conn.run_sync(Base.metadata.create_all)
