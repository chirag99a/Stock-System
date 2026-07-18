from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import redis.asyncio as redis


@dataclass
class RedisPriceWindow:
    redis_client: redis.Redis
    key_prefix: str
    window_seconds: int = 60

    def _zset_key(self, security_id: str) -> str:
        return f"{self.key_prefix}:{security_id}"

    async def append_tick(self, security_id: str, ts: datetime, ltp: float) -> None:
        """
        Store tick in a Sorted Set:
          ZADD key score=unix_ms member="<unix_ms>|<ltp>"
        We use score=timestamp_ms for fast range queries and expiry.
        """
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        unix_ms = int(ts.timestamp() * 1000)
        member = f"{unix_ms}|{ltp}"
        key = self._zset_key(security_id)

        await self.redis_client.zadd(key, {member: unix_ms})
        # Trim older than window + buffer so fetch_price_at_or_before_shift(..., shift_seconds=60) has target available
        cutoff_ms = unix_ms - ((self.window_seconds + 5) * 1000)
        await self.redis_client.zremrangebyscore(key, "-inf", cutoff_ms)

    async def fetch_price_at_or_before_shift(
        self,
        security_id: str,
        ts: datetime,
        shift_seconds: int,
    ) -> Optional[float]:
        """
        Approximate fetch of price at approximately (ts - shift_seconds) with minimal latency.

        Approach:
        - Compute target_ms = (ts - shift_seconds) in unix_ms.
        - Fetch the greatest score <= target_ms (ZRANGEBYSCORE ... LIMIT 0 1 WITHSCORES not needed).
        - Parse ltp from member string.
        """
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        target = ts - timedelta(seconds=shift_seconds)
        target_ms = int(target.timestamp() * 1000)

        key = self._zset_key(security_id)

        # Get one element with score <= target_ms
        # redis-py returns list of members
        res = await self.redis_client.zrevrangebyscore(key, target_ms, "-inf", start=0, num=1)
        if not res:
            return None

        # member format: "<unix_ms>|<ltp>"
        member = res[0]
        try:
            _, ltp_str = member.split("|", 1)
            return float(ltp_str)
        except Exception:
            return None
