import hashlib
from datetime import datetime

# Premium fetching/mocking.
# Spec allows "Fetch or mock option premium". For replay and offline evaluation,
# we provide deterministic mocked premiums, configurable via env.


def _deterministic_premium(instrument: str, ts: datetime, scale: float = 1.0) -> float:
    """
    Deterministic pseudo-premium so replay runs are stable.
    """
    payload = f"{instrument}|{ts.isoformat()}".encode("utf-8")
    h = hashlib.sha256(payload).hexdigest()
    # Map to [1, 200] then scale
    base = int(h[:8], 16) % 200 + 1
    return float(base) * scale


async def get_option_premium(instrument: str, side: str, ts: datetime) -> float:
    """
    Returns option premium for the option instrument.
    side: LONG/SHORT (can be used to vary premium if needed; here deterministic)
    """
    # Mock-only for now; could be extended to call DhanHQ option LTP.
    scale = float(__import__("os").environ.get("PREMIUM_MOCK_SCALE", "1.0"))
    premium = _deterministic_premium(instrument=instrument, ts=ts, scale=scale)

    # Optional tweak: CE/PE could have slight deterministic difference.
    if ":CE" in instrument:
        return premium * 1.01
    if ":PE" in instrument:
        return premium * 0.99
    return premium
