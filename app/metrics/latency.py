import json
import logging
import os
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

_METRICS_FILE = os.getenv("LATENCY_REPORT_FILE", "reports/latency.json")

# In-memory storage; flushed after replay completes.
_decision_latencies_ns: List[int] = []


def get_tick_start_ns() -> int:
    """
    Called when a tick enters the ingestion path.
    Uses monotonic clock suitable for latency measurement.
    """
    return time.perf_counter_ns()


async def record_tick_to_signal_latency(
    start_ns: int,
    decision_ns_start: Optional[int] = None,
    decision: str = "signal_emitted",
) -> None:
    """
    Records tick-to-signal latency up to the moment spike detector emits a decision.
    Excludes Postgres write and Celery dispatch by design: caller must provide start_ns
    and only call this after decision emission.
    """
    end_ns = time.perf_counter_ns()
    latency_ns = end_ns - start_ns
    _decision_latencies_ns.append(latency_ns)
    logger.debug("Tick-to-signal latency: %.3f ms", latency_ns / 1e6)


def _percentile(sorted_vals: List[int], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return float(d0 + d1)


def flush_latency_report() -> dict:
    """
    Writes latency report to LATENCY_REPORT_FILE and returns the summary.
    Called after replay runs to satisfy the deliverable requirement.
    """
    os.makedirs(os.path.dirname(_METRICS_FILE) or ".", exist_ok=True)

    vals = list(_decision_latencies_ns)
    vals_sorted = sorted(vals)
    total = len(vals_sorted)

    report = {
        "count": total,
        "p50_ms": round(_percentile(vals_sorted, 0.50) / 1e6, 4),
        "p95_ms": round(_percentile(vals_sorted, 0.95) / 1e6, 4),
        "p99_ms": round(_percentile(vals_sorted, 0.99) / 1e6, 4),
        "max_ms": round((max(vals_sorted) / 1e6) if vals_sorted else 0.0, 4),
    }

    with open(_METRICS_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info(
        "Latency report flushed: count=%d p50=%.3fms p95=%.3fms p99=%.3fms max=%.3fms",
        total, report["p50_ms"], report["p95_ms"], report["p99_ms"], report["max_ms"],
    )

    return report


def reset_latency_metrics() -> None:
    """Reset in-memory latency storage (useful between replay runs)."""
    _decision_latencies_ns.clear()
