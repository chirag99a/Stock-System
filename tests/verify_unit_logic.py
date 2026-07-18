import asyncio
import os
import shutil
from datetime import datetime, timezone
from app.quant.option_chain import select_atm_strike, build_option_instrument
from app.quant.premium_source import _deterministic_premium, get_option_premium
from app.metrics.latency import record_tick_to_signal_latency, flush_latency_report, reset_latency_metrics, get_tick_start_ns
from app.ai.nl_endpoint import _fallback_answer

def test_atm_strike_selection():
    print("Testing ATM Strike Selection (Spec Compliance)...")
    # Spec examples:
    # atm=22432 -> 22450
    # atm=22450 -> 22450
    # atm=22424 -> 22400
    # atm=22425 -> 22450 (.5 tie rounds up)
    assert select_atm_strike(22432) == 22450, f"Expected 22450, got {select_atm_strike(22432)}"
    assert select_atm_strike(22450) == 22450, f"Expected 22450, got {select_atm_strike(22450)}"
    assert select_atm_strike(22424) == 22400, f"Expected 22400, got {select_atm_strike(22424)}"
    assert select_atm_strike(22425) == 22450, f"Expected 22450, got {select_atm_strike(22425)}"
    assert select_atm_strike(22400) == 22400, f"Expected 22400, got {select_atm_strike(22400)}"
    
    inst = build_option_instrument("13", 22450, "CE")
    assert inst == "13:22450:CE", f"Expected 13:22450:CE, got {inst}"
    print("  ATM Strike Selection & Instrument Building: PASSED!")

async def test_premium_source():
    print("Testing Option Premium Sourcing...")
    ts = datetime(2026, 7, 10, 9, 30, tzinfo=timezone.utc)
    p_ce = await get_option_premium("13:22450:CE", "LONG", ts)
    p_pe = await get_option_premium("13:22450:PE", "SHORT", ts)
    assert p_ce > 0, "CE premium must be positive"
    assert p_pe > 0, "PE premium must be positive"
    print(f"  CE Premium: {p_ce:.2f}, PE Premium: {p_pe:.2f}: PASSED!")

async def test_latency_metrics():
    print("Testing Latency Measurement & Reporting...")
    reset_latency_metrics()
    start_ns = get_tick_start_ns()
    await record_tick_to_signal_latency(start_ns - 15_000_000) # simulated 15ms latency
    await record_tick_to_signal_latency(start_ns - 25_000_000) # simulated 25ms latency
    await record_tick_to_signal_latency(start_ns - 42_000_000) # simulated 42ms latency
    
    report = flush_latency_report()
    assert report["count"] == 3, f"Expected count 3, got {report['count']}"
    assert report["p99_ms"] > 0, "Expected positive p99_ms"
    assert os.path.exists("reports/latency.json"), "Expected reports/latency.json to be generated"
    print(f"  Latency Report: {report}: PASSED!")

async def test_nl_endpoint_fallback():
    print("Testing NL Endpoint Fallback Logic...")
    # Test fallback answers (when DB connection is not present or returns clean fallback)
    # We test that the handler doesn't crash on any expected prompt question types
    ans1 = await _fallback_answer("What was the last trade?")
    assert "answer" in ans1
    ans2 = await _fallback_answer("Show top losing trades")
    assert "answer" in ans2
    ans3 = await _fallback_answer("Which strike performed best?")
    assert "answer" in ans3
    ans4 = await _fallback_answer("Compare CE vs PE")
    assert "answer" in ans4
    ans5 = await _fallback_answer("How many total trades?")
    assert "answer" in ans5
    print("  NL Endpoint Fallback queries executed: PASSED!")

async def test_spike_window():
    print("Testing Redis Sliding Window & Spike Detection Logic...")
    from app.spikes.redis_window import RedisPriceWindow
    from datetime import datetime, timezone
    
    # Mock redis client using an in-memory ZSET matching redis-py async behavior
    class MockRedis:
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

    mock_redis = MockRedis()
    window = RedisPriceWindow(redis_client=mock_redis, key_prefix="instant-strike:prices", window_seconds=60)
    
    # Tick 1 at t=0s: price 22000
    ts0 = datetime.fromisoformat("2026-07-10T09:30:00+00:00")
    await window.append_tick("13", ts0, 22000.0)
    p_60_t0 = await window.fetch_price_at_or_before_shift("13", ts0, 60)
    assert p_60_t0 is None, f"First tick should have no history, got {p_60_t0}"
    
    # Tick 2 at t=60s: price 22200 (+0.9% move vs 22000, below threshold)
    ts60 = datetime.fromisoformat("2026-07-10T09:31:00+00:00")
    await window.append_tick("13", ts60, 22200.0)
    p_60_t60 = await window.fetch_price_at_or_before_shift("13", ts60, 60)
    assert p_60_t60 == 22000.0, f"Expected 22000.0 from ~60s ago, got {p_60_t60}"
    pct_t60 = (22200.0 - p_60_t60) / p_60_t60
    assert not (pct_t60 >= 0.05 or pct_t60 <= -0.05), "Small move should not trigger spike"
    
    # Tick 3 at t=121s: price 23350 vs ~60s ago price 22200 (+5.18% move -> LONG SPIKE!)
    ts121 = datetime.fromisoformat("2026-07-10T09:32:01+00:00")
    await window.append_tick("13", ts121, 23350.0)
    p_60_t121 = await window.fetch_price_at_or_before_shift("13", ts121, 60)
    assert p_60_t121 == 22200.0, f"Expected 22200.0 from ~60s ago, got {p_60_t121}"
    pct_t121 = (23350.0 - p_60_t121) / p_60_t121
    assert pct_t121 >= 0.05, f"Expected pct_t121 >= 0.05 (+5% trigger), got {pct_t121}"
    side_t121 = "LONG" if pct_t121 >= 0.05 else ("SHORT" if pct_t121 <= -0.05 else None)
    assert side_t121 == "LONG"
    
    # Tick 4 at t=182s: price 21800 vs ~60s ago price 23350 (-6.63% move -> SHORT SPIKE!)
    ts182 = datetime.fromisoformat("2026-07-10T09:33:02+00:00")
    await window.append_tick("13", ts182, 21800.0)
    p_60_t182 = await window.fetch_price_at_or_before_shift("13", ts182, 60)
    assert p_60_t182 == 23350.0, f"Expected 23350.0 from ~60s ago, got {p_60_t182}"
    pct_t182 = (21800.0 - p_60_t182) / p_60_t182
    assert pct_t182 <= -0.05, f"Expected pct_t182 <= -0.05 (-5% trigger), got {pct_t182}"
    side_t182 = "LONG" if pct_t182 >= 0.05 else ("SHORT" if pct_t182 <= -0.05 else None)
    assert side_t182 == "SHORT"
    
    print("  RedisPriceWindow append/fetch & LONG (+5.18%) / SHORT (-6.63%) spike triggers: PASSED!")


async def main():
    print("=== RUNNING INSTANT STRIKE UNIT & LOGIC VERIFICATION ===")
    test_atm_strike_selection()
    await test_premium_source()
    await test_latency_metrics()
    await test_nl_endpoint_fallback()
    await test_spike_window()
    print("=== ALL 5 CORE MODULE VERIFICATIONS COMPLETED SUCCESSFULLY ===")

if __name__ == "__main__":
    asyncio.run(main())
