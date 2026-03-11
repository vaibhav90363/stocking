#!/usr/bin/env python3
"""Full app test suite for Stocking system."""
import sys, os, traceback
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

results = []

def run_test(name, fn):
    try:
        fn()
        results.append((name, "PASS", ""))
        print(f"  ✅ {name}")
    except Exception as e:
        results.append((name, "FAIL", str(e)))
        print(f"  ❌ {name}: {e}")
        traceback.print_exc()

# ════════════════════════════════════════════════════════════
# TEST 1: Config
# ════════════════════════════════════════════════════════════
def test_config():
    from stocking_app.config import load_config
    c = load_config()
    assert c.ticker_suffix == ".NS", f"Expected .NS, got {c.ticker_suffix}"
    assert c.exchange_tz == "Asia/Kolkata"
    assert c.database_url, "DATABASE_URL is empty"
    assert c.cycle_seconds > 0
    assert c.daily_lookback_days > 0
    print(f"    suffix={c.ticker_suffix} tz={c.exchange_tz} cycle={c.cycle_seconds}s")

# ════════════════════════════════════════════════════════════
# TEST 2: Market Schedule
# ════════════════════════════════════════════════════════════
def test_market_schedule():
    from stocking_app.market_schedule import market_status, defaults_for_suffix
    for sfx in [".NS", ".L", ".US"]:
        tz, o, c = defaults_for_suffix(sfx)
        ms = market_status(tz, o, c)
        assert "market_open" in ms
        assert "next_event" in ms
        assert "next_event_in" in ms
        print(f"    {sfx}: open={ms['market_open']} next={ms['next_event']} in {ms['next_event_in']}")

# ════════════════════════════════════════════════════════════
# TEST 3: Indicators
# ════════════════════════════════════════════════════════════
def test_indicators():
    import pandas as pd, numpy as np
    from stocking_app.indicators import fractal_chaos_bands, calculate_cmo, ema, sma

    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=60, freq="B")
    close = 100 + np.cumsum(np.random.randn(60))
    high = close + abs(np.random.randn(60))
    low = close - abs(np.random.randn(60))
    opn = close + np.random.randn(60) * 0.5
    vol = np.random.randint(1000, 10000, 60).astype(float)
    df = pd.DataFrame({"open": opn, "high": high, "low": low, "close": close, "volume": vol}, index=dates)

    fb = fractal_chaos_bands(df, 2, 2)
    assert "upper_band_line" in fb.columns
    assert "lower_band_line" in fb.columns

    cmo = calculate_cmo(df, "close", 11)
    assert cmo.notna().sum() > 0

    e = ema(df["close"], 5)
    s = sma(df["close"], 5)
    assert e.notna().sum() > 0
    assert s.notna().sum() > 0

# ════════════════════════════════════════════════════════════
# TEST 4: Strategy Signal Computation
# ════════════════════════════════════════════════════════════
def test_strategy():
    import pandas as pd, numpy as np
    from stocking_app.strategy import compute_symbol_signal

    # Edge case: empty DataFrame
    result = compute_symbol_signal("TEST.NS", pd.DataFrame(), "Asia/Kolkata")
    assert result["signal"] is None
    assert result["signal_reason"] == "no_daily_candles"
    print(f"    empty df: reason={result['signal_reason']}")

    # Normal case: 60 daily bars
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=60, freq="B", tz="UTC")
    close = 100 + np.cumsum(np.random.randn(60))
    high = close + abs(np.random.randn(60))
    low = close - abs(np.random.randn(60))
    opn = close + np.random.randn(60) * 0.5
    vol = np.random.randint(1000, 10000, 60).astype(float)
    df = pd.DataFrame({"open": opn, "high": high, "low": low, "close": close, "volume": vol}, index=dates)

    result = compute_symbol_signal("TEST.NS", df, "Asia/Kolkata")
    assert "signal" in result
    assert "last_price" in result
    assert result["last_price"] is not None
    print(f"    60-bar synthetic: signal={result['signal']} reason={result['signal_reason']}")

# ════════════════════════════════════════════════════════════
# TEST 5: Database CRUD
# ════════════════════════════════════════════════════════════
def test_database():
    from stocking_app.config import load_config
    from stocking_app.db import TradingRepository

    cfg = load_config()
    r = TradingRepository(cfg.database_url, ".NS")

    # init_db (idempotent)
    r.init_db()
    r.init_db()  # second call should not error
    print("    init_db: idempotent ✓")

    # heartbeat round-trip
    r.set_engine_heartbeat({"state": "test", "ts": "2025-01-01T00:00:00Z"})
    hb = r.get_engine_heartbeat()
    assert hb is not None
    assert hb.get("state") == "test"
    print("    heartbeat round-trip ✓")

    # get_monitor_symbols (should not error)
    syms = r.get_monitor_symbols()
    assert isinstance(syms, list)
    print(f"    get_monitor_symbols: {len(syms)} symbols ✓")

    # get_universe_summary
    summary = r.get_universe_summary()
    assert "total" in summary
    print(f"    universe_summary: total={summary['total']} active={summary['active']} ✓")

    # insert_log → get_recent_logs
    r.insert_log("INFO", "test_full.py smoke test")
    logs = r.get_recent_logs(5)
    assert len(logs) > 0
    print(f"    insert_log → get_recent_logs: {len(logs)} rows ✓")

    # read_df
    df = r.read_df("SELECT COUNT(*) AS cnt FROM universe WHERE symbol LIKE %s", ("%.NS",))
    assert "cnt" in df.columns
    print(f"    read_df: {df['cnt'].iloc[0]} .NS symbols in universe ✓")

    r.close()

# ════════════════════════════════════════════════════════════
# TEST 6: Data Fetcher
# ════════════════════════════════════════════════════════════
def test_data_fetcher():
    from stocking_app.data_fetcher import fetch_daily_bars_gen

    syms = ["RELIANCE.NS", "TCS.NS", "INFY.NS"]
    results_list = list(fetch_daily_bars_gen(syms, lookback_days=10))
    assert len(results_list) == 3, f"Expected 3 results, got {len(results_list)}"

    ok = sum(1 for r in results_list if not r.error and not r.bars.empty)
    print(f"    fetched {ok}/{len(syms)} symbols successfully")
    assert ok >= 2, f"Expected at least 2 successful fetches, got {ok}"

    for r in results_list:
        if not r.error:
            assert "open" in r.bars.columns
            assert "close" in r.bars.columns
            assert len(r.bars) > 0

# ════════════════════════════════════════════════════════════
# RUN ALL
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("  STOCKING FULL APP TEST SUITE")
print("="*60 + "\n")

run_test("T1: Config Loading", test_config)
run_test("T2: Market Schedule", test_market_schedule)
run_test("T3: Indicators", test_indicators)
run_test("T4: Strategy Signals", test_strategy)
run_test("T5: Database CRUD", test_database)
run_test("T6: Data Fetcher", test_data_fetcher)

print("\n" + "="*60)
print("  RESULTS")
print("="*60)
passed = sum(1 for _, s, _ in results if s == "PASS")
failed = sum(1 for _, s, _ in results if s == "FAIL")
for name, status, err in results:
    icon = "✅" if status == "PASS" else "❌"
    line = f"  {icon} {name}"
    if err:
        line += f" — {err[:80]}"
    print(line)
print(f"\n  {passed} passed, {failed} failed out of {len(results)} tests")
if failed:
    sys.exit(1)
