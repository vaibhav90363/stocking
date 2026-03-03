from pathlib import Path
from stocking_app.strategy_loader import load_strategy
from stocking_app.config import load_config

import sys
sys.path.insert(0, str(Path(".").resolve()))

strategy_name_or_path = "fractal_momentum_nasdaq"
p = Path(strategy_name_or_path)

if not p.is_absolute() and not p.exists():
    root = Path(".").resolve()
    strategy_dir = root / "strategies" / p.name
else:
    strategy_dir = p

print(f"Strategy Dir: {strategy_dir}")

strat_config = load_strategy(strategy_dir)
global_cfg = load_config()

cfg = strat_config.to_app_config(database_url=global_cfg.database_url)

print(f"Ticker Suffix: {cfg.ticker_suffix}")
print(f"Fetch Lookback: {cfg.fetch_lookback_days}")
print(f"Market Open: {cfg.market_open}")
print(f"DB Path from strategy: {cfg.db_path}")

print("Test Passed!")
