"""
stocking_app/strategy_loader.py
Reads a strategy folder's strategy.yaml and builds AppConfig + strategy params.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml                          # pyyaml
except ImportError:
    # Minimal YAML parser fallback (safe for simple flat/nested YAML)
    yaml = None                          # type: ignore[assignment]

from .config import AppConfig


@dataclass
class StrategyConfig:
    """Everything parsed from strategy.yaml."""
    name: str
    description: str
    strategy_type: str
    strategy_dir: Path

    # exchange
    suffix: str
    timezone: str
    market_open: str
    market_close: str

    # engine
    cycle_seconds: int
    fetch_lookback_days: int
    daily_lookback_days: int
    compute_lookback_days: int
    fetch_concurrency: int
    compute_workers: int
    order_qty: int

    # paths (derived)
    db_path: Path
    log_dir: Path
    universe_csv: Path
    backtest_dir: Path

    # strategy parameters (forwarded to signal code)
    parameters: dict[str, Any]

    # backtest settings
    daily_lookback: str
    intraday_days: int
    backtest_days: int

    def to_app_config(self, database_url: str = "") -> AppConfig:
        return AppConfig(
            db_path=self.db_path,
            database_url=database_url,
            cycle_seconds=self.cycle_seconds,
            disabled_poll_seconds=60,  # match STOCKING_DISABLED_POLL_SECONDS env default
            fetch_lookback_days=self.fetch_lookback_days,
            daily_lookback_days=self.daily_lookback_days,
            compute_lookback_days=self.compute_lookback_days,
            max_fetch_concurrency=self.fetch_concurrency,
            compute_workers=self.compute_workers,
            order_qty=self.order_qty,
            ticker_suffix=self.suffix,
            exchange_tz=self.timezone,
            market_open=self.market_open,
            market_close=self.market_close,
        )


def load_strategy(strategy_dir: str | Path) -> StrategyConfig:
    """
    Load and validate a strategy.yaml from the given directory.
    Creates the data/ subdirectory structure if it doesn't exist.
    """
    d = Path(strategy_dir).resolve()
    yaml_path = d / "strategy.yaml"

    if not yaml_path.exists():
        raise FileNotFoundError(
            f"strategy.yaml not found in {d}\n"
            f"Expected: {yaml_path}"
        )

    raw = _parse_yaml(yaml_path)

    # Create directory structure
    data_dir    = d / "data"
    log_dir     = data_dir / "logs"
    backtest_dir= data_dir / "backtest"
    for p in (data_dir, log_dir, backtest_dir):
        p.mkdir(parents=True, exist_ok=True)

    exc = raw.get("exchange", {})
    eng = raw.get("engine", {})
    bt  = raw.get("backtest", {})

    return StrategyConfig(
        name              = raw.get("name", d.name),
        description       = raw.get("description", ""),
        strategy_type     = raw.get("strategy_type", "fractal_momentum"),
        strategy_dir      = d,

        suffix            = exc.get("suffix", ".NS"),
        timezone          = exc.get("timezone", "Asia/Kolkata"),
        market_open       = exc.get("market_open",  "09:15"),
        market_close      = exc.get("market_close", "15:30"),

        cycle_seconds         = int(eng.get("cycle_seconds", 300)),
        fetch_lookback_days   = int(eng.get("fetch_lookback_days", 2)),
        daily_lookback_days   = int(eng.get("daily_lookback_days", 60)),
        compute_lookback_days = int(eng.get("compute_lookback_days", 365)),
        fetch_concurrency     = int(eng.get("fetch_concurrency", 16)),
        compute_workers       = int(eng.get("compute_workers", 4)),
        order_qty             = int(eng.get("order_qty", 1)),

        db_path       = data_dir / "live.db",
        log_dir       = log_dir,
        universe_csv  = d / "universe.csv",
        backtest_dir  = backtest_dir,

        parameters    = raw.get("parameters", {}),

        daily_lookback= str(bt.get("daily_lookback", "2y")),
        intraday_days = int(bt.get("intraday_days", 60)),
        backtest_days = int(bt.get("backtest_days", 30)),
    )


def discover_strategies(root: str | Path) -> list[StrategyConfig]:
    """Find all strategy.yaml files under `root/strategies/`."""
    root = Path(root).resolve()
    strategies_dir = root / "strategies"
    if not strategies_dir.exists():
        return []
    result = []
    for child in sorted(strategies_dir.iterdir()):
        if child.is_dir() and (child / "strategy.yaml").exists():
            try:
                result.append(load_strategy(child))
            except Exception as e:
                print(f"Warning: could not load {child.name}: {e}", file=sys.stderr)
    return result


# ── Minimal YAML parser (fallback when pyyaml not installed) ─────────────────

def _parse_yaml(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    try:
        import importlib
        _yaml = importlib.import_module("yaml")
        return _yaml.safe_load(text) or {}
    except ImportError:
        pass
    return _minimal_yaml(text)


def _minimal_yaml(text: str) -> dict:
    """
    Parse simple YAML (no anchors) sufficient for strategy.yaml.
    Handles nested dicts via indentation, scalar values, and block lists.

    BUG-18 fix: lines starting with '- ' are now collected as list items
    and attached to the most recent parent key, so list-valued YAML fields
    (e.g. 'symbols:' followed by '  - AAPL') are not silently dropped.
    """
    result: dict = {}
    stack: list[tuple[int, dict]] = [(0, result)]
    last_key: str | None = None
    last_parent: dict | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        stripped = line.strip()

        # BUG-18: handle list items ( "  - value" lines )
        if stripped.startswith("- "):
            item_val = stripped[2:].strip().strip('"').strip("'")
            # Try numeric conversion for list items too
            if item_val.lower() in ("true", "yes"):
                item: Any = True
            elif item_val.lower() in ("false", "no"):
                item = False
            elif item_val.replace("-", "").replace(".", "").isnumeric():
                item = float(item_val) if "." in item_val else int(item_val)
            else:
                item = item_val
            # Attach to the most recent key in the current scope
            if last_key and last_parent is not None:
                existing = last_parent.get(last_key)
                if isinstance(existing, list):
                    existing.append(item)
                else:
                    last_parent[last_key] = [item]
            continue

        if ":" not in stripped:
            continue
        key, _, val = stripped.partition(":")
        key = key.strip()
        val = val.strip().strip('"').strip("'")

        # Pop stack to current indent level
        while len(stack) > 1 and stack[-1][0] > indent:
            stack.pop()

        parent_dict = stack[-1][1]
        last_parent = parent_dict
        last_key = key

        if not val or val.startswith(">"):
            # Nested block
            new_dict: dict = {}
            parent_dict[key] = new_dict
            stack.append((indent + 2, new_dict))
        else:
            # Scalar — try numeric conversion
            if val.lower() in ("true", "yes"):
                parsed: Any = True
            elif val.lower() in ("false", "no"):
                parsed = False
            elif val.replace("-", "").replace(".", "").isnumeric():
                parsed = float(val) if "." in val else int(val)
            else:
                parsed = val
            parent_dict[key] = parsed

    return result
