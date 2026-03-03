from stocking_app.db import TradingRepository
from stocking_app.config import load_config
cfg = load_config()
repo = TradingRepository(cfg.database_url)
print("Engine enabled:", repo.get_engine_enabled())
hb = repo.get_engine_heartbeat()
print("Heartbeat:", hb)
