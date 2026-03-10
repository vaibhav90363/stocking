import pandas as pd
import sys

try:
    user_trades = pd.read_csv("user_trades_60.csv")
    sys_trades = pd.read_csv("data/sys_trades_60.csv")
except Exception as e:
    print(f"Error loading CSVs: {e}")
    sys.exit(1)

user_buys = user_trades[['stock_name', 'buy_date', 'buy_price']].copy()
user_buys['date'] = pd.to_datetime(user_buys['buy_date']).dt.date
user_buys['symbol'] = user_buys['stock_name']

sys_buys = sys_trades[sys_trades['side'] == 'BUY'].copy()
sys_buys['date'] = pd.to_datetime(sys_buys['ts']).dt.date
sys_buys['symbol'] = sys_buys['symbol'].str.replace('.NS', '', regex=False)
sys_buys.rename(columns={'price': 'buy_price'}, inplace=True)

user_set = set(zip(user_buys['symbol'], user_buys['date']))
sys_set = set(zip(sys_buys['symbol'], sys_buys['date']))

only_user = user_set - sys_set
only_sys = sys_set - user_set
both = user_set & sys_set

print(f"Total User Buys  : {len(user_buys)}")
print(f"Total System Buys: {len(sys_buys)}")
print(f"Matching Buys    : {len(both)}")

# Check for off-by-1 or 2 days
fuzzy_matches = 0
still_only_user = []
for (s, d) in only_user:
    found = False
    for off in [-2, -1, 1, 2]:
        neighbor = d + pd.Timedelta(days=off)
        if (s, neighbor) in sys_set:
            fuzzy_matches += 1
            found = True
            break
    if not found:
        still_only_user.append((s, d))

print(f"Fuzzy Matches (+/- 2 days): {fuzzy_matches}")
print(f"Unique User Buys (not even fuzzy): {len(still_only_user)}")

if len(still_only_user) > 0:
    print("\n--- Top 10 Truly UNIQUE User Buys ---")
    for s, d in still_only_user[:10]:
        price = user_buys[(user_buys['symbol'] == s) & (user_buys['date'] == d)]['buy_price'].iloc[0]
        print(f"  {s} on {d} @ {price:.2f}")

# Check price differences for exact matches
if both:
    print("\n--- First 5 Matching Buys Price Check ---")
    for s, d in list(both)[:5]:
        u_p = user_buys[(user_buys['symbol'] == s) & (user_buys['date'] == d)]['buy_price'].iloc[0]
        s_p = sys_buys[(sys_buys['symbol'] == s) & (sys_buys['date'] == d)]['buy_price'].iloc[0]
        diff = abs(u_p - s_p)
        print(f"  {s} on {d}: User={u_p:.2f} Sys={s_p:.2f} Diff={diff:.2f}")
