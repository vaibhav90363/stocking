import pandas as pd
import requests

def fetch_html(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    return requests.get(url, headers=headers).text

def test_scrape():
    # NIFTY
    n500 = pd.read_html(fetch_html('https://en.wikipedia.org/wiki/NIFTY_500'))[2]
    print(f"NIFTY 500: {len(n500)} items, columns: {list(n500.columns)}")
    print(n500.head(3))
    
    f100 = pd.read_html(fetch_html('https://en.wikipedia.org/wiki/FTSE_100_Index'))
    for i, t in enumerate(f100):
        if "Ticker" in t.columns or "EPIC" in t.columns or "Ticker string" in t.columns or 'Ticker symbol' in t.columns:
            print(f"FTSE 100: table {i}, {len(t)} items, cols: {list(t.columns)}")
            print(t.head(3))
    
    f250 = pd.read_html(fetch_html('https://en.wikipedia.org/wiki/FTSE_250_Index'))
    for i, t in enumerate(f250):
        if "Ticker" in t.columns or "EPIC" in t.columns or "Ticker string" in t.columns or 'Ticker symbol' in t.columns:
            print(f"FTSE 250: table {i}, {len(t)} items, cols: {list(t.columns)}")
            print(t.head(3))

    nasdaq = pd.read_html(fetch_html('https://en.wikipedia.org/wiki/Nasdaq-100'))
    for i, t in enumerate(nasdaq):
        if "Ticker" in t.columns or "Symbol" in t.columns or 'Ticker symbol' in t.columns or 'Ticker' in t.columns:
            print(f"NASDAQ 100: table {i}, {len(t)} items, cols: {list(t.columns)}")
            print(t.head(3))

if __name__ == "__main__":
    test_scrape()
