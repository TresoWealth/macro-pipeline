#!/usr/bin/env python3
"""Backfill NSE sector indices historical from Yahoo Finance.
Provenance: NSE via Yahoo Finance (India).

Covers 12 of 15 required sector indices:
  ✅ Nifty Bank, IT, Financial Services, Pharma, Metal, Realty,
     Energy, Auto, FMCG, Infra, PSE, Private Bank
  ❌ Healthcare, Consumer Durables, Oil & Gas — not listed on Yahoo Finance;
     alternative tickers or NSE direct download needed for these three.
"""

import json, os
import pandas as pd
import yfinance as yf

OUT = os.path.join(os.path.dirname(__file__), 'sector_indices_historical.json')

# Yahoo Finance tickers for NSE sector indices (validated May 2026)
SECTOR_TICKERS = {
    'NIFTY BANK':              '^NSEBANK',
    'NIFTY IT':                '^CNXIT',
    'NIFTY FIN SERVICE':       'NIFTY_FIN_SERVICE.NS',
    'NIFTY PHARMA':            '^CNXPHARMA',
    'NIFTY METAL':             '^CNXMETAL',
    'NIFTY REALTY':            '^CNXREALTY',
    'NIFTY ENERGY':            '^CNXENERGY',
    'NIFTY AUTO':              '^CNXAUTO',
    'NIFTY FMCG':              '^CNXFMCG',
    'NIFTY INFRA':             '^CNXINFRA',
    'NIFTY PSE':               '^CNXPSE',
    'NIFTY PRIVATE BANK':      'NIFTY_PVT_BANK.NS',
}

# Missing from Yahoo Finance (require NSE direct download or alternative source):
#   NIFTY CONSUMER DURABLES
#   NIFTY OIL AND GAS
#   NIFTY HEALTHCARE


def build():
    dfs = {}
    for label, ticker in SECTOR_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            df = t.history(period='max')
            if df.empty:
                print(f'  ❌ {label} ({ticker}): no data')
                continue
            # Resample to month-end closing prices
            monthly = df['Close'].resample('ME').last().rename(label)
            dfs[label] = monthly
            print(f'  ✅ {label}: {len(monthly)} months, '
                  f'{monthly.index[0].strftime("%Y-%m")} -> {monthly.index[-1].strftime("%Y-%m")}')
        except Exception as e:
            print(f'  ❌ {label} ({ticker}): {e}')

    if not dfs:
        print('No sector data retrieved')
        return

    # Merge all on date index
    merged = None
    for label, series in dfs.items():
        merged = series if merged is None else pd.concat([merged, series], axis=1)

    merged = merged.sort_index()

    # Build records
    records = []
    for dt, row in merged.iterrows():
        rec = {'Date': dt.strftime('%Y-%m-%d'), 'Source': 'NSE_via_YahooFinance'}
        for label in SECTOR_TICKERS:
            v = row.get(label)
            rec[label] = round(float(v), 2) if pd.notna(v) else None
        records.append(rec)

    with open(OUT, 'w') as f:
        json.dump(records, f, indent=2)

    covered = [k for k in SECTOR_TICKERS if k in dfs]
    print(f'✅ sector_indices_historical.json: {len(records)} monthly records, '
          f'{records[0]["Date"]} -> {records[-1]["Date"]}')
    print(f'   Indices covered: {len(covered)}/{len(SECTOR_TICKERS)} '
          f'({", ".join(covered)})')
    print(f'   Missing from Yahoo Finance: NIFTY CONSUMER DURABLES, '
          f'NIFTY OIL AND GAS, NIFTY HEALTHCARE')


if __name__ == '__main__':
    build()
