#!/usr/bin/env python3
"""Backfill US macro historical data from FRED.  Provenance: FRED (global)."""
import json, os, sys
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fred_api_service import FredAPIClient

OUT = os.path.join(os.path.dirname(__file__), 'us_macro_historical.json')

SERIES = {
    'DGS10':     'DGS10',
    'FEDFUNDS':  'FEDFUNDS',
    'DTWEXBGS':  'DTWEXBGS',
    'VIXCLS':    'VIXCLS',
    'T10YIE':    'T10YIE',
}

def build():
    client = FredAPIClient()
    dfs = {}
    for sid, label in SERIES.items():
        df = client.get_historical(sid, start='2000-01-01')
        dfs[label] = df.set_index('date').resample('ME').last().rename(columns={'value': label})

    # Merge all
    merged = None
    for label in SERIES.values():
        m = dfs[label]
        merged = m if merged is None else merged.join(m, how='outer')

    merged = merged.sort_index().ffill()

    records = []
    for dt, row in merged.iterrows():
        rec = {'Date': dt.strftime('%Y-%m-%d'), 'Source': 'FRED'}
        for label in SERIES.values():
            v = row.get(label)
            rec[label] = round(float(v), 2) if pd.notna(v) else None
        records.append(rec)

    with open(OUT, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"✅ us_macro_historical.json: {len(records)} monthly records, {records[0]['Date']} → {records[-1]['Date']}")

if __name__ == '__main__':
    build()
