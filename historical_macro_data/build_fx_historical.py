#!/usr/bin/env python3
"""Backfill FX historical data (USDINR) from FRED.  Provenance: FRED (global)."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fred_api_service import FredAPIClient

OUT = os.path.join(os.path.dirname(__file__), 'fx_historical.json')

def build():
    client = FredAPIClient()
    df = client.get_historical('DEXINUS', start='2000-01-01')
    monthly = df.set_index('date').resample('ME').last().rename(columns={'value': 'USDINR'})

    records = []
    for dt, row in monthly.iterrows():
        records.append({
            'Date': dt.strftime('%Y-%m-%d'),
            'USDINR': round(float(row['USDINR']), 2),
            'Source': 'FRED_DEXINUS',
        })

    with open(OUT, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"✅ fx_historical.json: {len(records)} monthly records, {records[0]['Date']} → {records[-1]['Date']}")

if __name__ == '__main__':
    build()
