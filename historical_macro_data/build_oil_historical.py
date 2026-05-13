#!/usr/bin/env python3
"""Backfill oil historical data (Brent + WTI) from FRED.  Provenance: FRED (global)."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fred_api_service import FredAPIClient

OUT = os.path.join(os.path.dirname(__file__), 'oil_historical.json')

def build():
    client = FredAPIClient()
    brent = client.get_historical('DCOILBRENTEU', start='2000-01-01')
    wti   = client.get_historical('DCOILWTICO',   start='2000-01-01')

    brent = brent.set_index('date').resample('ME').last().rename(columns={'value': 'Brent_USD'})
    wti   = wti.set_index('date').resample('ME').last().rename(columns={'value': 'WTI_USD'})

    merged = brent.join(wti, how='outer').sort_index()
    # Forward-fill sparse early data
    merged = merged.ffill().dropna(subset=['Brent_USD', 'WTI_USD'], how='any')

    records = []
    for dt, row in merged.iterrows():
        records.append({
            'Date': dt.strftime('%Y-%m-%d'),
            'Brent_USD': round(float(row['Brent_USD']), 2),
            'WTI_USD': round(float(row['WTI_USD']), 2),
            'Source': 'FRED_DCOILBRENTEU_DCOILWTICO',
        })

    with open(OUT, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"✅ oil_historical.json: {len(records)} monthly records, {records[0]['Date']} → {records[-1]['Date']}")

if __name__ == '__main__':
    build()
