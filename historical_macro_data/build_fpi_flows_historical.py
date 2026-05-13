#!/usr/bin/env python3
"""Backfill FPI flows historical from existing fci_components_enhanced.csv.  Provenance: NSDL (India)."""
import json, os, csv
from datetime import datetime

SRC = os.path.join(os.path.dirname(__file__), 'fci_components_enhanced.csv')
OUT = os.path.join(os.path.dirname(__file__), 'fpi_flows_historical.json')

def build():
    rows = []
    with open(SRC) as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    # Group by year-month, take last day of month value
    monthly = {}
    for r in rows:
        try:
            dt = datetime.strptime(r['Date'], '%Y-%m-%d')
        except ValueError:
            continue
        key = dt.strftime('%Y-%m') + '-01'  # use 1st as key, replace later
        monthly[key] = {
            'FPI_Equity_Flow':    float(r.get('FPI_Equity_Flow', 0) or 0),
            'FPI_Debt_Flow':      float(r.get('FPI_Debt_Flow', 0) or 0),
            'FPI_Total_Flow':     float(r.get('FPI_Total_Flow', 0) or 0),
            'FPI_Equity_Flow_3M': float(r.get('FPI_Equity_Flow_3M', 0) or 0),
            'FPI_Debt_Flow_3M':   float(r.get('FPI_Debt_Flow_3M', 0) or 0),
            'FPI_Equity_Flow_12M': float(r.get('FPI_Equity_Flow_12M', 0) or 0),
            'FPI_Debt_Flow_12M':  float(r.get('FPI_Debt_Flow_12M', 0) or 0),
            'date': dt,
        }

    records = []
    for key in sorted(monthly.keys()):
        m = monthly[key]
        dt = m['date']
        # Use last day of month for Date
        if dt.month == 12:
            last_day = datetime(dt.year, 12, 31)
        else:
            last_day = datetime(dt.year, dt.month + 1, 1)
            from datetime import timedelta
            last_day = last_day - timedelta(days=1)

        records.append({
            'Date': last_day.strftime('%Y-%m-%d'),
            'FPI_Equity_Flow':    round(m['FPI_Equity_Flow'], 2),
            'FPI_Debt_Flow':      round(m['FPI_Debt_Flow'], 2),
            'FPI_Equity_Flow_3M': round(m['FPI_Equity_Flow_3M'], 2),
            'FPI_Debt_Flow_3M':   round(m['FPI_Debt_Flow_3M'], 2),
            'Source': 'NSDL_FPI_DailyReport',
        })

    with open(OUT, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"✅ fpi_flows_historical.json: {len(records)} monthly records, {records[0]['Date']} → {records[-1]['Date']}")

if __name__ == '__main__':
    build()
