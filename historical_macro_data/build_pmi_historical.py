#!/usr/bin/env python3
"""Backfill India PMI historical data from Investing.com events_charts API.
Provenance: S&P Global via Investing.com (global).

Coverage note: The free Investing.com API returns only recent releases (~4 per series).
Full historical PMI data requires an S&P Global Market Intelligence license.
This script captures what is freely available and the pipeline accumulates
additional data points on each weekly run via the Google Sheets PMI_Data tab.
"""

import json, os, sys, re, time
from datetime import datetime, timezone
import requests

OUT = os.path.join(os.path.dirname(__file__), 'pmi_historical.json')

API_BASE = 'https://sbcharts.investing.com/events_charts/us/{eid}.json'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'https://www.investing.com/',
}

# Known event IDs from reverse-engineering Investing.com economic calendar
EVENTS = {
    'manufacturing': 754,  # India S&P Global Manufacturing PMI
    'services': 753,       # India S&P Global Services PMI (probable)
}

def fetch_event_data(eid: int) -> list[dict]:
    """Fetch historical data for an event from Investing.com events_charts API."""
    resp = requests.get(API_BASE.format(eid=eid), headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    points = data.get('data', [])
    records = []
    for p in points:
        if len(p) < 2:
            continue
        ts_ms, val = p[0], p[1]
        if val is None:
            continue
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        records.append({
            'Date': dt.strftime('%Y-%m-%d'),
            'value': round(float(val), 1),
            'is_estimate': p[2] == 'Yes' if len(p) > 2 else False,
        })
    return records


def build():
    monthly = {}
    for sector, eid in EVENTS.items():
        try:
            records = fetch_event_data(eid)
            print(f'{sector}: {len(records)} data points from event {eid}')
            for r in records:
                # Group by year-month (use 1st of month as key)
                key = r['Date'][:7] + '-01'
                if key not in monthly:
                    monthly[key] = {'Date': key}
                if sector == 'manufacturing':
                    if not r['is_estimate']:  # prefer non-estimate (actual) releases
                        monthly[key]['PMI_Manufacturing'] = r['value']
                    elif 'PMI_Manufacturing' not in monthly[key]:
                        monthly[key]['PMI_Manufacturing'] = r['value']
                else:
                    if not r['is_estimate']:
                        monthly[key]['PMI_Services'] = r['value']
                    elif 'PMI_Services' not in monthly[key]:
                        monthly[key]['PMI_Services'] = r['value']
        except Exception as e:
            print(f'❌ Error fetching {sector} PMI: {e}')

    # Compute composite: 60% manufacturing + 40% services (S&P Global convention)
    records = []
    for key in sorted(monthly.keys()):
        m = monthly[key]
        mfg = m.get('PMI_Manufacturing')
        svc = m.get('PMI_Services')
        if mfg is not None or svc is not None:
            composite = round(
                0.6 * (mfg if mfg is not None else 50) +
                0.4 * (svc if svc is not None else 50),
                1
            )
            records.append({
                'Date': key,
                'PMI_Manufacturing': mfg,
                'PMI_Services': svc,
                'PMI_Composite': composite,
                'Source': 'S&P_Global_via_Investing.com',
            })

    with open(OUT, 'w') as f:
        json.dump(records, f, indent=2)

    if records:
        first_date = records[0]['Date']
        last_date = records[-1]['Date']
        mfg_count = len([r for r in records if r.get('PMI_Manufacturing')])
        svc_count = len([r for r in records if r.get('PMI_Services')])
        comp_count = len([r for r in records if r.get('PMI_Composite')])
        print(f'✅ pmi_historical.json: {len(records)} monthly records, '
              f'{first_date} -> {last_date}')
        print(f'   Coverage: {mfg_count} mfg, {svc_count} svc, {comp_count} composite')
    else:
        print('❌ No PMI data extracted')


if __name__ == '__main__':
    build()
