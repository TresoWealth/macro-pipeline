#!/usr/bin/env python3
"""Merge all standalone historical JSONs into the master macro data file.

Produces: macro_data_2000_2026_100pct_real.json with top-level keys for every
data domain.  Each domain is a list of monthly records keyed by Date.

All records carry a Source field with clear provenance (FRED=global,
RBI/MOSPI/NSE/NSDL=India).
"""

import json, os
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
MASTER = os.path.join(HERE, 'macro_data_2000_2026_100pct_real.json')

# Map domain key -> (source_file, normalise_fn | None)
# None = keep as-is; otherwise apply fn to each record
DOMAINS = {
    'rbi':             'rbi_historical.json',
    'inflation':       'inflation_historical.json',
    'growth':          'growth_historical.json',
    'nse':             'nse_historical.json',
    'oil':             'oil_historical.json',
    'fx':              'fx_historical.json',
    'fpi_flows':       'fpi_flows_historical.json',
    'us_macro':        'us_macro_historical.json',
    'pmi':             'pmi_historical.json',
    'sector_indices':  'sector_indices_historical.json',
    'regimes':         'regimes_2000_2026_100pct_real.json',
}


def load_json(filename: str) -> list:
    path = os.path.join(HERE, filename)
    if not os.path.exists(path):
        print(f'  ⚠️ {filename} not found — skipping')
        return []
    with open(path) as f:
        data = json.load(f)
    # Some files are dicts, some are lists
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Return first list value (ignore metadata keys)
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


def normalise_date(rec: dict) -> dict:
    """Ensure Date is YYYY-MM-01 (first of month)."""
    d = rec.get('Date', '')
    if not d:
        return rec
    # Handle various formats
    parts = d.split('-')
    if len(parts) >= 2:
        rec['Date'] = f'{parts[0]}-{parts[1]}-01'
    return rec


def build():
    # Load existing master (preserve gdp, cpi, iip, coverage, metadata)
    existing = {}
    if os.path.exists(MASTER):
        with open(MASTER) as f:
            existing = json.load(f)
        print(f'Loaded existing master: {list(existing.keys())}')

    merged = {
        'metadata': existing.get('metadata', {
            'created': datetime.now().isoformat(),
            'target_range': '2000-2026',
            'data_quality': '100%_REAL',
        }),
    }

    # Preserve existing domains
    for key in ['gdp', 'cpi', 'iip']:
        merged[key] = existing.get(key, [])

    # Merge new domains
    coverage = existing.get('coverage', {})
    coverage_update = {}

    for domain_key, filename in DOMAINS.items():
        records = load_json(filename)
        if not records:
            coverage_update[domain_key] = {'records': 0, 'status': 'missing'}
            continue

        # Normalise dates
        records = [normalise_date(r) for r in records]
        # Ensure Source field
        for r in records:
            if 'Source' not in r:
                r['Source'] = 'RBI' if domain_key == 'rbi' else \
                              'MOSPI' if domain_key in ('inflation', 'growth') else \
                              'FRED' if domain_key in ('oil', 'fx', 'us_macro') else \
                              'NSDL' if domain_key == 'fpi_flows' else \
                              'S&P_Global' if domain_key == 'pmi' else \
                              'NSE'

        merged[domain_key] = records
        coverage_update[domain_key] = {
            'records': len(records),
            'start': records[0]['Date'] if records else None,
            'end': records[-1]['Date'] if records else None,
            'source': records[0].get('Source', '?') if records else '?',
        }
        print(f'  ✅ {domain_key}: {len(records)} records, '
              f'{coverage_update[domain_key]["start"]} -> {coverage_update[domain_key]["end"]}')

    # Preserve existing coverage entries, overlay new
    merged['coverage'] = {**coverage, **coverage_update}

    # Update metadata timestamp
    merged['metadata']['updated'] = datetime.now().isoformat()
    merged['metadata']['domains'] = len([k for k in merged if k not in ('metadata', 'coverage')])

    with open(MASTER, 'w') as f:
        json.dump(merged, f, indent=2)

    total_domains = len([k for k in merged if k not in ('metadata', 'coverage')])
    total_records = sum(
        len(v) for k, v in merged.items()
        if isinstance(v, list) and k not in ('metadata', 'coverage')
    )
    print(f'✅ Master JSON updated: {total_domains} domains, {total_records} total records')
    print(f'   Domains: {[k for k in sorted(merged) if k not in ("metadata", "coverage")]}')


if __name__ == '__main__':
    build()
