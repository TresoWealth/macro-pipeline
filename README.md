# Macro Pipeline

Macro-economic regime classification and data pipeline for TresoWealth.

## Components

| File | Purpose |
|------|---------|
| `enhanced_regime_classifier.py` | 4-regime classifier: FCI components + Markov switching + persistence scoring |
| `regime_classifier.py` | Lightweight regime classifier with transition detection |
| `regime_transition_model.py` | Hamilton filter Markov switching model for regime transition probabilities |
| `macro_data_fetcher_v2.py` | Multi-source macro data fetcher (FRED, RBI, NSE, MOSPI) |
| `macro_data_historical.py` | Historical macro data store and retrieval |
| `macro_data_scheduler.py` | Scheduled macro data updates (weekly Sunday cron) |
| `macro_data_sheets_v2.py` | Google Sheets integration for macro data |
| `macro_report_generator.py` | Weekly macro report generation |
| `fred_api_service.py` | FRED API client for US macro data |
| `nifty_usd_regime_analyzer.py` | Nifty USD-denominated regime analysis |

## Data

- `historical_macro_data/` — Regime history, FPI flows, sector indices, NSE data, policy rates
- `current_regime.json` — Current regime classification output

## Current Regime Model

4 regimes based on Growth-Inflation quadrant:
- **Growth-Disinflation** (Green): High growth, falling inflation — risk-on
- **Growth-Inflation** (Orange): High growth, rising inflation — equity favorable
- **Stagnation-Disinflation** (Blue): Low growth, falling inflation — defensive
- **Stagflation** (Red): Low growth, rising inflation — risk-off

## Status

Extracted May 2026 from `treso_report/macro_pipeline/` for dedicated maintenance.
