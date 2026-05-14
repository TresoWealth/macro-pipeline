# Macro Pipeline

Macro-economic regime classification and data pipeline for TresoWealth. Runs weekly (Sunday 6PM IST), tracks 50+ indicators across 14 domains.

## Components

| File | Purpose |
|------|---------|
| `enhanced_regime_classifier.py` | 4-regime classifier: 6-component FCI + Markov switching + persistence scoring (1,766 lines) |
| `regime_classifier.py` | Lightweight regime classifier (304 lines) |
| `regime_transition_model.py` | Hamilton filter Markov switching model |
| `macro_data_fetcher_v2.py` | Multi-source macro data fetcher (FRED, RBI DBIE, NSE, MOSPI, NSDL, Trading Economics) |
| `macro_data_scheduler.py` | Scheduled updates (weekly Sunday cron) |
| `macro_data_sheets_v2.py` | Google Sheets integration — 13 tabs |
| `macro_report_generator.py` | Weekly markdown report with 6 charts |
| `fred_api_service.py` | FRED API client (oil, USD, rates) |

## Data sources

| Source | Indicators |
|--------|-----------|
| FRED | WTI crude, DXY, US 10Y, Fed funds |
| RBI DBIE | Repo rate, CPI, IIP, GDP, FPI flows |
| MOSPI | CPI, GDP, IIP |
| NSE | Nifty 50, 15 sector indices, India VIX |
| NSDL | FPI equity/debt flows |
| Trading Economics | PMI |

## Regime model

| Regime | Conditions | Allocation Signal |
|--------|-----------|-------------------|
| Growth-Disinflation | High growth, falling inflation | Risk-on (70% equity) |
| Growth-Inflation | High growth, rising inflation | Equity favorable (50% equity) |
| Stagnation-Disinflation | Low growth, falling inflation | Defensive (30% equity) |
| Stagflation | Low growth, rising inflation | Risk-off (20% equity) |

3-tier ensemble: FCI + output gap + inflation z-score → softmax → Markov switching → persistence prior.

## Architecture fit

```
Macro Pipeline (THIS)          Engine API                Portal
Weekly cron on EC2       →     GET /engine/regime  →     /macro page
Google Sheets (13 tabs)        GET /engine/macro         GaugesRow, RegimeHero
```

Currently standalone. Target: expose as API consumed by Engine and Portal.

## Next steps

- [ ] Expose regime classification as Engine API endpoint (currently filesystem-based)
- [ ] Wire current regime into fund analysis (engines don't consume it today)
- [ ] Build /macro page in production portal using pipeline data
- [ ] Add regime-conditional VaR/CVaR to risk metrics engine
- [ ] Automate sector performance integration (15 NSE sector indices)
- [ ] Add middleware for weekly report distribution (email, Slack)

## Status

Extracted May 2026. Production-grade data pipeline, weekly cadence. The output is live; the integration into Engine + Portal is the gap.
