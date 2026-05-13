# India Macro Regime Tracker for Foreign Investors – Architecture & Multi‑Agent Plan (Updated with Tier 1 Backend Status)

## 1. Context and Scope

This document defines the architecture, responsibilities, and implementation plan for the **India Macro Regime Tracker for Foreign Investors**. It is written to align with the existing **Multi‑Agent Collaboration Rules (/Users/akshayrandeva/Treso/MULTI_AGENT_COLLABORATION_RULES.md)** and **Feature Requests (/Users/akshayrandeva/Treso/FEATURE_REQUESTS.md)** process used across Treso Analytics.

The goal is to give foreign allocators, macro funds, and EM portfolio managers a concise, actionable view of India’s macro regime, policy stance, external vulnerabilities (including oil), and capital flows, so that their India exposure is aligned with the prevailing regime rather than fighting it.

---

## 2. Why Oil Belongs in the India Regime Engine

India imports roughly 85–90% of its crude oil consumption, making oil the single largest external terms‑of‑trade shock for growth, inflation, current account deficit (CAD) and the rupee.
Higher crude prices have been repeatedly shown to widen the CAD, push CPI and WPI inflation higher, and put pressure on INR, especially during global stress episodes.
Given this structural dependence, **oil must be a first‑class input** to the India macro regime engine, not just a hidden driver inside CPI or FX.

**Design choice:**

- Treat oil as a **shock factor** feeding both the **inflation leg** and the **Financial Conditions Index (FCI)**.
- Do *not* create a separate “oil regime”; instead, let high and rising oil tilt probabilities away from Goldilocks and towards Growth‑Inflation or Stagflation regimes.

---

## 3. What We Are Building

### 3.1 Product Definition

**Working name:** India Macro Regime Tracker for Foreign Investors

**Primary users:**

- Global macro funds and EM/Asia ex‑Japan PMs
- Global multi‑asset allocators with India as a satellite allocation
- UHNW / family offices in GCC and elsewhere allocating to India

**Core questions the product answers:**

- What macro regime is India currently in? (Growth‑Disinflation, Growth‑Inflation, Stagnation‑Disinflation, Stagflation)
- How persistent is the current regime, and what is the probability distribution of regimes over the next 3–12 months?
- Are domestic financial conditions, oil and FX currently a **tailwind or headwind** for foreign capital into India?
- How are FPI flows, sectors, and factors responding to the regime?

### 3.2 System Components

The system has four layers:

1. **Data Pipeline Layer (Data Pipeline Agent)**
   - Scrapes RBI, MOSPI, NSE, oil prices and related data using `EnhancedMacroDataFetcher` and friends.
   - Normalises and stores the data (JSON and/or time‑series DB).
   - Emits a stable **macro snapshot** object consumed by the backend.

2. **Backend Layer (Backend Agent)**
   - Enhanced Regime Classifier: smooth growth/inflation, compute FCI (including oil), compute regime probabilities.
   - Regime Transition Model: construct transition matrices and forward regime distributions.
   - API layer: expose `/api/regime/current` and `/api/dashboard/state` for the Integration/Frontend Agent.

3. **Frontend Layer (Frontend Agent / Integration Agent)**
   - Dyad dashboard renders the India macro state from backend APIs.
   - Components: Regime card, Rates & FCI (with oil), FX & flows, sectors & factors, risk/policy panel, and history strip.

4. **Reporting & Alerts Layer (Backend + Frontend)**
   - Weekly India Macro Snapshot and monthly Deep Dive for foreign allocators.
   - Event‑driven alerts on regime changes, FCI extremes, oil shocks, or major FPI flow reversals.

---

## 4. Multi‑Agent Collaboration Model

This project follows the existing **Multi‑Agent Collaboration Rules**.

### 4.1 Backend Agent

**Role:** Backend Owner for all analytics, including the India Macro Regime Tracker.

**Responsibilities:**

- Own and maintain backend code in the analytics repo (`*_engine.py`, `*_collector.py`, `main.py`, etc.).
- Implement and version API endpoints:
  - `GET /api/regime/current`
  - `GET /api/dashboard/state`
- Integrate macro snapshots from the Data Pipeline Agent into the regime classifier and transition model.
- Maintain `API_CONTRACTS.md` and `SYSTEM_ARCHITECTURE.md` for macro regime architecture.
- Ensure no breaking changes to existing contracts; if needed, add versioned endpoints (e.g. `/api/regime/current_v2`).

### 4.2 Data Pipeline Agent

**Role:** Owner of macro data acquisition and storage.

**Responsibilities:**

- Operate and extend `macro_data_fetcher_v2-3.py` and `macro_data_sheets_v2-4.py` to fetch and persist:
  - RBI rates, G‑sec yields, 91‑day T‑bill
  - MOSPI inflation and growth
  - NSE index and sector data, India VIX
  - FX data
  - Oil data (Brent/WTI)
- Keep Google Sheets macro tabs (`RBI_Data`, `Inflation_Data`, `Growth_Data`, `Market_Data`, `Exchange_Rates`, `Oil_Brent_Monthly`, `Oil_WTI_Monthly`, etc.) in sync.
- Provide a stable, documented **macro snapshot** structure consumed by the backend.
- Track and log data quality metrics, fallbacks and errors.

### 4.3 Frontend Agent (Integration / Dyad)

**Role:** Owner of Dyad/React front‑end and workflow‑platform integration.

**Responsibilities:**

- Consume backend APIs only; *never* modify backend code.
- Implement TypeScript types and API wrappers that exactly match `API_CONTRACTS.md`.
- Build the India macro dashboard and UI components.
- Raise new requirements via `FEATURE_REQUESTS-2.md` using the standard template.

---

## 5. Backend Status and Tier Plan

### 5.1 Capability Matrix

From the synthesis, current regime engine capabilities vs literature requirements:

**Already implemented (Tier 1 complete):**

- 2×2 Growth×Inflation regime matrix
- FCI with four components
- Softmax regime probabilities (probabilistic classification)
- Z‑score normalisation vs historical distributions
- K‑means clustering (statistical regime detection)
- 3‑month rolling mean smoothing
- **Tier 1 enhancements now implemented:**
  - **Regime persistence / transition buffer**: persistence prior of ~0.85–0.90 applied to softmax probabilities, reflecting Indian regime persistence.
  - **Output gap** instead of raw GDP: GDP growth minus rolling trend (e.g. 10‑year average) as the growth signal.
  - **Yield curve slope** (10Y minus 3M/91‑day T‑bill) added as an FCI component and/or leading indicator.

**Implemented (Tier 1, 2 & 3 — COMPLETE May 8, 2026):**

- Regime persistence / transition buffer with significance classification
- Output gap (GDP minus rolling 10-year trend) as growth signal
- Yield curve slope (10Y−3M) as FCI component
- **Tier 2:** Markov switching (Hamilton filter, 2-state, EM refinement, built from scratch)
- **Tier 2:** Time-varying FCI weights (stress-adaptive: VIX>20, slope<0.5, |oil_z|>1.5, credit_spread>1.5)
- **Tier 2:** Leading indicators composite (yield curve, VIX, credit spread, repo; pending FPI/credit growth)
- **Tier 3:** Regime-conditional VaR/CVaR (INR + USD, empirical + parametric pooled vol)
- **Tier 3:** Full ensemble classifier (softmax 0.50 + markov 0.30 + rule-based 0.20)
- **Tier 3:** Multi-signal escalation alerts (/api/alerts with 2+ RED → CRITICAL)
- Monthly deep-dive report generator with regime evolution, oil shock, policy stance, positioning matrix

### 5.2 Tiers (with Tier 1 marked as complete)

**Tier 1 — Done (high impact, low effort, uses existing data)**

1. **Regime persistence / transition buffer**
   - Implemented in the regime classifier via a persistence prior on the current regime when updating probabilities.
2. **Output gap as growth signal**
   - Implemented using GDP minus rolling trend (e.g. 10‑year average), derived from `macro_data_2000_2026_100pct_real.json`.
3. **Yield curve slope as FCI/leading component**
   - Implemented using 10Y–3M slope (requires 91‑day T‑bill from RBI via `macro_data_fetcher_v2` and existing 10Y G‑sec).

**Tier 2 — COMPLETE May 8, 2026**

4. **Markov Switching with Hamilton filter** ✅
   - 2-state Hamilton filter built from scratch (numpy/scipy, no statsmodels dependency).
   - 5-iteration EM refinement for transition probability estimation.
   - Runs in parallel with softmax; ensemble-blended via outer product of growth × inflation states.

5. **Time‑varying FCI weights** ✅
   - Stress multipliers applied when VIX>20, yield curve slope<0.5, |oil_z|>1.5, or credit_spread_norm>1.5.
   - Weights renormalised after stress adjustment.

6. **Leading Indicators Composite** ✅
   - Composite from yield curve slope, VIX, credit spread, repo stance.
   - FPI flows now wired via NSDL scraper + CSV fallback.

**Tier 3 — COMPLETE May 8, 2026**

7. **Regime‑conditional VaR/CVaR** ✅
   - Empirical where n≥10 months observed; parametric with pooled volatility for sparse regimes (<10 months).
   - Both INR and USD (FX-adjusted) metrics for each regime.
   - 95% and 99% VaR/CVaR, drawdown, volatility, Sharpe, positive months stats.

8. **Full ensemble classifier** ✅
   - 3-way blend: softmax (0.50) + Markov Hamilton (0.30) + rule-based (0.20).

9. **Early‑warning dashboard with multi‑signal triggers** ✅
   - `GET /api/alerts` with 6 trigger types: OIL_SHOCK, FCI_TIGHTENING, POLICY_DIVERGENCE, YIELD_INVERSION, CPI_REGIME_CHANGE, GROWTH_SLOWDOWN.
   - Multi-signal escalation: 2+ RED → HIGH (MULTI_SIGNAL_WARNING), 3+ RED → CRITICAL (MULTI_SIGNAL_CRITICAL).
   - Composite alert with recommended actions.

---

## 6. Data Pipeline Project (Data Pipeline Agent)

### 6.1 Current Pipeline (Updated May 8, 2026)

**Core files (in `/macro_pipeline/`):**

| File | Purpose |
|------|---------|
| `fred_api_service.py` | Single FRED client for all global series (8 series: Brent, WTI, USDINR, DGS10, FEDFUNDS, DTWEXBGS, VIXCLS, T10YIE) |
| `macro_data_fetcher_v2.py` | Fetches all macro data — RBI/MOSPI/NSE (India) + FRED (global) + NSDL (FPI) + Trading Economics (PMI) |
| `macro_data_sheets_v2.py` | Pushes snapshots into 13 Google Sheets tabs (batch-optimised) |
| `macro_data_scheduler.py` | 7-step weekly pipeline: fetch → classify → sheets → transitions → forward probs → event alerts → report |
| `enhanced_regime_classifier.py` | FCI (6 components incl. oil) + softmax + z-scores + ensemble |
| `regime_transition_model.py` | Markov transition matrix + forward regime distributions |
| `macro_report_generator.py` | Weekly regime report with charts |

**Data sources and provenance:**

| Domain | Source | Provenance | Frequency |
|--------|--------|------------|-----------|
| RBI (repo, reverse repo, 10Y, T-bill 91D, MSF, bank rate) | RBI DBIE API | India — RBI | Weekly |
| Inflation (CPI, WPI, core, food, fuel) | MOSPI web scrape | India — MOSPI | Monthly |
| Growth (GDP, IIP, manufacturing, services) | MOSPI web scrape | India — MOSPI | Monthly |
| Market (Nifty 50, DMA, cyclicals/defensives, VIX) | NSE index-sdk + CSV SMA | India — NSE | Daily |
| FX (USDINR, 3M change) | FRED DEXINUS | Global — FRED | Daily |
| Oil (Brent, WTI, 3M change) | FRED DCOILBRENTEU/DCOILWTICO | Global — FRED | Daily |
| FPI flows (equity, debt, total, 3M/12M) | NSDL FPI Daily Report + CSV fallback | India — NSDL | Daily |
| Sector indices (15 NSE sectors) | NSE index-sdk (live) + Yahoo Finance (historical) | India — NSE | Daily |
| PMI (manufacturing, services, composite) | Trading Economics scrape + Investing.com API | Global — S&P Global | Monthly |
| US macro (DGS10, FEDFUNDS, DTWEXBGS, VIXCLS, T10YIE) | FRED | Global — FRED | Daily |

**Google Sheets tabs (13):**
`RBI_Data`, `Inflation_Data`, `Growth_Data`, `Market_Data`, `Exchange_Rates`, `Oil_Brent_Monthly`, `Oil_WTI_Monthly`, `FPI_Flows`, `Sector_Indices`, `PMI_Data`, `US_Macro`, `Regime_Classification`, `Audit_Log`

**Historical data (14 domains in `historical_macro_data/`):**
`gdp`, `cpi`, `iip`, `rbi`, `inflation`, `growth`, `nse`, `oil` (317 records, 2000–2026), `fx` (317 records, 2000–2026), `fpi_flows` (195 records, 2010–2026), `sector_indices` (225 records, 2007–2026, 12/15 indices), `pmi` (3 records, 2026 only — free API limited), `us_macro` (317 records, 2000–2026), `regimes` (240 records, 2006–2026)

Merged master: `macro_data_2000_2026_100pct_real.json` — 14 domains, 3,590 records, all with Source provenance.

**Scheduler (7-step pipeline):**
1. Fetch macro data (all 10 domains)
2. Classify regime (enhanced: FCI + softmax + z-scores)
3. Update Google Sheets (13 tabs, batch)
4. Check regime transitions (vs previous from sheets)
5. Compute forward probabilities (Markov transition model, 3m/6m/12m)
6. Check event alerts (oil shock, FPI outflow, high VIX, flat curve)
7. Generate weekly regime report with charts

**Sunday cron:** `sunday_macro_update.sh` (24 lines) — single `MacroDataScheduler().run_full_pipeline()` call.

### 6.2 Oil Data Integration — COMPLETE ✅

- Oil fetcher uses FRED `FredAPIClient.get_latest('DCOILBRENTEU')` and `get_3m_ago()` for real 3-month change (was: broken API key placeholder).
- `fetch_all_macro_data()` includes `oil` key with `brent_usd`, `wti_usd`, `brent_3m_change_pct`, `wti_3m_change_pct`, `data_source='fred.stlouisfed.org'`.
- `macro_data_sheets_v2.py` writes to `Oil_Brent_Monthly` and `Oil_WTI_Monthly` tabs.
- Oil z-score wired into FCI (weight 0.10) and softmax regime classifier (tilt toward Growth-Inflation/Stagflation when oil >$100).

### 6.3 Macro Snapshot Contract (Current — May 8, 2026)

Stable macro snapshot emitted by `EnhancedMacroDataFetcher.fetch_all_macro_data()`:

```json
{
  "fetch_date": "2026-05-08",
  "fetch_timestamp": "2026-05-08T17:00:00Z",
  "rbi": {
    "date": "2026-05-08", "repo_rate": 6.5, "reverse_repo_rate": 3.35,
    "gsec_10y": 7.1, "msf_rate": 6.75, "bank_rate": 5.5,
    "tbill_91d": 6.35, "data_source": "rbi.org.in", "fetch_timestamp": "..."
  },
  "mospi_inflation": {
    "date": "2026-04-01", "cpi": 4.3, "wpi": 2.1, "cpi_trend": "declining",
    "core_cpi": 4.1, "data_source": "mospi.gov.in", "fetch_timestamp": "..."
  },
  "mospi_growth": {
    "date": "2026-03-01", "gdp_growth": 7.1, "iip": 6.2,
    "manufacturing": 5.8, "services": 7.5, "data_source": "mospi.gov.in", "fetch_timestamp": "..."
  },
  "nse": {
    "date": "2026-05-08", "nifty_50": 25845.0, "nifty_50dma": 25278.5,
    "nifty_200dma": 25304.2, "cyclicals_index": 24300.0, "defensives_index": 27200.0,
    "vix": 14.2, "market_trend": "bullish", "data_source": "nseindia.com", "fetch_timestamp": "..."
  },
  "fx": {
    "date": "2026-05-08", "usdinr": 94.9, "usdinr_3m_change_pct": 2.3,
    "data_source": "fred.stlouisfed.org (DEXINUS)", "fetch_timestamp": "..."
  },
  "oil": {
    "date": "2026-05-08", "brent_usd": 118.26, "wti_usd": 109.76,
    "brent_3m_change_pct": 5.2, "wti_3m_change_pct": 4.8,
    "data_source": "fred.stlouisfed.org (DCOILBRENTEU/DCOILWTICO)", "fetch_timestamp": "..."
  },
  "fpi_flows": {
    "date": "2026-05-07", "fpi_equity_flow": -489.73, "fpi_debt_flow": 511.27,
    "fpi_total_flow": 21.54, "fpi_equity_flow_3m": -2800.0, "fpi_debt_flow_3m": 1200.0,
    "data_source": "nsdl.co.in", "fetch_timestamp": "..."
  },
  "sector_indices": {
    "date": "2026-05-08", "data_source": "nseindia.com (index-sdk)",
    "sectors": { "NIFTY BANK": 54200.0, "NIFTY IT": 39200.0, ... }  /* 15 sectors */
  },
  "pmi": {
    "date": "2026-05-01", "pmi_manufacturing": 54.7, "pmi_services": 58.8,
    "pmi_composite": 56.3, "data_source": "tradingeconomics.com", "fetch_timestamp": "..."
  },
  "us_macro": {
    "date": "2026-05-08", "dgs10": 4.36, "fedfunds": 3.64,
    "dtwbgs": 102.5, "vixcls": 16.9, "t10yie": 2.45,
    "data_source": "fred.stlouisfed.org", "fetch_timestamp": "..."
  },
  "data_quality": { "errors": [], "warnings": [], "fallbacks_used": [] }
}
```

The Backend Agent reads this snapshot via `fetch_all_macro_data()`, not individual upstream sources.

---

## 7. Backend Project (Backend Agent)

### 7.1 Extend Regime Engine with Oil

1. **Add oil to FCI**
   - In the Enhanced Regime Classifier, extend the FCI calculation to include an oil shock component (e.g. oil price z‑score and/or % change).
   - Assign a weight that represents external cost stress and renormalise FCI weights.

2. **Tilt regime probabilities with oil**
   - Add an adjustment factor to the softmax logits based on `oil_z`:
     - When oil is significantly above trend and rising, increase the logits for Growth‑Inflation/Stagflation regimes relative to Goldilocks.
   - Keep the adjustment modest so oil does not dominate macro data.

3. **Expose oil metrics in outputs**
   - Include `oil` and `oil_z` fields in `/api/regime/current` and `/api/dashboard/state`.

### 7.2 `/api/regime/current` JSON Contract

**Method:** `GET`

**Path:** `/api/regime/current`

**Purpose:** Return the latest India macro regime classification and key drivers for a single date.

**Response schema (example):**

```json
{
  "date": "2026-05-08",
  "regime": "Growth-Disinflation",
  "regime_code": "GROWTH_DISINFLATION",
  "color": "Green",
  "confidence": 0.78,
  "probabilities": {
    "Growth-Disinflation": 0.78,
    "Growth-Inflation": 0.10,
    "Stagnation-Disinflation": 0.07,
    "Stagflation": 0.05
  },
  "growth_signal": {
    "gdp_growth": 7.1,
    "trend_growth": 5.6,
    "output_gap": 1.5,
    "growth_z": 0.8,
    "pmi_composite": 54.2
  },
  "inflation_signal": {
    "cpi": 4.3,
    "core_cpi": 4.1,
    "inflation_z": -0.3
  },
  "fci": {
    "fci_signal": -0.4,
    "repo_rate": 6.5,
    "gsec_10y": 7.1,
    "yield_curve_slope": 0.8,
    "credit_spread": 1.5,
    "vix": 14.2,
    "oil_z": 1.2
  },
  "oil": {
    "brent_usd": 96.2,
    "brent_3m_change_pct": 12.4
  },
  "fx": {
    "usdinr": 92.4,
    "usdinr_3m_change_pct": 4.5
  },
  "transition": {
    "previous_regime": "Growth-Inflation",
    "transition_detected": true,
    "transition_type": "Improvement - disinflation",
    "significance": "MEDIUM"
  },
  "classification_timestamp": "2026-05-08T08:30:00Z",
  "method": "hybrid_v2"
}
```

### 7.3 `/api/dashboard/state` JSON Contract

**Method:** `GET`

**Path:** `/api/dashboard/state`

**Purpose:** Provide all data required for the Dyad India macro dashboard in a single call.

**Response schema (example):**

```json
{
  "as_of": "2026-05-08",
  "regime_card": {
    "regime": "Growth-Disinflation",
    "regime_code": "GROWTH_DISINFLATION",
    "color": "Green",
    "confidence": 0.78,
    "probabilities": {
      "Growth-Disinflation": 0.78,
      "Growth-Inflation": 0.10,
      "Stagnation-Disinflation": 0.07,
      "Stagflation": 0.05
    },
    "forward_probabilities": {
      "3m": {
        "Growth-Disinflation": 0.62,
        "Growth-Inflation": 0.18,
        "Stagnation-Disinflation": 0.12,
        "Stagflation": 0.08
      },
      "6m": {
        "Growth-Disinflation": 0.50,
        "Growth-Inflation": 0.22,
        "Stagnation-Disinflation": 0.16,
        "Stagflation": 0.12
      },
      "12m": {
        "Growth-Disinflation": 0.42,
        "Growth-Inflation": 0.25,
        "Stagnation-Disinflation": 0.18,
        "Stagflation": 0.15
      }
    }
  },
  "rates_and_fci": {
    "repo_rate": 6.5,
    "gsec_10y": 7.1,
    "tbill_91d": 6.3,
    "yield_curve_slope": 0.8,
    "fci_signal": -0.4,
    "fci_z": -0.6,
    "oil_brent_usd": 96.2,
    "oil_z": 1.2
  },
  "fx_and_flows": {
    "usdinr": 92.4,
    "usdinr_3m_change_pct": 4.5,
    "fpi_equity_1d": -350.0,
    "fpi_equity_20d_sum": -2800.0,
    "fpi_debt_1d": 120.0,
    "fpi_debt_20d_sum": 950.0,
    "flow_zscore_20d": -1.1
  },
  "equity_and_sectors": {
    "nifty_50": 24500.0,
    "nifty_vs_em_1y_rel": 8.3,
    "sectors": [
      { "name": "Financials", "rel_perf_3m": 4.2, "stance": "overweight" },
      { "name": "IT", "rel_perf_3m": -2.1, "stance": "neutral" },
      { "name": "Energy", "rel_perf_3m": 5.7, "stance": "overweight" }
    ],
    "factors": [
      { "name": "Quality", "rel_perf_3m": 2.3 },
      { "name": "Value", "rel_perf_3m": 1.1 },
      { "name": "Growth", "rel_perf_3m": -0.5 }
    ]
  },
  "risk_and_policy": {
    "valuation_flag": "AMBER",
    "liquidity_flag": "GREEN",
    "policy_flag": "GREEN",
    "upcoming_events": [
      { "date": "2026-06-07", "type": "RBI_MPC", "description": "RBI policy decision" },
      { "date": "2026-06-12", "type": "DATA", "description": "CPI release" }
    ]
  },
  "history": {
    "regimes": [
      { "date": "2023-01-31", "regime": "Growth-Disinflation" },
      { "date": "2023-02-28", "regime": "Growth-Inflation" }
    ],
    "fci": [
      { "date": "2023-01-31", "fci_signal": -0.8 },
      { "date": "2023-02-28", "fci_signal": -0.6 }
    ],
    "oil_brent": [
      { "date": "2023-01-31", "brent_usd": 82.1 },
      { "date": "2023-02-28", "brent_usd": 88.3 }
    ],
    "fpi_flows": [
      { "date": "2023-01-31", "equity": 250.0, "debt": 30.0 },
      { "date": "2023-02-28", "equity": -150.0, "debt": 40.0 }
    ]
  }
}
```

The Backend Agent maintains this contract in `API_CONTRACTS.md` and adds versioned variants as needed.

---

## 8. Frontend Project (Frontend Agent)

Using `/api/dashboard/state`, the Frontend Agent implements the Dyad dashboard with:

- **Top row:**
  - Regime Card (current + forward probabilities)
  - Rates & FCI & Oil Card
  - FX & Flows Card
- **Middle row:**
  - Equity & Sectors Panel
  - Risk & Policy Panel
- **Bottom row:**
  - History strip for regimes, FCI, oil and FPI flows

Tasks:

- Define TypeScript types matching the JSON contracts.
- Implement API wrappers (e.g. `getCurrentRegime()`, `getDashboardState()`).
- Build UI components and charts wired to these contracts.
- Handle loading/error states and missing data gracefully.

---

## 9. Reporting & Alerts Layer

With the dashboard and APIs in place:

- **Weekly report:**
  - Auto‑generate a short India Macro Snapshot highlighting regime, FCI & oil, FX & flows, sector/factor rotations.
- **Monthly report:**
  - Deep dive into regime evolution, external shocks (especially oil), policy moves, and positioning.
- **Event‑driven alerts:**
  - Regime changes, FCI extremes, oil z‑score spikes, FPI flow reversals.
  - Implemented either via a separate `/api/alerts` endpoint or as part of `/api/dashboard/state` flags.

This layered approach keeps India’s importance in a global portfolio proportional (not over‑reporting), while still surfacing regime‑critical information when it matters most.

---

## 10. Implementation Status & Remaining Work (May 8, 2026)

### 10.1 What’s Done

| Layer | Component | Status |
|-------|-----------|--------|
| Data | FRED API service (8 series) | ✅ Live |
| Data | RBI/MOSPI/NSE scrapers (India) | ✅ Live |
| Data | FX fetcher (USDINR from FRED) | ✅ Live |
| Data | Oil fetcher (Brent/WTI from FRED) | ✅ Live |
| Data | FPI flows fetcher (NSDL + CSV fallback) | ✅ Live |
| Data | PMI fetcher (Trading Economics + Investing.com) | ✅ Live |
| Data | US macro fetcher (5 FRED series) | ✅ Live |
| Data | Sector indices fetcher (15 NSE sectors) | ✅ Live |
| Data | T-bill 91D (4-tier fallback) | ✅ Live |
| Data | Nifty DMA (real historical CSV, not synthetic) | ✅ Live |
| Data | Google Sheets (13 tabs, batch-optimised) | ✅ Wired |
| Data | Historical data (14 domains, 3,590 records) | ✅ Built |
| Data | 7-step scheduler (fetch→classify→sheets→transitions→forward→alerts→report) | ✅ Coded |
| Data | Sunday cron shell script (single call) | ✅ Simplified |
| Backend | FCI with 6 components (incl. oil) | ✅ Live |
| Backend | Softmax + z-scores + ensemble classifier | ✅ Live |
| Backend | Oil tilt in regime probabilities | ✅ Live |
| Backend | Markov transition model + forward distributions | ✅ Live |
| Backend | `/api/regime/current` endpoint | ✅ Live |
| Backend | `/api/dashboard/state` endpoint | ✅ Live |
| Backend | `/api/alerts` endpoint (6 trigger types) | ✅ Live |
| Backend | Forward probabilities via RegimeTransitionModel (not hardcoded 0.85) | ✅ Wired |
| Frontend | Dyad dashboard components (Regime card, FCI, FX, sectors, risk) | ✅ Built |
| Reports | Weekly macro snapshot generator | ✅ Coded |

### 10.2 What’s Left

**Operational (needs attention):**

| # | Item | Priority | Detail |
|---|------|----------|--------|
| 1 | **NSDL live scraper** | ✅ Done | `Latest.aspx` works directly from Indian VM (no Browserbase needed). Rowspan-aware parser extracts Equity + sum of all Debt-* sub-categories from Stock Exchange route. Verified 2026-05-10: Equity=-69.31, Debt=-260.6. `FPI_DailyReport.aspx` is universally WAF-blocked. |
| 2 | **Sunday cron on VM** | ✅ Done | `30 12 * * 0` (Sun 6 PM IST). `/home/ubuntu/clawd/treso_analytics/sunday_macro_update.sh`. PYTHONPATH set. Logs: `/home/ubuntu/clawd/logs/treso_analytics/`. |
| 3 | **Google Sheets populated** | ✅ Done | Full pipeline run 2026-05-09: all 13 tabs wrote fresh rows. Spreadsheet ID: `10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU`. |
| 4 | **VM pipeline sync** | ✅ Done | All v2 files rsync’d. Dependencies installed: scipy, scikit-learn, schedule, browserbase, matplotlib. |

**Historical data gaps (non-blocking):**

| # | Item | Priority | Detail |
|---|------|----------|--------|
| 5 | **PMI historical coverage** | LOW | 3 months (Mar-May 2026). Free Investing.com API limits to ~4 releases. Accumulates weekly via sheets tab. |
| 6 | **Sector indices historical gaps** | LOW | 12/15 from Yahoo Finance. 3 missing (Healthcare, Consumer Durables, Oil & Gas). Live NSE allIndices API covers all 15. |

**Resolved (was in previous What’s Left):**

| # | Item | Resolution |
|---|------|------------|
| 7 | **NSE sectors 0/15 on VM** | ✅ Fixed — `index-sdk` API removed by NSE. Switched to `allIndices` API (single call, 15/15). |
| 8 | **Hardcoded local path in scheduler** | ✅ Fixed — `script_dir` now auto-detected via `os.path.dirname(__file__)`. |
| 9 | **pandas ‘M’ deprecation** | ✅ Fixed — `resample(‘ME’)` in all build scripts. |
| 10 | **FRED intermittent 500s** | ✅ Fixed — 3-retry loop in `fred_api_service.py:_fetch()`. |
| 11 | **Indian govt sites geo-block** | KNOWN — NSE, MOSPI, NSDL, RBI-DBIE block non-Indian IPs. Pipeline runs from AWS Mumbai VM. |

### 10.3 Verification Commands

```bash
# VM pipeline test (India IP — unblocks NSE, MOSPI, NSDL, RBI)
ssh aws-clawdbot ‘cd /home/ubuntu/clawd/treso_analytics && python3 -c "
from macro_data_scheduler import MacroDataScheduler
print(\"Pipeline:\", \"OK\" if MacroDataScheduler().run_full_pipeline() else \"FAIL\")
"’

# Verify cron
ssh aws-clawdbot "crontab -l | grep sunday_macro_update"

# Check latest log
ssh aws-clawdbot "ls -lt /home/ubuntu/clawd/logs/treso_analytics/ | head -5"

# API contract check (local)
curl -s http://localhost:8000/api/dashboard/state | python3 -c "
import sys,json; d=json.load(sys.stdin)
print(‘Keys:’, sorted(d.keys()))
print(‘FX:’, d.get(‘fx_and_flows’,{}).get(‘usdinr’))
print(‘Oil:’, d.get(‘rates_and_fci’,{}).get(‘oil_brent_usd’))
print(‘Forward 3m:’, d.get(‘regime_card’,{}).get(‘forward_probabilities’,{}).get(‘3m’))
"
```