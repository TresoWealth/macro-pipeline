# Treso Analytics — Multi-Agent Project Plan (v2, May 2026)

**Status**: Phase 0-2 Backend COMPLETE | Phase 1 Data Pipeline COMPLETE | Phase 3-5 Frontend PENDING
**Last Updated**: May 12, 2026

## 0. Context and Agents

**Scope**: Extend Treso Analytics with active/passive fund performance analytics, PE/VC liquidity tracking, and IPO activity panels — built on top of the existing macro pipeline infrastructure.

**Agents**:
- **Data Pipeline Agent** — owns data ingestion, cleaning, and storage. Maintains the existing macro pipeline (14 domains, 13 Sheets tabs, weekly cron).
- **Backend Engineer Agent** — owns `/Treso/code/treso_analytics`, APIs, engines, contracts, and tests.
- **Frontend Engineer Agent** — owns `/Treso/code/treso-workflow-platform` integration, TS wrappers, and Dyad UI.

6–8 week project; phases can overlap where dependencies allow.

---

## 0.1 Existing Macro Pipeline (Starting Point)

Not greenfield. The macro pipeline is production-live on AWS Mumbai VM:

| What | Detail |
|------|--------|
| Scheduler | 7-step weekly (fetch→classify→sheets→transitions→forward→alerts→report) |
| Cron | `30 12 * * 0` (Sun 6 PM IST) on `aws-clawdbot` (13.200.124.101) |
| Data domains | 14 (oil, fx, us_macro, rbi, inflation, growth, nse, fpi_flows, sector_indices, pmi, regimes, gdp, cpi, iip) |
| Google Sheets | 13 tabs in spreadsheet `10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU` |
| Live fetchers | FRED (8 series), NSDL (FPI flows via Latest.aspx), NSE (allIndices API, 15/15 sectors), RBI, MOSPI, Trading Economics (PMI) |
| Classifier | EnhancedRegimeClassifier v2.1: FCI (6-comp) + softmax + z-scores + Markov ensemble → 4 regimes |
| Regime labels | `Growth-Disinflation`, `Growth-Inflation`, `Stagnation-Disinflation`, `Stagflation` |
| Historical | 3,590 records, 2000–2026, 14-domain master JSON |
| Docs | `README.md` (operational ref), `india_macro_regime_tracker.md` (design/status/API contracts) |

### Overlap Map: Existing → New Project

| New project needs | Already covered by macro pipeline? | Action |
|-------------------|------------------------------------|--------|
| `macro_regime` (current regime label) | Yes — `EnhancedRegimeClassifier` | Direct consume from existing Sheets tab `Regime_Classification` |
| Nifty 50 level | Yes — NSE fetcher | Already in Sheets tab `Market_Data` |
| Sector index performance | Yes — 15/15 sectors via `allIndices` | Already in Sheets tab `Sector_Indices` |
| FPI equity/debt flows | Yes — NSDL `Latest.aspx` | Already in Sheets tab `FPI_Flows` |
| USDINR, oil, US macro | Yes — FRED API | Already in Sheets tabs `Exchange_Rates`, `Oil_*`, `US_Macro` |
| PMI | Yes — Trading Economics | Already in Sheets tab `PMI_Data` |
| Active vs passive fund performance | **No** | New: SPIVA India + Morningstar US/Europe barometers |
| India mutual fund universe (schemes, categories, NAV) | **No** | New: mfapi.in (free REST API — 10,000+ schemes, NAV history, scheme categories including Index/ETF vs active) |
| PE/VC investment & exit activity | **No** | New: Bain Global PE + India VC reports |
| IPO counts & proceeds (India + global) | **No** (partial — NSE report exists but not fetched) | New: EY IPO Trends + NSE annual reports |
| AMFI category flows & AUM | **No** | New: AMFI monthly Excel |

**Bottom line**: The macro environment data (regime, rates, FX, flows, PMI) is already automated. The new project adds fund-structural, PE/VC, and IPO data that doesn't overlap.

**Morningstar India note**: No India Active/Passive Barometer exists. Morningstar India publishes fund-level data through their platform (paid) but no recurring barometer report. A 2017 one-off article found Indian active funds significantly outperformed benchmarks (unlike US/Europe) — the India active/passive story is structurally different.

---

## 1. Phase 0 — Alignment, Inventory, and Architecture (Week 0–1)

### Data Pipeline Agent

- [ ] Audit existing macro pipeline sources, schemas, and refresh cadences against the overlap map above. Confirm which Sheets tabs the backend should read for `macro_regime`, `nifty`, `fpi_flows`, `sector_indices`.
- [ ] Produce the canonical data model for new tables below (Section 1.1), including primary keys, grain, and which columns go into which Sheets tab.
- [ ] Document data quality SLAs per source (Section 1.2).
- [ ] Define `data_freshness` metadata contract per source: `as_of` date, `next_expected` date, staleness thresholds (see Section 1.3 below). This contract feeds every API response.

### Backend Engineer Agent

- [ ] Review `API_CONTRACTS.md` vs actual `main.py` endpoints; fix drift.
- [ ] Define new API surfaces under `/api/v1/analytics/` (versioned from day one — see Key Decision #10): active/passive summary, active/passive timeseries, PE liquidity summary, IPO activity summary, India opportunity snapshot.
- [ ] Create shared `schemas/analytics/` directory with one JSON Schema file per endpoint. These are the contract between backend and frontend before any code is written (see Key Decision #11).
- [ ] Align with Data Pipeline on storage: all new data → Google Sheets (same spreadsheet as macro pipeline). Backend reads via existing `GoogleSheetsHelper` / collector pattern. No new DB.
- [ ] Define `data_freshness` response block structure (status, as_of, next_expected, stale_days) — see Section 1.3. Every analytics endpoint returns this alongside data.

### Frontend Engineer Agent

- [ ] Inventory existing Dyad pages: `FundDetail`, `PortfolioManager`, regime dashboard.
- [ ] Draft wireframes for new panels using the specific field names from Section 1.1.
- [ ] Write feature request entries in `FEATURE_REQUESTS.md` with **Expected API contract** blocks.

**Deliverables**: Updated `API_CONTRACTS.md`, `DATA_MODEL.md` (new canonical tables), and UI wireframes.

---

### 1.1 New Data Model: Canonical Tables

All new tables live in the existing macro pipeline Google Sheets spreadsheet. Each source gets its own tab (or tabs).

#### `spiva_india_results`
| Column | Type | Grain |
|--------|------|-------|
| `report_period` | string | e.g. "2025-H1", "2025-H2" |
| `category` | string | e.g. "Large-Cap", "Mid/Small-Cap", "ELSS", "Composite Bond" |
| `horizon_years` | int | 1, 3, 5, 10 |
| `pct_underperformed` | float | % of active funds that underperformed the benchmark |
| `pct_survivors` | float | % of funds that survived the period |
| `benchmark_name` | string | e.g. "S&P BSE 100" |
| `num_funds_start` | int | Funds at start of period |
| `num_funds_end` | int | Funds at end of period |

#### `spiva_global_results` (US, Europe, Asia ex-Japan, etc.)
Same schema as `spiva_india_results` + `region` column.

#### `morningstar_barometer`
| Column | Type | Grain |
|--------|------|-------|
| `report_date` | string | e.g. "2025-12-31" |
| `region` | string | "US", "Europe", "Australia" |
| `category` | string | e.g. "US Equity Large Blend", "International Equity" |
| `horizon_years` | int | 1, 3, 5, 10, 15, 20 |
| `success_rate_equal_wt` | float | % of active funds that survived AND outperformed (equal-weighted) |
| `success_rate_asset_wt` | float | Same, asset-weighted |
| `survivorship_rate` | float | % of funds that survived the period |

#### `pe_activity_global`
| Column | Type | Grain |
|--------|------|-------|
| `year` | int | Calendar year |
| `region` | string | "Global", "North America", "Europe", "Asia-Pacific" |
| `deal_value_usd_bn` | float | Total PE buyout deal value |
| `exit_value_usd_bn` | float | Total PE exit value |
| `fundraising_usd_bn` | float | Total PE capital raised |
| `dry_powder_usd_bn` | float | Uncalled capital |
| `deal_count` | int | Number of buyout deals |
| `exit_count` | int | Number of exits |
| `median_ev_ebitda` | float | Median entry multiple |

#### `pe_activity_india` (from Bain-IVCA India VC Report)
| Column | Type | Grain |
|--------|------|-------|
| `year` | int | Calendar year |
| `vc_investment_usd_bn` | float | Total VC/growth investment |
| `vc_deal_count` | int | Number of VC deals |
| `exit_value_usd_bn` | float | Total VC exit value |
| `exit_channel_ipo_pct` | float | % of exits via IPO |
| `exit_channel_strategic_pct` | float | % via strategic/M&A |
| `exit_channel_secondary_pct` | float | % via secondary |
| `fundraising_usd_bn` | float | VC capital raised |
| `fintech_investment_usd_bn` | float | Fintech sector investment |
| `saas_investment_usd_bn` | float | Software/SaaS sector investment |

#### `ipo_activity`
| Column | Type | Grain |
|--------|------|-------|
| `year` | int | Calendar year (or quarter for EY) |
| `period` | string | "Q1", "Q2", "Q3", "Q4", "FY" |
| `country` | string | "India", "US", "China", "Global" |
| `num_ipos` | int | Number of IPOs |
| `proceeds_usd_bn` | float | Total IPO proceeds |
| `market_share_volume_pct` | float | % of global IPO volume |
| `market_share_proceeds_pct` | float | % of global IPO proceeds |
| `avg_deal_size_usd_m` | float | Average IPO size |

#### `amfi_monthly`
| Column | Type | Grain |
|--------|------|-------|
| `month` | date | Month-end date |
| `category` | string | e.g. "Equity Large Cap", "Debt Short Duration" |
| `category_type` | string | "Equity", "Debt", "Hybrid", "Solution", "Passive" |
| `aum_inr_cr` | float | Month-end AUM |
| `net_inflows_inr_cr` | float | Net inflows/outflows for the month |
| `folio_count` | int | Total investor folios |
| `sip_contributions_inr_cr` | float | SIP book for the month |

#### `mfapi_india_fund_universe` (from mfapi.in)
| Column | Type | Grain |
|--------|------|-------|
| `scheme_code` | int | Unique scheme identifier (PK) |
| `scheme_name` | string | Full scheme name |
| `fund_house` | string | e.g. "HDFC Mutual Fund", "SBI Mutual Fund" |
| `scheme_type` | string | e.g. "Open Ended Schemes" |
| `scheme_category` | string | e.g. "Equity Scheme - Large Cap Fund", "Index Fund", "ETF" |
| `isin_growth` | string | ISIN for growth option |
| `is_active` | bool | Derived: True if scheme_category is NOT "Index Fund"/"ETF" |
| `category_group` | string | Derived: "Large Cap", "Mid Cap", "Small Cap", "Flexi Cap", "ELSS", "Debt", "Hybrid", "Index/ETF", "Other" |
| `latest_nav` | float | Latest available NAV |
| `nav_date` | date | Date of latest NAV |
| `scrape_date` | date | When this row was last refreshed |

---

### 1.2 Data Quality SLAs

| Source | Latency after release | Completeness | Acceptable gaps |
|--------|-----------------------|-------------|-----------------|
| SPIVA India | 1 week | 100% of published categories | Only current + prior 5 scorecards needed for launch |
| Morningstar Barometer | 1 week | 100% of published categories | US + Europe only for launch |
| Bain Global PE | 2 weeks | All major regions | India-specific from separate IVCA report |
| Bain-IVCA India VC | 2 weeks | All published tables | Historical backfill 2018+ |
| EY IPO Trends | 1 week | India + Global from EMEIA section | India data requires extraction from regional commentary; supplement with NSE reports |
| AMFI Monthly | 3 days | All categories | Historical from April 2004 via Excel |
| NSE IPO data | 1 week | Annual summaries | Supplement EY; use NSE annual IPO reports |

---

### 1.3 Data Freshness Contract (shared backend ↔ frontend)

Every analytics API response includes a `data_freshness` block. The backend derives it from metadata written by the Data Pipeline alongside each data write.

```json
{
  "data_freshness": {
    "spiva_india": {"status": "current", "as_of": "2026-03-31", "next_expected": "2026-09-30"},
    "pe_activity_india": {"status": "stale", "as_of": "2025-04-15", "next_expected": "2026-04-15", "stale_days": 391}
  },
  "data": { ... }
}
```

| Field | Type | Meaning |
|-------|------|---------|
| `status` | `current` / `stale` / `missing` | `stale` = past `next_expected` + grace period; `missing` = no data ever ingested |
| `as_of` | date | The date the source data represents (not when it was ingested) |
| `next_expected` | date | When the next edition is expected based on historical release cadence |
| `stale_days` | int | Days since `next_expected` (only present when `status: "stale"`) |

**Staleness thresholds** (days past `next_expected` before flipping `current` → `stale`):

| Source | Grace period | Reasoning |
|--------|-------------|-----------|
| AMFI Monthly | 35 days | Monthly; ~10th of next month + buffer |
| SPIVA India/Global | 90 days | Semi-annual; releases ~Mar and ~Sep |
| Morningstar Barometer | 90 days | Semi-annual |
| Bain Global PE | 120 days | Annual; ~Mar release |
| Bain-IVCA India VC | 120 days | Annual; ~Apr release |
| EY IPO Trends | 45 days | Quarterly |
| mfapi.in | 35 days | Monthly refresh |
| NSE IPO data | 120 days | Annual |

The Data Pipeline writes a `_metadata` row per tab with `as_of` and `next_expected`. The backend reads it, computes `status` and `stale_days` at query time. Partial failures are surfaced per-source — the composite `/india-opportunity/snapshot` endpoint returns 200 with individual `data_freshness` entries marked `stale` or `missing` rather than failing wholesale.

---

## 2. Phase 1 — Data Model and Source Integration (Week 1–3)

### Source Deep Dives

#### SPIVA India Scorecard (S&P Dow Jones Indices)
- **Frequency**: Semi-annual (Mid-Year ~Sep, Year-End ~Mar)
- **Format**: PDF report + Excel data file downloadable from spglobal.com/spdji/en/spiva/
- **Categories**: Large-Cap, Mid/Small-Cap, ELSS, Composite Equity, Government Bond, Composite Bond, Indian Rupee Composite Bond
- **Metrics per category**: % underperformed (equal-wt and asset-wt), % survivors, number of funds at start/end, benchmark name
- **Horizons**: 1yr, 3yr, 5yr, 10yr
- **Extraction approach**: Download Excel. Direct read with `pandas`. No PDF parsing needed.
- **Global equivalents**: US (semi-annual), Europe (semi-annual), Asia ex-Japan, Australia, Canada, Latin America — all have Excel downloads
- **NotebookLM fit**: Not needed — structured Excel, direct ingest

#### Morningstar Active/Passive Barometer
- **Frequency**: Semi-annual (~Mar and ~Sep)
- **Format**: PDF report + limited Excel. US and Europe barometers available via morningstar.com.
- **Categories**: US Equity (by style cap), International Equity, Global Equity, US Fixed Income (by sector), Municipal, etc.
- **Metrics**: Success rate (equal-wt and asset-wt), survivorship rate
- **Horizons**: 1yr, 3yr, 5yr, 10yr, 15yr, 20yr
- **India**: No India-specific barometer exists. Morningstar India has fund data through their direct platform (paid), not packaged as active/passive barometer.
- **Extraction approach**: US and Europe reports are PDF. Morningstar also publishes summary tables on their website. Two options:
  a. Parse PDF tables with `tabula-py` or `camelot`
  b. Use NotebookLM: upload PDF, query for structured table extraction
- **NotebookLM fit**: Yes — PDF reports with consistent table structure. Create notebook per region, upload new edition, query for the canonical table.

#### Bain Global PE Report + India VC Report
- **Frequency**: Annual (Global ~Mar, India VC ~Apr)
- **Format**: PDF only (no Excel supplement). Global report ~80 pages, India VC report ~40 pages.
- **Global PE metrics**: Deal value by region/sector, exit value by channel, fundraising, dry powder, returns (median IRR, pooled MOIC), entry/exit multiples
- **India VC metrics**: Investment value by stage (seed, early, late), deal count, exit value and channel mix (IPO, strategic, secondary), fundraising by vehicle size, sector breakdown (fintech, SaaS, consumer, health, etc.)
- **Extraction approach**: PDF parsing with NotebookLM. Key tables are consistent year-to-year (Bain uses near-identical exhibit structures):
  - "Global buyout deal value by region" (exhibit ~3)
  - "Global buyout exit value by region" (exhibit ~8)
  - "India VC investment by stage and sector" (exhibits ~5–10)
- **NotebookLM fit**: Strong fit — Bain reports have consistent exhibit numbering and table structure. Create one notebook per report, upload new PDF each year, query for the canonical tables. Human spot-check required for format changes.

#### EY Global IPO Trends
- **Frequency**: Quarterly (Q1 ~Apr, Q2 ~Jul, Q3 ~Oct, Annual ~Jan)
- **Format**: PDF report (2MB), data sourced from Dealogic
- **Metrics**: Number of IPOs, proceeds raised, by region/sector/exchange, aftermarket performance
- **Regions**: Americas, Asia-Pacific, EMEIA (India lumped into EMEIA — no separate India breakout in most quarterly editions)
- **India data**: The EY quarterly PDF does NOT consistently break out India. The annual edition sometimes has India-specific commentary but no dedicated India data table. India IPO statistics must come from:
  a. **NSE annual IPO reports** — NSE publishes yearly summaries with counts, proceeds, and comparisons
  b. **SEBI monthly bulletins** — Monthly IPO approval/listing data
  c. **Prime Database** (paid) — Most comprehensive India IPO data
  d. News aggregation for quarterly India totals (Moneycontrol, Fortune India, etc. report the EY/NSE figures)
- **Extraction approach**:
  - EY quarterly PDF → NotebookLM for Global/US/China numbers
  - India IPO numbers → NSE annual report scraping OR manual entry from news summaries (12 data points/year)
- **NotebookLM fit**: Mixed — PDF works for global numbers; India requires supplementary sources.

#### AMFI Monthly Data
- **Frequency**: Monthly (released ~5th-10th of following month)
- **Format**: Excel (.xls) from April 2004 onward; PDF for earlier months
- **Available in Excel**: Category-wise AUM, scheme-level AUM, folio counts (retail/HNI/institutional), sales/redemption data, SIP contribution data, passive fund data separated from active
- **Categories**: 35+ SEBI categories across Equity, Debt, Hybrid, Solution-Oriented, Index/ETF, Fund of Funds
- **Extraction approach**: Direct download from amfiindia.com/research-information/amfi-monthly. `pandas.read_excel()`. The Excel structure is consistent month-to-month.
- **NotebookLM fit**: Not needed — structured Excel, direct ingest.

#### mfapi.in — India Mutual Fund Universe API
- **Frequency**: NAV updated 6x daily (10:05, 14:05, 18:05, 21:05, 03:09, 05:05 IST)
- **Format**: REST API, JSON, no auth required, no rate limiting
- **Maintainer**: Yuvaraj L. (community project), status page at mfapi.in
- **Endpoints**:
  - `GET /mf?limit=&offset=` — paginated list of all 10,000+ schemes
  - `GET /mf/{schemeCode}?startDate=&endDate=` — full NAV history with date range
  - `GET /mf/{schemeCode}/latest` — latest NAV only
  - `GET /mf/search?q=` — search by scheme name
- **Fields per scheme**: `fund_house`, `scheme_type`, `scheme_category`, `scheme_name`, `isin_growth`, `isin_div_reinvestment`, full NAV history (date + value)
- **NOT available**: AUM, expense ratio, returns, portfolio holdings, benchmark index
- **Extraction approach**: Fetch full scheme list via `/mf` (paginated, ~100 calls at limit=100). Store as fund universe catalog. Refresh monthly (new schemes, delisted schemes). NAV history fetched on-demand per scheme if needed.
- **Use in project**: 
  1. Categorize the Indian mutual fund universe into active vs passive using `scheme_category`
  2. Track passive fund growth over time (count of Index/ETF schemes, fund houses launching passives)
  3. Cross-reference with SPIVA categories for active/passive market sizing
  4. NOT a replacement for SPIVA (doesn't compute returns or benchmark comparisons)
- **NotebookLM fit**: Not needed — structured JSON API, direct ingest.

#### NSE/BSE Market Data (beyond existing pipeline)
- The existing pipeline already fetches: Nifty 50, 15 sector indices, PE ratio
- For the active/passive project, additional data needed:
  - **NSE IPO annual summary**: NSE publishes yearly "IPO Market Review" reports — scrape or manual entry
  - **NSE market cap**: Total market cap and free-float (already available via NSE APIs)
  - **BSE market cap and listings**: BSE publishes monthly bulletins with listing counts

---

### Ingestion Architecture Decision: NotebookLM + Sheets

| Source | Format | Ingest Method | NotebookLM? |
|--------|--------|--------------|-------------|
| SPIVA India/Global | Excel | `pandas` direct | No |
| Morningstar Barometer | PDF | NotebookLM query → Sheets | **Yes** |
| Bain Global PE | PDF | NotebookLM query → Sheets | **Yes** |
| Bain-IVCA India VC | PDF | NotebookLM query → Sheets | **Yes** |
| EY IPO Trends | PDF | NotebookLM query → Sheets (global) + NSE/manual (India) | **Yes** (global portion) |
| AMFI Monthly | Excel | `pandas` direct | No |
| mfapi.in | JSON REST API | `requests` paginated fetch → Sheets | No |
| NSE IPO data | Web/Report | Scrape or manual | No |

**NotebookLM workflow per PDF source**:
1. Create a notebook per source (e.g. "Bain Global PE Report", "Morningstar US Barometer")
2. Upload each new edition PDF as it's released
3. Structured prompt: "Extract the following table: [canonical schema]. Return as CSV with columns: [list]"
4. NotebookLM returns structured data via its query API
5. Data Pipeline Agent validates, transforms, writes to Sheets tab
6. Notebook serves as auditable record of extraction (reproducible)

### Data Pipeline Agent — Phase 1 Tasks

- [ ] Create new Sheets tabs in the existing macro spreadsheet:
  - `SPIVA_India`, `SPIVA_Global`, `Morningstar_Barometer`
  - `PE_Activity_Global`, `PE_Activity_India`
  - `IPO_Activity`, `AMFI_Monthly`, `MFAPI_Fund_Universe`
- [ ] Write mfapi.in fund universe ingest: paginated fetch of all schemes, categorize into active/passive via `scheme_category`, write to `MFAPI_Fund_Universe` tab.
- [ ] Write SPIVA India + Global ingest: download Excel, read with pandas, validate row counts, write to Sheets.
- [ ] Write AMFI monthly ingest: download `.xls`, parse category-wise sheet, write to `AMFI_Monthly` tab. **AMFI is the canonical source for active/passive AUM splits** — mfapi.in is supplementary scheme-level detail (see Key Decision #14).
- [ ] Set up NotebookLM notebooks for: (a) Morningstar US Barometer, (b) Morningstar Europe Barometer, (c) Bain Global PE Report, (d) Bain-IVCA India VC Report, (e) EY Global IPO Trends.
- [ ] Write extraction prompts per notebook matching the canonical table schemas in Section 1.1.
- [ ] **NotebookLM validation gate** (see Key Decision #13): Every ingest script that consumes NotebookLM output must:
  1. Schema-validate every row against the canonical table (column count, types, non-null required fields)
  2. Check row count vs expected (from the PDF's reported universe: number of categories × horizons)
  3. If row count deviates >20% from expected → abort, log error, alert operator, do NOT write to Sheets
  4. If row count deviates ≤20% → write to Sheets with `needs_review: true` flag in metadata row
  5. Log the deviation for audit regardless
- [ ] For India IPO data: scrape NSE annual IPO report page OR set up quarterly manual entry with documented procedure.
- [ ] Implement QA checks: row count vs report pages, year-over-year deltas within reasonable bands, category/region enumeration matches expected set.
- [ ] Write `_metadata` rows per Sheets tab with `as_of` and `next_expected` fields (feeds the `data_freshness` contract in Section 1.3).

### Backend Engineer Agent

- [ ] Provide read-only collector functions that read from the new Sheets tabs: `_fetch_spiva_india()`, `_fetch_pe_activity_india()`, etc. Each reader also fetches the `_metadata` row to populate `data_freshness`.
- [ ] Implement two-tier cache architecture (see Key Decision #12):
  - **Tier 1 (startup-loaded)**: Annual/semi-annual data (SPIVA, Morningstar, Bain, EY, NSE IPO) → loaded at FastAPI startup into module-level dict. Exposed via `POST /api/admin/refresh-cache` (extend existing endpoint at main.py:3141) for force-refresh after Data Pipeline updates.
  - **Tier 2 (BulkSheetCache)**: Monthly data (AMFI, mfapi.in) → use existing `BulkSheetCache` singleton (24-hour TTL).
  - Add `ETag` headers (MD5 of response body) to all analytics GET responses. Frontend sends `If-None-Match` → backend returns 304 if unchanged.
- [ ] Add mfapi.in reachability to `GET /api/health` response (see Key Decision #14). Serve last-known-good fund universe with `stale: true` if refresh fails.
- [ ] Define error response schema for analytics endpoints: `{"success": false, "error": "...", "error_code": "DATA_STALE"|"SOURCE_UNAVAILABLE"|"PARTIAL_DATA"}`.

### Frontend Engineer Agent

- [ ] Define TS types matching the canonical table schemas.
- [ ] Build mock API clients using expected contract shapes to unblock UI.
- [ ] Update `FEATURE_REQUESTS.md` with expected API contracts for each of the 5 endpoints.

**Deliverables**: All new Sheets tabs populated (3–5 years of historical data), ingestion scripts + NotebookLM notebooks, TS types defined.

---

## 3. Phase 2 — Backend Analytics APIs (Week 3–5)

### Data Pipeline Agent

- [ ] Refresh all upstream tables for most recent periods (latest SPIVA scorecard, latest Bain/EY editions).
- [ ] Provide sample query outputs (JSON snippets) for backend development and tests.

### Backend Engineer Agent ✅ COMPLETE (May 11, 2026)

5 new read-only endpoints under `/api/v1/analytics/` (versioned from day one — Key Decision #10):

Endpoint specs, request params, response schemas (with `data_freshness` blocks) defined above. All endpoints return `ETag` headers; support `If-None-Match` for 304 responses (Key Decision #12). Error responses follow: `{"success": false, "error": "...", "error_code": "DATA_STALE"|"SOURCE_UNAVAILABLE"|"PARTIAL_DATA"}`.

- [x] Implement the 5 endpoints against shared `schemas/analytics/` JSON Schema files (Key Decision #11). Run contract tests that validate actual responses against schemas before marking any endpoint complete.
- [x] Wire collector functions (from Phase 1) → two-tier cache → response assembly with `data_freshness` per source.
- [x] Composite `/india-opportunity/snapshot`: assemble from 4 sources. If one source is `missing` or `stale`, return 200 with per-source `data_freshness` entries reflecting it — never fail wholesale.
- [x] Add `POST /api/admin/refresh-cache` logic for Tier 1 (annual) data: clear + reload from Sheets on demand.
- [x] Deployed as `treso-analytics` systemd service on VM (port 8000, auto-start on boot).
- [x] ETag 304 conditional responses verified on all 5 endpoints.
- [x] AMFI data flowing end-to-end (47 rows, April 2026) — verified via live API response.
- [x] Snapshot endpoint returns live regime classification (Growth-Disinflation, 65% confidence, Nifty 22,500).

### Frontend Engineer Agent

- [ ] Replace mock clients with real fetch calls as endpoints come online.
- [ ] Handle loading/error/stale-data states.

**Deliverables**: All 5 endpoints tested, documented in `API_CONTRACTS.md`, with Postman/cURL examples.

---

## 4. Phase 3 — Frontend Views (Week 4–7)

### Data Pipeline Agent

- [ ] On-call for schema adjustments and derived fields needed by specific charts.
- [ ] Add pre-aggregated views if needed (currently not expected — the data volumes are small).

### Backend Engineer Agent

- [ ] Add light derived metrics as needed: classification of exit environments ("drought" / "normal" / "boom"), underperformance trend direction.
- [ ] Ensure all 5 endpoints respond <2s with caching in place.

### Frontend Engineer Agent

Four UI panels:
1. **Global Active vs Passive** — table + bar chart, dropdowns for region/category/horizon
2. **PE/VC Liquidity** — heatmap (region × year) of investment and exit values
3. **IPO & Exit Liquidity** — time-series (India vs US IPO counts + proceeds)
4. **India Opportunity Snapshot** — single summary card consuming the composite endpoint

**Deliverables**: All 4 panels wired in Dyad with real data, behind feature flags if needed.

---

## 5. Phase 4 — Testing, UAT, and Hardening (Week 6–8)

### Data Pipeline Agent

- [ ] Regression-test all ingestion scripts: re-run on latest PDFs/Excels, confirm no schema breakage.
- [ ] Add automated checks for missing years or categories in each table. Alert Backend if gaps appear.

### Backend Engineer Agent

- [ ] Expand tests: happy paths, error paths, performance budgets, JSON schema contract tests.
- [ ] End-to-end UAT with Frontend in staging environment.

### Frontend Engineer Agent

- [ ] E2E UI tests for all 4 panels.
- [ ] Validate edge cases: no data, partial data, stale data, timeouts.

**Deliverables**: Green test suite, signed-off UAT, critical bugs cleared.

---

## 6. Phase 5 — Launch, Monitoring, and Iteration (Week 8+)

### Data Pipeline Agent

**Schedule design — based on actual source release cadences:**

| Source | Release Cadence | Refresh Schedule | Method |
|--------|----------------|------------------|--------|
| SPIVA India | Semi-annual (Mar, Sep) | 1 week after release | Manual upload Excel → script OR automated if download URL is stable |
| SPIVA Global | Semi-annual | 1 week after release | Same as India |
| Morningstar Barometer | Semi-annual (Mar, Sep) | 1 week after release | Upload PDF to NotebookLM → query → Sheets |
| Bain Global PE | Annual (Mar) | 2 weeks after release | Upload PDF to NotebookLM → query → Sheets |
| Bain-IVCA India VC | Annual (Apr) | 2 weeks after release | Same |
| EY IPO Trends | Quarterly | 1 week after each quarterly release | Upload PDF to NotebookLM → query → Sheets |
| AMFI Monthly | Monthly (~10th) | 12th of each month | Cron-triggered script: download Excel → validate → Sheets |
| mfapi.in | NAV: 6x daily | Monthly refresh (schemes list) | Cron-triggered: paginated fetch → deduplicate → update `MFAPI_Fund_Universe` tab |
| NSE IPO data | Annual + news | Within 1 week of NSE report | Manual or scrape |

**Implementation**:
- AMFI monthly + mfapi.in: Add to cron **offset by 5 minutes** from the existing macro pipeline to avoid Sheets write contention (Key Decision #15):
  ```
  # Existing macro pipeline (unchanged)
  30 12 * * 0 /home/ubuntu/clawd/treso_analytics/sunday_macro_update.sh

  # New: active/passive monthly ingests (Sun 6:35 PM IST — 5 min offset)
  35 12 * * 0 /home/ubuntu/clawd/treso_analytics/run_active_passive_ingests.sh
  ```
  The existing pipeline writes 13 tabs; the new one writes 2 more. Google Sheets rate limit is 60 requests/100s/user. 5-minute offset ensures sequential execution and avoids burst contention.
- All others: Event-driven or manual trigger (annual/semi-annual data doesn't justify a cron timer). Document SOP per source.
- NotebookLM sources: Manual upload of new PDF → run query → validation gate (Key Decision #13) → write to Sheets (15 min/quarter)
- Write runbooks: "How to add a new SPIVA/Morningstar/Bain/EY edition" in `DATA_MODEL.md`
- Set up failure alerts for AMFI monthly (the only automated monthly source)
- Add mfapi.in reachability check to `GET /api/health` — if unreachable, health returns degraded status with detail "mfapi_unreachable"

### Backend Engineer Agent

- [ ] Add lightweight monitoring for analytics endpoints (request counts, latency, error rates).
- [ ] Maintain versioning discipline: breaking changes → new `/v2` endpoints + deprecation notices.

### Frontend Engineer Agent

- [ ] Basic usage analytics on new panels.
- [ ] Propose next-wave enhancements via `FEATURE_REQUESTS.md`.

---

## 7. Summary Swimlane

| Phase | Data Pipeline | Backend | Frontend |
|-------|--------------|---------|----------|
| 0 — Alignment | Audit pipeline overlap; propose canonical tables; define `data_freshness` metadata contract | Update API_CONTRACTS; define 5 `/v1/analytics/` endpoints; create `schemas/analytics/` with JSON Schema per endpoint | Inventory pages; draft wireframes with concrete field names from schemas |
| 1 — Data Model | Build Sheets tabs; SPIVA/AMFI ingests; NotebookLM notebooks + **validation gate**; write `_metadata` rows | Collector functions (each reads `_metadata`); two-tier cache (startup-loaded + BulkSheetCache); `ETag` headers; mfapi.in health check | TS types from shared schemas; mock clients generated from schemas; FEATURE_REQUESTS entries |
| 2 — Backend APIs | Keep data fresh; share sample outputs | Implement 5 `/v1/analytics/` endpoints; contract tests against shared schemas; `data_freshness` per response; extend `POST /api/admin/refresh-cache` | Replace mocks with real fetch; handle loading/error/stale-data states from `data_freshness` |
| 3 — Frontend | Support derived fields; pre-aggregation if needed | Performance: <2s target with two-tier cache; derive light metrics (no LLM text) | Build 4 analytics panels in Dyad; derive narrative flags client-side from structured data |
| 4 — Testing/UAT | Regression-test ingests; NotebookLM validation gate tests | Contract tests against schemas; perf tests; stale/missing/partial-data error path tests | E2E UI tests; UAT fixes |
| 5 — Launch/Ops | Cron offset 5 min (`35 12 * * 0`); runbooks per source; failure alerts for AMFI | Monitoring; `ETag` hit-rate; health check with mfapi.in status; `/v1/` versioning discipline | Usage analytics; next-wave features |

---

## 8. Key Decisions Captured

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Storage: Google Sheets (not DB) | Existing macro pipeline uses Sheets; small data volumes (<10K rows total); backend already has Sheets read layer |
| 2 | PDF extraction: NotebookLM | Bain/EY/Morningstar PDFs are 40–80 pages with complex tables. NotebookLM handles unstructured → structured reliably. Auditable (notebook = record of extraction). |
| 3 | Structured sources: direct pandas | SPIVA and AMFI provide Excel downloads. No NotebookLM overhead needed. |
| 4 | Regime labels: actual 4-regime taxonomy | `Growth-Disinflation`, `Growth-Inflation`, `Stagnation-Disinflation`, `Stagflation` — backend reads from existing `Regime_Classification` Sheets tab. No ad-hoc "expansion"/"contraction" labels. |
| 5 | India IPO data: composite approach | EY quarterly PDF for global context (India not separately broken out); NSE annual reports + news for India numbers. |
| 6 | AMFI: automated monthly | Only source with monthly cadence. Added to existing Sunday cron (data from ~10th will be fresh). |
| 7 | All semi-annual/annual sources: manual trigger | SPIVA, Morningstar, Bain, EY release 2–4 times/year combined. Automated cron doesn't make sense — event-driven SOP instead. |
| 8 | mfapi.in: added as fund universe source | Free REST API (no auth). Provides scheme-level NAV, category, fund house for 10,000+ Indian mutual funds. Enables active/passive fund categorization and universe sizing. Does NOT replace SPIVA or AMFI. |
| 9 | Morningstar India: no barometer | Verified — no regular India Active/Passive Barometer. A 2017 one-off found Indian active funds outperform (structurally different from US/Europe). |
| 10 | API versioning: `/v1/` from day one | Retrofitting versioning after frontend integration is expensive. Adding `/v1/` now costs nothing and prevents a forced migration when breaking changes hit. |
| 11 | Shared JSON schemas for contract testing | Backend and Frontend both read from `schemas/analytics/*.json`. Backend validates responses against them. Frontend generates mock clients from them. Eliminates "mock drift" during parallel Phase 2–3 work. Committed before any endpoint code is written. |
| 12 | Two-tier cache architecture | **Tier 1 (startup-loaded)**: Annual/semi-annual data loaded at FastAPI startup. Force-refresh via `POST /api/admin/refresh-cache`. **Tier 2 (BulkSheetCache)**: Monthly data via existing `BulkSheetCache` singleton (24-hr TTL). All GET responses carry `ETag` headers for conditional requests (304 Not Modified). Rationale: in-memory cache is per-worker; startup-load + admin refresh avoids inconsistency across uvicorn workers for data that changes once/year. |
| 13 | NotebookLM validation gate | Every NotebookLM-based ingest must: (1) schema-validate every row, (2) check row count vs expected, (3) abort write if >20% deviation, (4) flag for human review if ≤20% deviation. The gate is in the ingest script — it returns `{"valid": false, "reason": "..."}` rather than silently writing garbage. |
| 14 | mfapi.in: supplementary, not canonical | AMFI Monthly Excel is the canonical source for active/passive AUM splits and category flows. mfapi.in provides scheme-level detail (NAV, fund house, ISIN) but is a community project with no SLA. If mfapi.in is unreachable, serve last-known-good fund universe with `stale: true` and surface in `GET /api/health`. |
| 15 | Cron offset: 5 minutes after macro pipeline | Existing pipeline writes 13 Sheets tabs; new pipeline writes 2 more. Both at Sun 12:30 UTC would burst 30+ writes against Google Sheets' 60 req/100s/user limit. Offset by 5 minutes (`35 12 * * 0`) eliminates contention. |

---

## 9. Data Pipeline Implementation

See **[active_passive_implementation.md](active_passive_implementation.md)** for the detailed implementation plan covering:

- NotebookLM authentication on VM (`notebooklm-py` 0.4.0, one-time `notebooklm login`)
- 8 new Google Sheets tabs (SPIVA_India, SPIVA_Global, Morningstar_Barometer, PE_Activity_Global, PE_Activity_India, IPO_Activity, AMFI_Monthly, MFAPI_Fund_Universe)
- 6 ingest scripts (2 automated via cron, 4 manual + NotebookLM)
- Cron schedule (AMFI + mfapi.in monthly at `35 12 * * 0` — 5 min offset from main pipeline)
- NotebookLM validation gate (Key Decision #13) in every PDF-based ingest script
- Source-to-script mapping with effort estimates (~5 days total)
