# Data Pipeline Implementation — Active/Passive Project

**Status**: Phase 1 Data Pipeline COMPLETE | Phase 0-2 Backend COMPLETE (verified 2026-05-12)

## 1. NotebookLM Authentication (one-time)

`notebooklm-py` 0.4.0 installed on VM via `pipx`. Auth is cookie-based — Playwright Chromium crashes on Apple Silicon Mac and headless VM has no display.

**Working auth flow** (completed 2026-05-11):

1. On Mac: Chrome with `--remote-debugging-port` opened NotebookLM, user logged in
2. Playwright `connect_over_cdp` extracted session cookies → `storage_state.json`
3. File synced to VM at `/home/ubuntu/.notebooklm/profiles/default/storage_state.json`

```bash
# Verify on VM
ssh aws-clawdbot 'export PATH="$HOME/.local/bin:$PATH" && notebooklm auth check --test && notebooklm list'
```

**Cookie refresh**: Google session cookies expire. To refresh:
- Open NotebookLM in Chrome with remote debugging, then re-extract via CDP and re-sync `storage_state.json`
- Or run the WebKit-based login script: `python3 macro_pipeline/scripts/notebooklm_login_webkit.py` (uses Safari engine, no Chromium crash)

## 2. Google Sheets — New Tabs

Add to spreadsheet `10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU`:

| # | Tab | Source | Key columns | Update |
|---|-----|--------|-------------|--------|
| 14 | `SPIVA_India` | spglobal.com Excel | `report_period`, `category`, `horizon_years`, `pct_underperformed`, `pct_survivors`, `benchmark_name`, `num_funds_start`, `num_funds_end` | Semi-annual |
| 15 | `SPIVA_Global` | spglobal.com Excel | Same + `region` | Semi-annual |
| 16 | `Morningstar_Barometer` | NotebookLM → PDFs | `report_date`, `region`, `category`, `horizon_years`, `success_rate_equal_wt`, `success_rate_asset_wt`, `survivorship_rate` | Semi-annual |
| 17 | `PE_Activity_Global` | NotebookLM → Bain PE PDF | `year`, `region`, `deal_value_usd_bn`, `exit_value_usd_bn`, `fundraising_usd_bn`, `dry_powder_usd_bn`, `deal_count`, `exit_count` | Annual |
| 18 | `PE_Activity_India` | NotebookLM → Bain-IVCA PDF | `year`, `vc_investment_usd_bn`, `vc_deal_count`, `exit_value_usd_bn`, `exit_channel_ipo_pct`, `exit_channel_strategic_pct`, `exit_channel_secondary_pct`, `fundraising_usd_bn` | Annual |
| 19 | `IPO_Activity` | NotebookLM (EY) + NSE | `year`, `period`, `country`, `num_ipos`, `proceeds_usd_bn`, `market_share_volume_pct`, `market_share_proceeds_pct` | Quarterly |
| 20 | `AMFI_Monthly` | amfiindia.com Excel | `month`, `category`, `category_type`, `aum_inr_cr`, `net_inflows_inr_cr`, `folio_count`, `sip_contributions_inr_cr` | Monthly |
| 21 | `MFAPI_Fund_Universe` | mfapi.in REST API | `scheme_code`, `scheme_name`, `fund_house`, `scheme_type`, `scheme_category`, `is_active`, `category_group`, `latest_nav`, `nav_date`, `scrape_date` | Monthly |

## 3. Ingest Scripts

All scripts: `/home/ubuntu/clawd/treso_analytics/`. Output → Google Sheets via `OptimizedMacroDataSheetsManager`.

### 3.1 SPIVA India + Global (`ingest_spiva.py`)

- Download Excel from spglobal.com/spdji/en/spiva/
- `pandas.read_excel()`, map columns → canonical schema
- Validate: row count, category enumeration
- Write to `SPIVA_India` / `SPIVA_Global` tabs
- **Trigger**: Semi-annual, manual (URL changes per edition). Quarterly check cron for new edition.

### 3.2 Morningstar Barometer (`ingest_morningstar_barometer.py`)

- Download PDF from morningstar.com/business/insights/research/
- Upload to NotebookLM notebook "Morningstar Barometer" via `client.sources.add_pdf()`
- Query: `"Extract the active vs passive success rate table. For each category and horizon (1/3/5/10/15/20 yr), return: category_name, horizon_years, success_rate_equal_weighted, success_rate_asset_weighted, survivorship_rate. Output as CSV."`
- Download CSV → validate → write to `Morningstar_Barometer` tab
- **Trigger**: Semi-annual (~Mar, ~Sep), manual.

### 3.3 Bain PE Reports (`ingest_bain_pe.py`)

- Download PDFs from bain.com (Global PE + India VC)
- Upload to NotebookLM notebooks; query per exhibit
- Prompts target specific exhibits: "Exhibit 3: Global buyout deal value by region", "Exhibit 8: Global exit value by region and channel", "India VC investment by stage and sector"
- Download CSV → validate → write to `PE_Activity_Global` / `PE_Activity_India`
- **Trigger**: Annual (Global ~Mar, India VC ~Apr), manual.

### 3.4 EY IPO Trends (`ingest_ey_ipo.py`)

- Download PDF from ey.com/insights/ipo/trends
- Upload to NotebookLM; query for IPO counts/proceeds by region/country
- India supplement: NSE annual report scrape or news aggregation
- Write to `IPO_Activity` tab
- **Trigger**: Quarterly, manual.

### 3.5 AMFI Monthly (`ingest_amfi_monthly.py`)

- Download `.xls` from amfiindia.com/research-information/amfi-monthly
- `pandas.read_excel()`, parse category-wise sheet
- Validate: AUM total check, MoM delta <20%
- Write to `AMFI_Monthly` tab
- **Trigger**: Monthly, **automated via cron**.

### 3.6 mfapi.in Fund Universe (`ingest_mfapi_universe.py`)

- `GET /mf?limit=100&offset=0` — paginate through all 10,000+ schemes
- Extract: `fund_house`, `scheme_type`, `scheme_category`, `scheme_name`, ISIN
- Derive: `is_active` (not Index/ETF/FoF), `category_group` (Large/Mid/Small/Flexi/ELSS/Debt/Hybrid/Index/Other)
- Deduplicate by `scheme_code`, track adds/drops
- Write to `MFAPI_Fund_Universe` tab
- **Trigger**: Monthly, **automated via cron**.

## 4. Cron

```
# Existing macro pipeline (unchanged)
30 12 * * 0 /home/ubuntu/clawd/treso_analytics/sunday_macro_update.sh

# Active/passive monthly ingests (Sun 6:35 PM IST)
35 12 * * 0 /home/ubuntu/clawd/treso_analytics/run_active_passive_ingests.sh

# Active/passive staleness checker (Sun 6:40 PM IST — 5 min after ingests)
40 12 * * 0 /home/ubuntu/clawd/treso_analytics/macro_pipeline/scripts/staleness_checker.py
```

`run_active_passive_ingests.sh`:
```bash
#!/bin/bash
export PYTHONPATH=/home/ubuntu/clawd/treso_analytics
LOGDIR=/home/ubuntu/clawd/logs/treso_analytics
python3 -c "from ingest_amfi_monthly import ingest; ingest()" >> $LOGDIR/amfi_monthly.log 2>&1
python3 -c "from ingest_mfapi_universe import ingest; ingest()" >> $LOGDIR/mfapi_universe.log 2>&1
```

Semi-annual/annual sources: manual trigger, documented SOP per source.

## 5. Source → Script → Trigger Map

| Source | Script | Frequency | Trigger | Effort |
|--------|--------|-----------|---------|--------|
| SPIVA India | `ingest_spiva.py` | Semi-annual | Manual | 0.5d |
| SPIVA Global | `ingest_spiva.py` | Semi-annual | Manual | 0.25d |
| Morningstar Barometer | `ingest_morningstar_barometer.py` | Semi-annual | Manual + NotebookLM | 0.5d |
| Bain Global PE | `ingest_bain_pe.py` | Annual | Manual + NotebookLM | 0.25d |
| Bain-IVCA India VC | `ingest_bain_pe.py` | Annual | Manual + NotebookLM | 0.25d |
| EY IPO Trends | `ingest_ey_ipo.py` | Quarterly | Manual + NotebookLM + NSE | 0.5d |
| AMFI Monthly | `ingest_amfi_monthly.py` | Monthly | **Cron** | 1d |
| mfapi.in Universe | `ingest_mfapi_universe.py` | Monthly | **Cron** | 1d |
| Sheets tab setup | `migrate_macro_tabs.py` | Once | Manual | 0.5d |
| NotebookLM auth + notebooks | `notebooklm login` + create | Once | Manual | 0.5d |
| **Total** | | | | **~5d** |

## 6. NotebookLM Shared Notebook (Production Approach)

**Notebook ID**: `b852a192-7997-4154-ab33-3051849d0a1c`

All 6 semi-annual/annual/quarterly sources are in a single shared notebook (can hold up to 50 sources). This is simpler than the original per-source notebook plan and avoids context-switching.

**Extraction pattern** (proven):
```
notebooklm ask -n b852a192 "From [source]: Output ONLY a CSV table with NO text before or after.
Columns: [comma-separated list]
[Rules for each column, expected ranges, "Include ALL categories/horizons/years"]"
```

**Critical rules**:
- No `--json` flag (returns empty answer)
- No `-s` source filters (blocks responses)
- Always `notebooklm clear` between queries
- Strict "NO text before or after" in prompt produces clean CSV
- CSV line-wrapping: NotebookLM wraps header lines >80 chars — parse by joining continuation lines (no-commas lines appended to previous)

**Manual refresh SOP**: Query each source via CLI, parse CSV, write to Sheets with `_metadata` row. Full extraction script: `/tmp/run_extraction_v3.py` on VM.

---

## 7. CRITICAL: NotebookLM Validation Gate (Key Decision #13)

Every ingest script that consumes NotebookLM output MUST implement this gate BEFORE writing to Sheets:

```
1. Schema-validate every row against the canonical table:
   - Column count matches expected
   - Column types correct (int where int, float where float)
   - Required non-null fields present

2. Row count check vs expected:
   expected_rows = num_categories × num_horizons × num_regions
   deviation_pct = abs(actual - expected) / expected * 100

3. Decision:
   If deviation > 20%:
     → ABORT (do NOT write to Sheets)
     → Log error with actual vs expected row count
     → Alert operator
     → Return {"valid": false, "reason": "Row count deviation: X% (>20% threshold)"}

   If deviation ≤ 20%:
     → PROCEED with write
     → Set "needs_review": true in _metadata row
     → Log warning for audit

4. Always log deviation regardless of outcome (audit trail)
```

## 8. CRITICAL: _metadata Row Format

Every Sheets tab MUST have a `_metadata` row as the first row. The backend reads this to populate `data_freshness` blocks in every API response.

**Row format** (columns match the tab's schema, with these special values):

| Column | Value | Required |
|--------|-------|----------|
| `row_type` | `"_metadata"` | YES |
| `as_of` | Date the source data represents, ISO format (e.g. `"2026-03-31"`) | YES |
| `next_expected` | Date next edition is expected, ISO format (e.g. `"2026-09-30"`) | YES |
| `needs_review` | `"true"` if NotebookLM validation flagged issues, else `"false"` | YES (NotebookLM sources only) |
| All other columns | Empty string `""` | — |

**Examples**:

```
# SPIVA_India tab _metadata row:
{"row_type": "_metadata", "as_of": "2026-03-31", "next_expected": "2026-09-30",
 "report_period": "", "category": "", "horizon_years": "", "pct_underperformed": "",
 "pct_survivors": "", "benchmark_name": "", "num_funds_start": "", "num_funds_end": ""}

# Morningstar_Barometer tab _metadata row (NotebookLM source — needs_review required):
{"row_type": "_metadata", "as_of": "2026-03-31", "next_expected": "2026-09-30",
 "needs_review": "false", "report_date": "", "region": "", "category": "",
 "horizon_years": "", "success_rate_equal_wt": "", "success_rate_asset_wt": "",
 "survivorship_rate": ""}
```

**Staleness thresholds** (days past `next_expected` before backend marks `status: "stale"`):

| Source | Grace period |
|--------|-------------|
| AMFI Monthly | 35 days |
| SPIVA India/Global | 90 days |
| Morningstar Barometer | 90 days |
| Bain Global PE | 120 days |
| Bain-IVCA India VC | 120 days |
| EY IPO Trends | 45 days |
| mfapi.in | 35 days |
| NSE IPO data | 120 days |

## 9. Current Status and Dependency Chain

```
Phase 0 (Alignment)         Phase 1 (Data Pipeline)        Phase 2 (Backend APIs)
─────────────────────       ─────────────────────          ──────────────────────
Backend: COMPLETE ✅        Data Pipeline: COMPLETE ✅      Backend: COMPLETE ✅
- API_CONTRACTS audited     - 8 Sheets tabs created        - 5 routes wired in main.py
- 6 JSON Schemas created    - 4 ingest scripts deployed    - ETag (304) on all endpoints
- data_freshness defined    - 4 NotebookLM notebooks       - Deployed to VM
- analytics_api.py (380 ln) - NotebookLM validation gate   - AMFI data flowing (47 rows)
- Health check extended     - _metadata rows per tab       - Snapshot: live regime + macro
- Cache extended            - AMFI cron deployed

Frontend: PENDING 🔴                                        Frontend: PENDING 🔴
- TS types from schemas                                    - Replace mocks
- Mock clients                                              - Wire UI panels
```

### Phase 1 Delivered

| # | Deliverable | Status | Evidence |
|---|------------|--------|----------|
| 1 | 8 Sheets tabs created | Done | `get_all_values()` on all 8 tabs returns headers |
| 2 | AMFI_Monthly populated | Done | 47 rows, April 2026, AUM ₹80.76L Cr |
| 3 | `_metadata` row per tab | Done | `get_all_records()` finds `row_type="_metadata"` with `as_of`/`next_expected` |
| 4 | `ingest_amfi_monthly.py` | Deployed | Cron `35 12 * * 0`, verified run 2026-05-11 |
| 5 | `ingest_mfapi_universe.py` | Deployed | API down (502); serve-stale fallback active |
| 6 | `ingest_spiva.py` | Deployed | Manual trigger; spglobal.com blocks automated access |
| 7 | `ingest_notebooklm.py` | Deployed | 4 sources configured (Morningstar, Bain Global, Bain India, EY IPO) |
| 8 | NotebookLM notebooks | Done | 4 notebooks created, IDs in script config |
| 9 | NotebookLM validation gate | Done | Schema-validate + row count check + >20% abort in ingest_notebooklm.py |
| 10 | Cron | Deployed | `35 12 * * 0 /home/ubuntu/clawd/treso_analytics/run_active_passive_ingests.sh` |

### Tab population status

| Tab | Rows | as_of | Status |
|-----|------|-------|--------|
| AMFI_Monthly | 329 | 2026-04 | healthy (7 months: Oct 2025-Apr 2026) |
| MFAPI_Fund_Universe | 593 | 2026-05-12 | degraded (1,181 codes discovered, 593 fetched, 588 errors — search index biased toward old/closed schemes, mostly debt FMPs; 29 fund houses) |
| SPIVA_India | 21 | 2025-H2 | healthy (5 categories × 4 horizons, NotebookLM) |
| SPIVA_Global | 40 | 2025-H2 | healthy (8 categories × 5 horizons, NotebookLM) |
| Morningstar_Barometer | 114 | 2025-12-31 | healthy (~19 categories × 6 horizons, NotebookLM) |
| PE_Activity_Global | 12 | 2026-03-01 | healthy (3 years × 4 regions, NotebookLM) |
| PE_Activity_India | 4 | 2025-04-01 | healthy (4 years, NotebookLM) |
| IPO_Activity | 4 | 2026-03-31 | healthy (India + Global, NotebookLM) |

## 10. Backend Service Deployment

Service: `treso-analytics` (systemd)

```bash
# Service file: /etc/systemd/system/treso-analytics.service
# Working dir: /home/ubuntu/clawd/treso_analytics
# Port: 8000
# Status: active (running), enabled (auto-start on boot)

systemctl status treso-analytics
curl -s http://localhost:8000/api/health | python3 -m json.tool
```

**Deployed dependencies**:
- `postgres_connector.py` — synced to VM (was missing)
- `gspread-dataframe` — pip installed (was missing from VM)
- `analytics_api.py` — synced with BulkSheetCache dependency removed
- `main.py` — synced with 5 `/api/v1/analytics/*` routes + ETag support

**Known gaps**:
- `mfapi.in` paginated `/mf` endpoint returns 502; search `/mf/search?q=` works (max 15 results/query); detail `/mf/{code}` works. Enumeration via 26x26 2-letter prefix search + fund house search yielded 1,181 unique codes. Detail fetch: 593 success, 588 stale/invalid codes. Data biased toward older close-ended debt schemes. A proper AMFI registration-based universe source would be more complete.
- AMFI historical backfill: Oct 2025-Apr 2026 (7 months). Older months use different sheet formats (month-name sheets like "October 2025", "MCR_MonthlyReport"). Script updated to handle all 4 known formats: MCR_Report, MCR_MonthlyReport, AMFI MONTHLY, month-name sheets.
- AMFI regex fixed: handles both "Month Year" and "Month-Year" formats in report header (Feb/Mar 2026 used dash format).

## 11. Verification Commands

```bash
# Verify AMFI_Monthly via analytics_api (live data):
ssh aws-clawdbot 'cd /home/ubuntu/clawd/treso_analytics && PYTHONPATH=/home/ubuntu/clawd/treso_analytics python3 -c "
from analytics_api import _fetch_amfi_monthly
data, freshness = _fetch_amfi_monthly()
print(f\"Records: {len(data)}, Freshness: {freshness}\")
print(\"First:\", data[0] if data else \"EMPTY\")
"'

# Test all 5 API endpoint handlers:
ssh aws-clawdbot 'cd /home/ubuntu/clawd/treso_analytics && PYTHONPATH=/home/ubuntu/clawd/treso_analytics python3 -c "
from analytics_api import (
    handle_active_vs_passive_summary,
    handle_pe_liquidity_summary,
    handle_india_opportunity_snapshot,
)
# Active vs Passive
r, code, etag = handle_active_vs_passive_summary({\"region\": \"India\", \"horizon_years\": 10})
print(f\"AVP: status={code}, categories={len(r[\"data\"][\"categories\"])}\")

# PE Liquidity
r, code, etag = handle_pe_liquidity_summary({\"region\": \"India\"})
print(f\"PE: status={code}, records={len(r[\"data\"][\"records\"])}\")

# Snapshot
r, code, etag = handle_india_opportunity_snapshot({\"horizon_years\": 5})
print(f\"Snapshot: status={code}, regime={r[\"data\"][\"macro_context\"][\"regime\"]}\")
"'

# All 5 API endpoints (backend service running on port 8000):
curl -s http://localhost:8000/api/v1/analytics/active-vs-passive/summary | python3 -m json.tool | head -20
curl -s http://localhost:8000/api/v1/analytics/india-opportunity/snapshot | python3 -m json.tool | head -30
curl -s http://localhost:8000/api/v1/analytics/pe-liquidity/summary | python3 -m json.tool | head -20
curl -s http://localhost:8000/api/v1/analytics/ipo-activity/summary | python3 -m json.tool | head -20

# ETag test (conditional request):
etag=$(curl -s -i "http://localhost:8000/api/v1/analytics/active-vs-passive/summary?region=India" | grep -i "^etag:" | sed "s/.*ETag: //")
curl -s -w "\\nHTTP %{http_code}\\n" -H "If-None-Match: $etag" "http://localhost:8000/api/v1/analytics/active-vs-passive/summary?region=India"
# Expected: HTTP 304

# Backend service management:
systemctl status treso-analytics
sudo journalctl -u treso-analytics -f
sudo systemctl restart treso-analytics
```

# Verify cron:
ssh aws-clawdbot 'crontab -l | grep active_passive'

# Verify NotebookLM auth + notebooks:
ssh aws-clawdbot 'export PATH="$HOME/.local/bin:$PATH" && notebooklm auth check --test && notebooklm list'
```
