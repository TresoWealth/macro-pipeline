#!/usr/bin/env python3
"""Create 8 new Sheets tabs + _Metadata tab for active/passive project.

Run on VM: python3 create_new_tabs.py
"""

import sys
from datetime import date

sys.path.insert(0, "/home/ubuntu/clawd/treso_analytics")
from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

SPREADSHEET_ID = "10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU"

NEW_TABS = {
    "SPIVA_India": [
        "report_period", "category", "horizon_years", "pct_underperformed",
        "pct_survivors", "benchmark_name", "num_funds_start", "num_funds_end",
        "data_source", "ingest_timestamp",
    ],
    "SPIVA_Global": [
        "report_period", "category", "horizon_years", "pct_underperformed",
        "pct_survivors", "benchmark_name", "num_funds_start", "num_funds_end",
        "region", "data_source", "ingest_timestamp",
    ],
    "Morningstar_Barometer": [
        "report_date", "region", "category", "horizon_years",
        "success_rate_equal_wt", "success_rate_asset_wt",
        "survivorship_rate", "data_source", "ingest_timestamp",
    ],
    "PE_Activity_Global": [
        "year", "region", "deal_value_usd_bn", "exit_value_usd_bn",
        "fundraising_usd_bn", "dry_powder_usd_bn", "deal_count",
        "exit_count", "median_ev_ebitda", "data_source", "ingest_timestamp",
    ],
    "PE_Activity_India": [
        "year", "vc_investment_usd_bn", "vc_deal_count",
        "exit_value_usd_bn", "exit_channel_ipo_pct",
        "exit_channel_strategic_pct", "exit_channel_secondary_pct",
        "fundraising_usd_bn", "fintech_investment_usd_bn",
        "saas_investment_usd_bn", "data_source", "ingest_timestamp",
    ],
    "IPO_Activity": [
        "year", "period", "country", "num_ipos", "proceeds_usd_bn",
        "market_share_volume_pct", "market_share_proceeds_pct",
        "avg_deal_size_usd_m", "data_source", "ingest_timestamp",
    ],
    "AMFI_Monthly": [
        "month", "category", "category_type", "aum_inr_cr",
        "net_inflows_inr_cr", "folio_count", "sip_contributions_inr_cr",
        "data_source", "ingest_timestamp",
    ],
    "MFAPI_Fund_Universe": [
        "scheme_code", "scheme_name", "fund_house", "scheme_type",
        "scheme_category", "isin_growth", "is_active",
        "category_group", "latest_nav", "nav_date", "scrape_date",
        "data_source", "ingest_timestamp",
    ],
}

METADATA_HEADERS = [
    "tab_name", "as_of", "next_expected", "last_ingested",
    "row_count", "status", "notes",
]


def main():
    mgr = OptimizedMacroDataSheetsManager()
    spreadsheet = mgr.gc.open_by_key(SPREADSHEET_ID)
    existing = {ws.title for ws in spreadsheet.worksheets()}

    print(f"Existing tabs: {len(existing)}")

    created = 0
    for tab_name, headers in NEW_TABS.items():
        if tab_name in existing:
            print(f"  SKIP (exists): {tab_name}")
            continue
        print(f"  CREATE: {tab_name} ({len(headers)} cols)")
        ws = spreadsheet.add_worksheet(tab_name, rows=2000, cols=len(headers))
        ws.append_row(headers)
        last_col = chr(65 + len(headers) - 1) if len(headers) <= 26 else "Z"
        ws.format(
            f"A1:{last_col}1",
            {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
            },
        )
        created += 1

    # Create _Metadata tab
    if "_Metadata" not in existing:
        print("  CREATE: _Metadata")
        ws = spreadsheet.add_worksheet("_Metadata", rows=100, cols=len(METADATA_HEADERS))
        ws.append_row(METADATA_HEADERS)
        ws.format("A1:G1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.95},
        })
        today = date.today().isoformat()
        for tab_name in NEW_TABS:
            ws.append_row([tab_name, "", "", today, 0, "missing", "Awaiting first ingest"])
        created += 1
        print(f"  DONE: _Metadata initialized with {len(NEW_TABS)} tab entries")
    else:
        print("  SKIP (exists): _Metadata")

    # Summary
    worksheets = spreadsheet.worksheets()
    print(f"\nTotal tabs: {len(worksheets)} (created {created} new)")
    for ws in worksheets:
        marker = " [NEW]" if ws.title in NEW_TABS or ws.title == "_Metadata" else ""
        if marker:
            print(f"  {ws.title} ({ws.row_count} rows){marker}")


if __name__ == "__main__":
    main()
