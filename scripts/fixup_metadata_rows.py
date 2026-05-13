#!/usr/bin/env python3
"""Fixup: add row_type column to all active/passive tabs and insert _metadata row.

Backend contract (analytics_api.py:_read_metadata):
  - get_all_records() maps header row → dict keys
  - Looks for row where row_type == "_metadata"
  - _metadata row is first data row (index 2, after header)

Strategy: read all existing data from each tab, clear, rewrite with row_type prepended.
Run on VM: python3 fixup_metadata_rows.py
"""

import sys
from datetime import date, timedelta

sys.path.insert(0, "/home/ubuntu/clawd/treso_analytics")
from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

SPREADSHEET_ID = "10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU"

TAB_METADATA = {
    "AMFI_Monthly":        {"as_of": "2026-04", "next_expected": (date.today() + timedelta(days=35)).isoformat()},
    "MFAPI_Fund_Universe":  {"as_of": "", "next_expected": (date.today() + timedelta(days=35)).isoformat()},
    "SPIVA_India":          {"as_of": "", "next_expected": (date.today() + timedelta(days=90)).isoformat()},
    "SPIVA_Global":         {"as_of": "", "next_expected": (date.today() + timedelta(days=90)).isoformat()},
    "Morningstar_Barometer":{"as_of": "", "next_expected": (date.today() + timedelta(days=90)).isoformat()},
    "PE_Activity_Global":   {"as_of": "", "next_expected": (date.today() + timedelta(days=120)).isoformat()},
    "PE_Activity_India":    {"as_of": "", "next_expected": (date.today() + timedelta(days=120)).isoformat()},
    "IPO_Activity":         {"as_of": "", "next_expected": (date.today() + timedelta(days=45)).isoformat()},
}


def fix_tab(manager, spreadsheet, tab_name, meta):
    ws = manager._get_or_create_worksheet(spreadsheet, tab_name)
    records = ws.get_all_values()

    if not records:
        print(f"  SKIP: {tab_name} — empty")
        return

    old_header = records[0]
    old_data = records[1:] if len(records) > 1 else []

    # Check if already fixed
    if old_header and old_header[0] == "row_type":
        has_meta = any(r and r[0] == "_metadata" for r in old_data)
        if has_meta:
            print(f"  SKIP: {tab_name} — already has row_type + _metadata")
            return
        # Has row_type header but no _metadata — just insert it
        meta_row = ["_metadata", meta["as_of"], meta["next_expected"]] + [""] * (len(old_header) - 3)
        ws.insert_row(meta_row, index=2)
        print(f"  FIXED: {tab_name} — inserted _metadata row")
        return

    # Build new header and rows
    new_header = ["row_type"] + old_header
    new_rows = []

    # _metadata row first
    meta_row = ["_metadata", meta["as_of"], meta["next_expected"]] + [""] * (len(old_header) - 2)
    new_rows.append(meta_row)

    # Existing data rows with empty row_type
    for row in old_data:
        if row and row[0] == "_metadata":
            continue  # Skip any old-style metadata
        new_rows.append([""] + row)

    # Clear and rewrite
    ws.clear()
    ws.append_row(new_header)
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    data_count = len([r for r in new_rows if r[0] != "_metadata"])
    print(f"  FIXED: {tab_name} — header={len(new_header)} cols, metadata + {data_count} data rows")


def main():
    manager = OptimizedMacroDataSheetsManager()
    spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)

    print(f"Applying row_type + _metadata to 8 tabs in {SPREADSHEET_ID}\n")
    for tab_name, meta in TAB_METADATA.items():
        try:
            fix_tab(manager, spreadsheet, tab_name, meta)
        except Exception as e:
            print(f"  ERROR: {tab_name} — {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
