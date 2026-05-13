#!/usr/bin/env python3
"""Fixup v2: add as_of, next_expected columns to all active/passive tabs.

Backend _compute_freshness reads metadata_row.get("as_of") and .get("next_expected").
These must be column headers in the tab for get_all_records() to map them as dict keys.

Also ensures row_type column exists and _metadata row is present.
Run on VM: python3 fixup_metadata_v2.py
"""

import sys
from datetime import date, timedelta

sys.path.insert(0, "/home/ubuntu/clawd/treso_analytics")
from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

SPREADSHEET_ID = "10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU"

# Per-tab metadata values
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

# NotebookLM-source tabs that also need needs_review column
NOTEBOOKLM_TABS = {"Morningstar_Barometer", "PE_Activity_Global", "PE_Activity_India", "IPO_Activity"}


def fix_tab(manager, spreadsheet, tab_name, meta):
    ws = manager._get_or_create_worksheet(spreadsheet, tab_name)
    records = ws.get_all_values()

    if not records:
        print(f"  SKIP: {tab_name} — empty")
        return

    header = records[0]
    data_rows = records[1:] if len(records) > 1 else []

    # Check what columns already exist
    has_row_type = header[0] == "row_type" if header else False
    has_as_of = "as_of" in header
    has_next = "next_expected" in header
    has_needs_review = "needs_review" in header
    needs_nr = tab_name in NOTEBOOKLM_TABS

    cols_to_add = []
    if not has_row_type:
        cols_to_add.append("row_type")
    if not has_as_of:
        cols_to_add.append("as_of")
    if not has_next:
        cols_to_add.append("next_expected")
    if needs_nr and not has_needs_review:
        cols_to_add.append("needs_review")

    if not cols_to_add:
        # Check if _metadata row exists
        has_meta = any(r and r[0] == "_metadata" for r in data_rows if r)
        if has_meta:
            print(f"  SKIP: {tab_name} — all columns present + _metadata exists")
            return
        # Insert _metadata row
        n_cols = len(header)
        meta_row = [""] * n_cols
        meta_row[0] = "_metadata"
        as_of_idx = header.index("as_of") if "as_of" in header else -1
        next_idx = header.index("next_expected") if "next_expected" in header else -1
        nr_idx = header.index("needs_review") if "needs_review" in header else -1
        if as_of_idx >= 0:
            meta_row[as_of_idx] = meta["as_of"]
        if next_idx >= 0:
            meta_row[next_idx] = meta["next_expected"]
        if nr_idx >= 0:
            meta_row[nr_idx] = "false"
        ws.insert_row(meta_row, index=2)
        print(f"  FIXED: {tab_name} — inserted _metadata row")
        return

    # Need to add columns. Strategy: read all, clear, rewrite.
    print(f"  FIXING: {tab_name} — adding columns: {cols_to_add}")

    # Build new header
    new_header = list(header)
    if "row_type" in cols_to_add:
        new_header = ["row_type"] + new_header
    for c in ["as_of", "next_expected", "needs_review"]:
        if c in cols_to_add:
            new_header.append(c)

    n_new = len(new_header)

    # Build new rows
    new_rows = []

    # _metadata row
    meta_row = [""] * n_new
    meta_row[0] = "_metadata"
    if "as_of" in new_header:
        meta_row[new_header.index("as_of")] = meta["as_of"]
    if "next_expected" in new_header:
        meta_row[new_header.index("next_expected")] = meta["next_expected"]
    if "needs_review" in new_header:
        meta_row[new_header.index("needs_review")] = "false"
    new_rows.append(meta_row)

    # Existing data rows — pad with empty values for new columns
    old_n = len(header)
    for row in data_rows:
        if row and row[0] == "_metadata":
            continue
        nr = list(row) + [""] * (n_new - len(row))
        # If row_type was just added, all existing data rows have no row_type → prepend empty
        if "row_type" in cols_to_add and nr[0] != "":
            nr = [""] + nr
            nr = nr[:n_new]  # Truncate any extra
        # Pad to exact length
        while len(nr) < n_new:
            nr.append("")
        nr = nr[:n_new]
        new_rows.append(nr)

    # Clear and rewrite
    ws.clear()
    ws.append_row(new_header)
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")

    data_count = len([r for r in new_rows if r[0] != "_metadata"])
    print(f"  DONE: {tab_name} — {len(new_header)} cols, _metadata + {data_count} data rows")


def main():
    manager = OptimizedMacroDataSheetsManager()
    spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)

    print(f"Fixing 8 tabs: adding as_of, next_expected columns + _metadata row\n")
    for tab_name, meta in TAB_METADATA.items():
        try:
            fix_tab(manager, spreadsheet, tab_name, meta)
        except Exception as e:
            print(f"  ERROR: {tab_name} — {e}")
            import traceback
            traceback.print_exc()

    print("\nDone.")


if __name__ == "__main__":
    main()
