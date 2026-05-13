#!/usr/bin/env python3
"""Ingest SPIVA (S&P Indices vs Active) scorecard data → Google Sheets.

Source: https://www.spglobal.com/spdji/en/spiva/
Frequency: Semi-annual (Year-End ~Mar, Mid-Year ~Sep)
Trigger: Manual — user downloads Excel and provides file path

SPIVA Excel structure (typical):
  - Sheet "India" or "Global": per-region data
  - Columns: Fund Category | Number of Funds (Start) | Number of Funds (End) |
             Benchmark | 1-Year % | 3-Year % | 5-Year % | 10-Year % | Survivors %
  - Multiple horizons per category row
  - Report period in sheet header or filename

Usage:
  python3 ingest_spiva.py --file ~/Downloads/SPIVA_India_YE2025.xlsx
  python3 ingest_spiva.py --file ~/Downloads/SPIVA_Global_YE2025.xlsx --region "US"
"""

import sys
import io
import re
import argparse
import logging
from datetime import date, datetime
from typing import Optional

import pandas as pd

sys.path.insert(0, "/home/ubuntu/clawd/treso_analytics")
from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SPREADSHEET_ID = "10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU"

# Canonical columns for each tab
INDIA_COLS = [
    "report_period", "category", "horizon_years", "pct_underperformed",
    "pct_survivors", "benchmark_name", "num_funds_start", "num_funds_end",
    "data_source", "ingest_timestamp",
]
GLOBAL_COLS = INDIA_COLS + ["region"]

# Known horizon keywords in SPIVA column headers
HORIZON_PATTERNS = {
    "1": 1, "1-Year": 1, "1 Yr": 1, "One Year": 1,
    "3": 3, "3-Year": 3, "3 Yr": 3, "Three Year": 3,
    "5": 5, "5-Year": 5, "5 Yr": 5, "Five Year": 5,
    "10": 10, "10-Year": 10, "10 Yr": 10, "Ten Year": 10,
    "15": 15, "15-Year": 15, "15 Yr": 15, "Fifteen Year": 15,
    "20": 20, "20-Year": 20, "20 Yr": 20, "Twenty Year": 20,
}

# Fund category normalization
CATEGORY_MAP = {
    "Large-Cap": "Large Cap",
    "Large Cap": "Large Cap",
    "LargeCap": "Large Cap",
    "Mid-Cap": "Mid Cap",
    "Mid Cap": "Mid Cap",
    "MidCap": "Mid Cap",
    "Small-Cap": "Small Cap",
    "Small Cap": "Small Cap",
    "SmallCap": "Small Cap",
    "Multi-Cap": "Multi Cap",
    "Multi Cap": "Multi Cap",
    "MultiCap": "Multi Cap",
    "ELSS": "ELSS",
    "Equity Linked Savings Scheme": "ELSS",
    "Flexi Cap": "Flexi Cap",
    "Flexi-Cap": "Flexi Cap",
    "Focused": "Focused",
    "Dividend Yield": "Dividend Yield",
    "Value": "Value/Contra",
    "Contra": "Value/Contra",
    "Sectoral/Thematic": "Sectoral/Thematic",
    "Thematic": "Sectoral/Thematic",
    "Sectoral": "Sectoral/Thematic",
    "Aggressive Hybrid": "Aggressive Hybrid",
    "Balanced": "Balanced",
    "Conservative Hybrid": "Conservative Hybrid",
    "Arbitrage": "Arbitrage",
    "Dynamic Asset Allocation": "Dynamic Asset Allocation",
    "Equity Savings": "Equity Savings",
    "Government Bond": "Government Bond",
    "Corporate Bond": "Corporate Bond",
    "Composite Bond": "Composite Bond",
    "Short Term Bond": "Short Term Bond",
    "Liquid": "Liquid",
    "Indian Equity": "Indian Equity",
    "Indian Bond": "Indian Bond",
}


def _detect_type(filepath: str) -> str:
    """Detect if this is SPIVA India or SPIVA Global based on content."""
    try:
        xl = pd.ExcelFile(filepath)
        sheets = [s.lower() for s in xl.sheet_names]
        combined = " ".join(sheets)
        if "india" in combined or "indian" in combined:
            return "India"
        if any(w in combined for w in ["global", "world", "us", "europe", "latin", "canada", "japan", "australia"]):
            return "Global"
        # Check first sheet content for region hints
        if xl.sheet_names:
            df = xl.parse(xl.sheet_names[0], header=None)
            text = " ".join(str(c) for c in df.iloc[:5, 0] if pd.notna(c))
            if "india" in text.lower():
                return "India"
    except Exception as e:
        logger.warning(f"Type detection failed: {e}")
    return "India"  # Default


def _extract_report_period(df: pd.DataFrame, filename: str) -> str:
    """Extract report period from the spreadsheet or filename."""
    # Check first few rows for date
    for i in range(min(10, len(df))):
        row_text = " ".join(str(c) for c in df.iloc[i] if pd.notna(c))
        # Match: "Year-End 2025", "Mid-Year 2025", "YE 2025", "MY 2025"
        match = re.search(r"(Year-End|Mid-Year|YE|MY)\s*(\d{4})", row_text, re.IGNORECASE)
        if match:
            period_type = match.group(1).upper()
            year = match.group(2)
            if "MID" in period_type or period_type == "MY":
                return f"{year}-H1"
            else:
                return f"{year}-H2"

    # Try filename
    match = re.search(r"(YE|MY|YearEnd|MidYear).*?(\d{4})", filename, re.IGNORECASE)
    if match:
        period_type = match.group(1).upper()
        year = match.group(2)
        if "MID" in period_type or period_type == "MY":
            return f"{year}-H1"
        else:
            return f"{year}-H2"

    logger.warning("Could not extract report period, using filename")
    return filename


def _parse_spiva(filepath: str) -> tuple[pd.DataFrame, str, str]:
    """Parse SPIVA Excel into a standardized DataFrame. Returns (df, report_period, report_type)."""
    xl = pd.ExcelFile(filepath)
    report_type = _detect_type(filepath)
    logger.info(f"Detected type: {report_type} | Sheets: {xl.sheet_names}")

    all_rows = []
    report_period = ""

    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name, header=None)
        if df.empty:
            continue

        # Extract report period from first sheet that has it
        if not report_period:
            report_period = _extract_report_period(df, filepath)

        # Find the header row — typically has "Fund Category", "Benchmark", etc.
        header_row = None
        for i in range(min(20, len(df))):
            row_text = " ".join(str(c).lower() for c in df.iloc[i] if pd.notna(c))
            if any(kw in row_text for kw in ["fund category", "benchmark", "number of funds", "% underperformed"]):
                header_row = i
                break

        if header_row is None:
            logger.warning(f"No header row found in sheet '{sheet_name}', skipping")
            continue

        # Build column mapping from header row
        headers = []
        for c in df.iloc[header_row]:
            h = str(c).strip().lower() if pd.notna(c) else ""
            headers.append(h)

        col_map = {}
        for idx, h in enumerate(headers):
            h_clean = re.sub(r"[^a-z0-9\s%]", "", h).strip()
            if any(kw in h_clean for kw in ["fund category", "category", "fund type"]):
                col_map["category"] = idx
            elif any(kw in h_clean for kw in ["benchmark", "index"]):
                col_map["benchmark"] = idx
            elif any(kw in h_clean for kw in ["number of fund", "fund count", "funds at start", "funds start", "start"]):
                col_map["num_start"] = idx
            elif any(kw in h_clean for kw in ["funds at end", "funds end", "funds remaining", "end"]):
                col_map["num_end"] = idx
            elif "survivor" in h_clean:
                col_map["survivors"] = idx
            # Horizon columns: "1-Year % Underperformed" etc.
            for hkey, hyears in HORIZON_PATTERNS.items():
                hkey_lower = hkey.lower()
                if hkey_lower in h_clean and ("underperform" in h_clean or "%" in h_clean or "pct" in h_clean):
                    col_map[f"horizon_{hyears}"] = idx
                    break

        logger.info(f"Sheet '{sheet_name}': header at row {header_row}, mapped columns: {list(col_map.keys())}")

        # Parse data rows
        for idx in range(header_row + 1, len(df)):
            row = df.iloc[idx]
            category = str(row.iloc[col_map["category"]]).strip() if "category" in col_map else ""
            if not category or category.lower() in ("nan", "", "total", "overall"):
                continue
            if category.startswith("Source:") or category.startswith("Note:"):
                continue

            # Normalize category name
            category = CATEGORY_MAP.get(category, category)

            benchmark = str(row.iloc[col_map["benchmark"]]).strip() if "benchmark" in col_map else ""
            num_start = _safe_int(row, col_map.get("num_start", -1))
            num_end = _safe_int(row, col_map.get("num_end", -1))
            survivors = _safe_pct(row, col_map.get("survivors", -1))

            # Extract each horizon's underperformance
            for key, idx in col_map.items():
                if key.startswith("horizon_"):
                    horizon_years = int(key.split("_")[1])
                    pct_under = _safe_pct(row, idx)
                    all_rows.append({
                        "report_period": report_period,
                        "category": category,
                        "horizon_years": horizon_years,
                        "pct_underperformed": pct_under,
                        "pct_survivors": survivors,
                        "benchmark_name": benchmark,
                        "num_funds_start": num_start,
                        "num_funds_end": num_end,
                    })

    if not all_rows:
        raise ValueError("No data rows extracted from any sheet")

    result = pd.DataFrame(all_rows)
    logger.info(f"Parsed {len(result)} rows ({len(result['category'].unique())} categories)")
    return result, report_period, report_type


def _safe_float(row, col_idx: int) -> float:
    if col_idx < 0 or col_idx >= len(row):
        return 0.0
    try:
        val = row.iloc[col_idx]
        if pd.isna(val):
            return 0.0
        return float(str(val).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _safe_int(row, col_idx: int) -> int:
    if col_idx < 0 or col_idx >= len(row):
        return 0
    try:
        val = row.iloc[col_idx]
        if pd.isna(val):
            return 0
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return 0


def _col_letter(n: int) -> str:
    """Convert column number (1-based) to letter: 1→A, 27→AA."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _safe_pct(row, col_idx: int) -> float:
    """Parse percentage value — may be 0.85 (85%) or 85.0 (85%)."""
    if col_idx < 0 or col_idx >= len(row):
        return 0.0
    try:
        val = row.iloc[col_idx]
        if pd.isna(val):
            return 0.0
        v = float(str(val).replace(",", "").replace("%", "").strip())
        return v if v <= 1.0 else v / 100.0
    except (ValueError, TypeError):
        return 0.0


def _validate(df: pd.DataFrame, tab_name: str) -> dict:
    """Validate parsed SPIVA data."""
    issues = []
    if df.empty:
        return {"valid": False, "reason": "Zero rows", "issues": ["No data"]}

    expected_cols = {"category", "horizon_years", "pct_underperformed"}
    missing = expected_cols - set(df.columns)
    if missing:
        issues.append(f"Missing columns: {missing}")

    # Check horizon coverage
    horizons = df["horizon_years"].unique()
    logger.info(f"Horizons present: {sorted(horizons)}")

    # Check for invalid percentages
    if "pct_underperformed" in df.columns:
        bad = df[df["pct_underperformed"] < 0]
        if len(bad) > 0:
            issues.append(f"{len(bad)} rows with negative underperformance")

    # Category count check
    cats = df["category"].nunique()
    if cats < 3:
        issues.append(f"Only {cats} categories (expected 6+)")

    row_issues = [i for i in issues if "negative" in i.lower()]
    return {
        "valid": len(row_issues) == 0,
        "reason": "; ".join(issues) if issues else "OK",
        "issues": issues,
    }


def _write_metadata(manager, spreadsheet, tab_name: str, report_period: str,
                    row_count: int, status: str, notes: str = ""):
    """Update _Metadata tab."""
    from datetime import timedelta
    try:
        today = date.today()
        next_expected = (today + timedelta(days=90)).isoformat()
        ws = manager._get_or_create_worksheet(spreadsheet, "_Metadata")
        records = ws.get_all_values()
        updated = False
        for i, row in enumerate(records):
            if row and row[0] == tab_name:
                ws.update(f"A{i+1}:G{i+1}", [[
                    tab_name, report_period, next_expected,
                    today.isoformat(), row_count, status, notes,
                ]])
                updated = True
                break
        if not updated:
            ws.append_row([
                tab_name, report_period, next_expected,
                today.isoformat(), row_count, status, notes,
            ])
        logger.info(f"Metadata: {tab_name} status={status}, rows={row_count}")
    except Exception as e:
        logger.error(f"Metadata update failed: {e}")


def ingest(filepath: str, report_type: Optional[str] = None):
    """Main entry point."""
    logger.info(f"=== SPIVA Ingest: {filepath} ===")

    manager = OptimizedMacroDataSheetsManager()

    # 1. Parse
    df, report_period, detected_type = _parse_spiva(filepath)
    report_type = report_type or detected_type
    tab_name = f"SPIVA_{report_type}"
    logger.info(f"Report: {report_period} | Type: {report_type} | Tab: {tab_name}")

    # 2. Validate
    validation = _validate(df, tab_name)
    logger.info(f"Validation: {validation['reason']}")
    if not validation["valid"]:
        logger.error(f"ABORT: {validation['reason']}")
        return False

    # 3. Write to Sheets (batch: header, _metadata row, all data rows)
    spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)
    ws = manager._get_or_create_worksheet(spreadsheet, tab_name)
    ws.clear()

    next_expected = (date.today() + timedelta(days=90)).isoformat()
    ts = datetime.now().isoformat()

    # Build header
    base_header = INDIA_COLS if report_type == "India" else GLOBAL_COLS
    header = ["row_type"] + base_header + ["as_of", "next_expected"]
    n_cols = len(header)
    all_rows = [header]

    # _metadata row
    meta = [""] * n_cols
    meta[0] = "_metadata"
    meta[-2] = report_period  # as_of
    meta[-1] = next_expected   # next_expected
    all_rows.append(meta)

    # Data rows
    for _, row in df.iterrows():
        row_data = [
            "",  # row_type
            row["report_period"], row["category"], row["horizon_years"],
            row["pct_underperformed"], row["pct_survivors"],
            row["benchmark_name"], row["num_funds_start"], row["num_funds_end"],
            "SPIVA Scorecard", ts,
        ]
        if report_type == "Global" and "region" in df.columns:
            row_data.insert(-2, row.get("region", ""))
        row_data += ["", ""]  # as_of, next_expected
        all_rows.append(row_data)

    last_col = _col_letter(n_cols)
    ws.update(f"A1:{last_col}{len(all_rows)}", all_rows, value_input_option="USER_ENTERED")
    logger.info(f"Wrote header + metadata + {len(df)} data rows to {tab_name}")

    # 4. Metadata
    _write_metadata(manager, spreadsheet, tab_name, report_period, len(df),
                    "healthy", f"Manual ingest {date.today().isoformat()}")

    logger.info(f"=== SPIVA Complete: {tab_name} = {len(df)} rows ===")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SPIVA Scorecard Ingest")
    parser.add_argument("--file", "-f", required=True, help="Path to SPIVA Excel file")
    parser.add_argument("--type", "-t", choices=["India", "Global"],
                        help="Report type (auto-detected if not specified)")
    args = parser.parse_args()

    success = ingest(args.file, args.type)
    sys.exit(0 if success else 1)
