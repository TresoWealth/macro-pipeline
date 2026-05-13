#!/usr/bin/env python3
"""Ingest AMFI monthly AUM/flow/folio data → Google Sheets.

Source: https://portal.amfiindia.com/spages/am{mon3}{yyyy}repo.xls
Frequency: Monthly (~7th-10th of each month for prior month data)
Trigger: Cron (Sun 6:35 PM IST) or manual

Excel structure (MCR_Report sheet):
  Row 0: empty
  Row 1: "Monthly Report for <Month> <Year>"
  Row 2: Multi-column header
  Rows 3+: Section headers (A-I, i-xviii) + data rows
  Col 0: Sr. No. (roman numeral or letter)
  Col 1: Category name
  Col 2: No. of Schemes
  Col 3: No. of Folios
  Col 4: Funds Mobilized (INR Cr)
  Col 5: Repurchase/Redemption (INR Cr)
  Col 6: Net Inflow/Outflow (INR Cr)
  Col 7: AUM (INR Cr)
  Col 8: Average AUM (INR Cr)

Known gap: SIP contributions not in this report (separate AMFI publication).
"""

import sys
import io
import re
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import requests
import pandas as pd

sys.path.insert(0, "/home/ubuntu/clawd/treso_analytics")
from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SPREADSHEET_ID = "10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU"
TAB_NAME = "AMFI_Monthly"
AMFI_URL = "https://portal.amfiindia.com/spages/am{mon3}{yyyy}repo.xls"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Full header with row_type + metadata columns (backend contract: analytics_api.py)
# _compute_freshness reads as_of/next_expected from the _metadata row dict
HEADER_ROW = [
    "row_type", "month", "category", "category_type",
    "aum_inr_cr", "net_inflows_inr_cr", "folio_count",
    "sip_contributions_inr_cr", "data_source", "ingest_timestamp",
    "as_of", "next_expected",
]

# Section context: Roman numeral section → category_type
# AMFI hierarchy: top-level (A/B/C) → sub-section (I-V) → categories (i-xviii)
# We track the sub-section to classify individual categories
SECTION_MAP = {
    "I": "Debt",       # Income/Debt Oriented Schemes
    "II": "Equity",    # Growth/Equity Oriented Schemes
    "III": "Hybrid",   # Hybrid Schemes
    "IV": "Other",     # Solution Oriented Schemes
    "V": "Index",      # Other Schemes (Index Funds, ETFs, FoFs)
}

# Per-category overrides within sections (AMFI category name → type)
CATEGORY_TYPE_OVERRIDES = {
    "Index Funds": "Index",
    "ETFs": "Index",
    "Fund of Funds": "Other",
    "Solution Oriented Schemes": "Other",
}

# Debt sub-types that map to "Debt"
DEBT_CATEGORIES = {
    "Overnight Fund", "Liquid Fund", "Ultra Short Duration Fund",
    "Low Duration Fund", "Money Market Fund", "Short Duration Fund",
    "Medium Duration Fund", "Medium to Long Duration Fund",
    "Long Duration Fund", "Dynamic Bond Fund", "Corporate Bond Fund",
    "Credit Risk Fund", "Banking and PSU Fund", "Gilt Fund",
    "Gilt Fund with 10 year constant duration", "Floater Fund",
}

# Equity sub-types that map to "Equity"
EQUITY_CATEGORIES = {
    "Large Cap Fund", "Large & Mid Cap Fund", "Mid Cap Fund",
    "Small Cap Fund", "Flexi Cap Fund", "Multi Cap Fund",
    "ELSS", "Focused Fund", "Value Fund", "Contra Fund",
    "Dividend Yield Fund", "Sectoral/Thematic Funds",
}

# Hybrid sub-types
HYBRID_CATEGORIES = {
    "Arbitrage Fund", "Balanced Hybrid Fund", "Aggressive Hybrid Fund",
    "Conservative Hybrid Fund", "Dynamic Asset Allocation Fund",
    "Multi Asset Allocation Fund", "Equity Savings Fund",
}


def _build_url(report_date: date) -> str:
    """Build AMFI URL from report date: amapr2026repo.xls for April 2026."""
    mon3 = report_date.strftime("%b").lower()  # apr, may, jun...
    yyyy = report_date.year
    return AMFI_URL.format(mon3=mon3, yyyy=yyyy)


def _col_letter(n: int) -> str:
    """Convert column number (1-based) to letter: 1→A, 27→AA."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _find_url(session: requests.Session, target_date: date) -> Optional[str]:
    """Try current month URL, fall back to prior months."""
    for months_back in range(3):
        # Proper month arithmetic: subtract months, not days
        y, m = target_date.year, target_date.month
        m -= months_back
        while m < 1:
            m += 12
            y -= 1
        dt = date(y, m, 1)
        url = _build_url(dt)
        try:
            resp = session.head(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                logger.info(f"Found: {url}")
                return url
        except Exception:
            continue
        logger.debug(f"Not found: {url}")

    # Fallback: scrape the listing page
    try:
        resp = session.get(
            "https://www.amfiindia.com/research-information/amfi-monthly",
            headers=HEADERS, timeout=30,
        )
        matches = re.findall(r'href="(https?://portal\.amfiindia\.com/[^"]+repo\.xls)"', resp.text)
        if matches:
            logger.info(f"Scraped URL: {matches[0]}")
            return matches[0]
    except Exception as e:
        logger.warning(f"Scrape fallback failed: {e}")

    return None


def _parse_report(df: pd.DataFrame) -> tuple[str, list[dict]]:
    """Parse MCR_Report sheet into canonical rows. Returns (report_month, rows)."""
    # Extract report month from row 1
    report_month = ""
    month_match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
        str(df.iloc[1, 0]) if len(df) > 1 else "", re.IGNORECASE,
    )
    if month_match:
        mon_str, year_str = month_match.groups()
        mon_num = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }.get(mon_str.lower(), 1)
        report_month = f"{year_str}-{mon_num:02d}"
        logger.info(f"Report month: {report_month}")
    else:
        # Fallback: prior month
        today = date.today()
        prev = today.replace(day=1) - timedelta(days=1)
        report_month = prev.strftime("%Y-%m")
        logger.warning(f"Could not parse report month, using {report_month}")

    rows = []
    ts = datetime.now().isoformat()
    current_section = None  # "Debt", "Equity", "Hybrid", "Index", "Other"
    top_level = None  # "Open", "Close", "Interval"

    for idx, row in df.iterrows():
        if idx < 3:
            continue  # Skip title + header rows

        col0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
        col1 = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ""

        if not col1:
            continue

        # Detect top-level blocks: A = Open Ended, B = Close Ended, C = Interval
        top_match = re.match(r"^([ABC])\b", col0)
        if top_match and not _has_numbers(row):
            top_map = {"A": "Open", "B": "Close", "C": "Interval"}
            top_level = top_map.get(top_match.group(1), top_level)
            current_section = None
            continue

        # Detect section headers (Roman numerals I, II, III, IV, V)
        section_match = re.match(r"^(I{1,3}|IV|V)\b", col0)
        if section_match:
            section_key = section_match.group(1)
            current_section = SECTION_MAP.get(section_key, current_section)
            # Also check if this is a summary row that has numbers
            if not _has_numbers(row):
                continue

        # Detect sub-totals / parent section labels
        if col1 in ("Open Ended Schemes", "Close Ended Schemes", "Interval Schemes", "Other Schemes"):
            continue
        if col1.startswith("Total") or col1.startswith("Grand Total") or col1.startswith("Sub Total"):
            continue
        if col1 in CATEGORY_TYPE_OVERRIDES:
            continue  # These are section labels, not data rows

        # Check if this is a data row (has numeric data in columns 2-8)
        if not _has_numbers(row):
            continue

        # Determine category type
        category_type = _classify_category(col1, current_section)

        # Extract numeric fields
        schemes = _safe_int(row, 2)
        folios = _safe_int(row, 3)
        funds_mob = _safe_float(row, 4)
        repurchase = _safe_float(row, 5)
        net_flow = _safe_float(row, 6)
        aum = _safe_float(row, 7)

        # Clean category name, disambiguate with top-level context
        cat_name = re.sub(r"\s+", " ", col1.replace("  ", " ")).strip()
        if top_level and top_level != "Open":
            cat_name = f"{cat_name} ({top_level})"

        rows.append({
            "month": report_month,
            "category": cat_name,
            "category_type": category_type,
            "aum_inr_cr": aum,
            "net_inflows_inr_cr": net_flow,
            "folio_count": folios,
            "sip_contributions_inr_cr": 0.0,  # Not in this report
            "num_schemes": schemes,
            "funds_mobilized_inr_cr": funds_mob,
            "repurchase_inr_cr": repurchase,
            "data_source": "AMFI Monthly Report",
            "ingest_timestamp": ts,
        })

    return report_month, rows


def _has_numbers(row) -> bool:
    """Check if a row has numeric data in the value columns (cols 2-8)."""
    for i in range(2, min(9, len(row))):
        try:
            val = row.iloc[i]
            if pd.notna(val) and str(val).strip():
                float(str(val).replace(",", ""))
                return True
        except (ValueError, TypeError):
            continue
    return False


def _safe_float(row, col: int) -> float:
    """Safely extract float from a row column."""
    try:
        if col >= len(row):
            return 0.0
        val = row.iloc[col]
        if pd.isna(val):
            return 0.0
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _safe_int(row, col: int) -> int:
    """Safely extract int from a row column."""
    try:
        if col >= len(row):
            return 0
        val = row.iloc[col]
        if pd.isna(val):
            return 0
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return 0


def _classify_category(category_name: str, section: Optional[str]) -> str:
    """Determine category_type for a given AMFI category."""
    # Direct match
    for cat_set, ctype in [
        (DEBT_CATEGORIES, "Debt"),
        (EQUITY_CATEGORIES, "Equity"),
        (HYBRID_CATEGORIES, "Hybrid"),
    ]:
        if category_name in cat_set:
            return ctype

    # Section-based fallback
    if section:
        return section

    # Keyword fallback
    cat_lower = category_name.lower()
    if any(kw in cat_lower for kw in ["debt", "bond", "gilt", "liquid", "money market", "overnight", "floater"]):
        return "Debt"
    if any(kw in cat_lower for kw in ["equity", "elss", "large cap", "mid cap", "small cap", "flexi cap", "multi cap", "focused", "value", "contra", "dividend yield", "sectoral", "thematic"]):
        return "Equity"
    if any(kw in cat_lower for kw in ["hybrid", "arbitrage", "balanced", "asset allocation", "equity savings"]):
        return "Hybrid"
    if any(kw in cat_lower for kw in ["fund of fund", "fof"]):
        return "Other"
    if any(kw in cat_lower for kw in ["index", "etf"]):
        return "Index"

    return "Other"


def _validate(rows: list[dict]) -> dict:
    """Validate extracted rows."""
    issues = []
    if not rows:
        return {"valid": False, "reason": "Zero rows extracted", "issues": issues}

    total_aum = sum(r["aum_inr_cr"] for r in rows)
    if total_aum < 1_000_000:  # Industry AUM is ~₹50-60L Cr
        issues.append(f"Total AUM suspiciously low: ₹{total_aum:,.0f} Cr (expected >₹10L Cr)")

    cats = [r["category"] for r in rows]
    dups = set(c for c in cats if cats.count(c) > 1)
    if dups:
        issues.append(f"Duplicate categories: {dups}")

    return {
        "valid": len([i for i in issues if "suspiciously low" in i]) == 0,
        "reason": "; ".join(issues) if issues else "OK",
        "issues": issues,
    }


def _write_metadata(manager, spreadsheet, report_month: str, row_count: int,
                    status: str, notes: str = ""):
    """Update _Metadata tab."""
    try:
        today = date.today()
        next_expected = (today + timedelta(days=35)).isoformat()
        ws = manager._get_or_create_worksheet(spreadsheet, "_Metadata")
        records = ws.get_all_values()
        updated = False
        for i, row in enumerate(records):
            if row and row[0] == TAB_NAME:
                ws.update(f"A{i+1}:G{i+1}", [[
                    TAB_NAME, report_month, next_expected,
                    today.isoformat(), row_count, status, notes,
                ]])
                updated = True
                break
        if not updated:
            ws.append_row([
                TAB_NAME, report_month, next_expected,
                today.isoformat(), row_count, status, notes,
            ])
        logger.info(f"Metadata: status={status}, rows={row_count}")
    except Exception as e:
        logger.error(f"Metadata update failed: {e}")


def ingest():
    """Main entry point."""
    logger.info("=== AMFI Monthly Ingest ===")

    manager = OptimizedMacroDataSheetsManager()
    session = requests.Session()

    # 1. Find latest URL
    url = _find_url(session, date.today())
    if not url:
        logger.error("Could not find AMFI Excel URL")
        return False

    # 2. Download
    logger.info(f"Downloading: {url}")
    try:
        resp = session.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return False

    # 3. Parse
    try:
        xl = pd.ExcelFile(io.BytesIO(resp.content))
    except Exception as e:
        logger.error(f"Cannot open Excel: {e}")
        return False

    # Handle format change: April 2026+ uses "MCR_Report", earlier uses "AMFI MONTHLY"
    if "MCR_Report" in xl.sheet_names:
        sheet_name = "MCR_Report"
    elif "AMFI MONTHLY" in xl.sheet_names:
        sheet_name = "AMFI MONTHLY"
    else:
        logger.error(f"Neither MCR_Report nor AMFI MONTHLY found. Available: {xl.sheet_names}")
        return False

    df = xl.parse(sheet_name, header=None)
    report_month, rows = _parse_report(df)
    logger.info(f"Extracted {len(rows)} category rows for {report_month}")

    # 4. Validate
    validation = _validate(rows)
    logger.info(f"Validation: {validation['reason']}")
    if not validation["valid"]:
        logger.error(f"ABORT: {validation['reason']}")
        return False

    # 5. Write to Sheets (batch: header, _metadata row, all data rows)
    spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)
    ws = manager._get_or_create_worksheet(spreadsheet, TAB_NAME)
    ws.clear()

    next_expected = (date.today() + timedelta(days=35)).isoformat()
    batch_rows = [HEADER_ROW]

    # _metadata row (first data row, per implementation doc Section 8)
    batch_rows.append([
        "_metadata",   # row_type
        report_month,  # month (as_of value)
        "",            # category
        "",            # category_type
        0,             # aum_inr_cr
        0,             # net_inflows_inr_cr
        0,             # folio_count
        0,             # sip_contributions_inr_cr
        "",            # data_source
        "",            # ingest_timestamp
        report_month,  # as_of (backend reads this key)
        next_expected, # next_expected (backend reads this key)
    ])

    # Data rows with empty row_type and metadata columns
    for r in rows:
        batch_rows.append([
            "",  # row_type
            r["month"], r["category"], r["category_type"],
            r["aum_inr_cr"], r["net_inflows_inr_cr"], r["folio_count"],
            r["sip_contributions_inr_cr"], r["data_source"], r["ingest_timestamp"],
            "",  # as_of (empty for data rows)
            "",  # next_expected (empty for data rows)
        ])

    # Single batch update to avoid per-row API rate limiting
    ws.update(f"A1:{_col_letter(len(HEADER_ROW))}{len(batch_rows)}",
              batch_rows, value_input_option="USER_ENTERED")

    logger.info(f"Wrote header + metadata + {len(rows)} data rows to {TAB_NAME}")

    # 6. Update _Metadata ops tab
    _write_metadata(manager, spreadsheet, report_month, len(rows),
                    "healthy", f"Auto-ingested {date.today().isoformat()}")

    logger.info(f"=== AMFI Monthly Complete: {len(rows)} rows ===")
    return True


if __name__ == "__main__":
    success = ingest()
    sys.exit(0 if success else 1)
