#!/usr/bin/env python3
"""Ingest Indian mutual fund universe from mfapi.in → Google Sheets.

Source: https://api.mfapi.in/ (community-maintained, no official API)
Frequency: Monthly
Trigger: Cron (Sun 6:35 PM IST) or manual

Resilience strategy:
  - Primary: GET /mf?limit=100&offset=N (paginated full list)
  - Fallback A: Letter-by-letter search via /mf/search?q=X to enumerate
  - Fallback B: Use last-known-good scheme list from Sheets, refresh NAV only
  - Serve-stale: if all paths fail, keep existing data and log warning
"""

import sys
import time
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import requests

sys.path.insert(0, "/home/ubuntu/clawd/treso_analytics")
from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SPREADSHEET_ID = "10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU"
TAB_NAME = "MFAPI_Fund_Universe"

MFAPI_BASE = "https://api.mfapi.in/mf"
HEADERS = {"User-Agent": "TresoWealth-Analytics/1.0", "Accept": "application/json"}
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
BATCH_SIZE = 100

CANONICAL_COLS = [
    "scheme_code", "scheme_name", "fund_house", "scheme_type",
    "scheme_category", "isin_growth", "is_active",
    "category_group", "latest_nav", "nav_date", "scrape_date",
    "data_source", "ingest_timestamp",
]
HEADER_ROW = ["row_type"] + CANONICAL_COLS + ["as_of", "next_expected"]

# Keywords that indicate an inactive/passive scheme
INACTIVE_TYPES = {"Index Fund", "ETF", "Fund of Funds", "FOF"}
INACTIVE_CATEGORY_KW = ["Index", "ETF", "FoF", "Fund of Funds"]

# Category → group mapping
CATEGORY_GROUP_MAP = {
    "Large Cap": "Large", "Large & Mid Cap": "Large",
    "Mid Cap": "Mid", "Small Cap": "Small",
    "Flexi Cap": "Flexi", "Multi Cap": "Flexi",
    "ELSS": "ELSS", "Focused": "Flexi",
    "Value": "Flexi", "Contra": "Flexi", "Dividend Yield": "Flexi",
    "Sectoral": "Other", "Thematic": "Other",
    "Liquid": "Debt", "Money Market": "Debt",
    "Overnight": "Debt", "Ultra Short Duration": "Debt",
    "Low Duration": "Debt", "Short Duration": "Debt",
    "Medium Duration": "Debt", "Medium to Long Duration": "Debt",
    "Long Duration": "Debt", "Dynamic Bond": "Debt",
    "Corporate Bond": "Debt", "Credit Risk": "Debt",
    "Banking and PSU": "Debt", "Gilt": "Debt", "Floater": "Debt",
    "Arbitrage": "Hybrid", "Aggressive Hybrid": "Hybrid",
    "Conservative Hybrid": "Hybrid", "Dynamic Asset Allocation": "Hybrid",
    "Multi Asset Allocation": "Hybrid", "Equity Savings": "Hybrid",
    "Balanced": "Hybrid", "Balanced Hybrid": "Hybrid",
    "Index": "Index", "ETF": "Index",
    "Fund of Funds": "Other", "FOF": "Other",
    "Solution Oriented": "Other", "Retirement": "Other",
    "Children": "Other",
}


def _col_letter(n: int) -> str:
    """Convert column number (1-based) to letter: 1→A, 27→AA."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _derive_category_group(scheme_category: str, scheme_type: str) -> str:
    """Derive simplified category group from scheme category and type."""
    cat = scheme_category or ""
    stype = scheme_type or ""

    # Check inactive first
    for kw in INACTIVE_CATEGORY_KW:
        if kw.lower() in cat.lower() or kw.lower() in stype.lower():
            return "Index"

    # Direct match
    for key, group in CATEGORY_GROUP_MAP.items():
        if key.lower() in cat.lower():
            return group

    # Fallback based on type
    if "Debt" in cat or "Bond" in cat or "Gilt" in cat or "Liquid" in cat or "Money Market" in cat:
        return "Debt"
    if "Equity" in cat or "ELSS" in cat:
        return "Flexi"
    if "Hybrid" in cat or "Balanced" in cat or "Arbitrage" in cat:
        return "Hybrid"

    return "Other"


def _derive_is_active(scheme_type: str, scheme_category: str) -> bool:
    """Determine if a scheme is an active (non-index, non-ETF, non-FoF) fund."""
    stype = (scheme_type or "").lower()
    scat = (scheme_category or "").lower()
    inactive_kw = ["index", "etf", "fund of funds", "fof"]
    for kw in inactive_kw:
        if kw in stype or kw in scat:
            return False
    return True


def _fetch_paginated(session: requests.Session) -> Optional[list[dict]]:
    """Primary path: paginate /mf endpoint."""
    all_schemes = []
    offset = 0

    while True:
        url = f"{MFAPI_BASE}?limit={BATCH_SIZE}&offset={offset}"
        logger.info(f"Fetching offset={offset}")

        for attempt in range(MAX_RETRIES):
            try:
                resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.Timeout:
                logger.warning(f"Timeout at offset={offset}, attempt {attempt+1}/{MAX_RETRIES}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(5 * (attempt + 1))
            except requests.RequestException as e:
                logger.warning(f"Request error at offset={offset}: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(5 * (attempt + 1))

        if not data or (isinstance(data, list) and len(data) == 0):
            break

        all_schemes.extend(data if isinstance(data, list) else [])
        offset += BATCH_SIZE

        if len(data) < BATCH_SIZE:
            break

        time.sleep(0.5)  # Rate limit

    logger.info(f"Paginated {len(all_schemes)} schemes total")
    return all_schemes if all_schemes else None


def _fetch_via_search(session: requests.Session) -> Optional[list[dict]]:
    """Fallback A: letter-by-letter search to enumerate all schemes."""
    all_schemes = {}
    # Common prefixes for Indian mutual funds
    prefixes = [chr(c) for c in range(ord("a"), ord("z") + 1)]

    for prefix in prefixes:
        url = f"{MFAPI_BASE}/search?q={prefix}"
        logger.info(f"Search prefix: '{prefix}'")
        try:
            resp = session.get(url, headers=HEADERS, timeout=60)
            if resp.status_code == 502:
                logger.warning(f"502 for prefix '{prefix}' — API backend down")
                return None
            resp.raise_for_status()
            results = resp.json()
            if isinstance(results, list):
                for s in results:
                    code = s.get("schemeCode")
                    if code and code not in all_schemes:
                        all_schemes[code] = s
            logger.info(f"  Found {len(results) if isinstance(results, list) else 0} schemes")
        except Exception as e:
            logger.warning(f"Search '{prefix}' failed: {e}")
            continue
        time.sleep(0.3)

    logger.info(f"Search enumeration: {len(all_schemes)} unique schemes")
    return list(all_schemes.values()) if all_schemes else None


def _fetch_scheme_details(session: requests.Session, scheme_codes: list[int]) -> list[dict]:
    """Fetch detailed metadata + latest NAV for each scheme."""
    results = []
    total = len(scheme_codes)
    ts = datetime.now().isoformat()

    for i, code in enumerate(scheme_codes):
        if i % 50 == 0:
            logger.info(f"Detail fetch: {i}/{total}")

        try:
            resp = session.get(f"{MFAPI_BASE}/{code}", headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                continue
            data = resp.json()
        except Exception:
            continue

        meta = data.get("meta", {}) if isinstance(data, dict) else {}
        nav_data = data.get("data", []) if isinstance(data, dict) else []

        scheme_name = meta.get("scheme_name", "")
        fund_house = meta.get("fund_house", "")
        scheme_type = meta.get("scheme_type", "")
        scheme_category = meta.get("scheme_category", "")
        isin = meta.get("isin_growth", "") or meta.get("isin_div_reinvestment", "")

        is_active = _derive_is_active(scheme_type, scheme_category)
        category_group = _derive_category_group(scheme_category, scheme_type)

        latest_nav = None
        nav_date_str = ""
        if nav_data and isinstance(nav_data, list):
            latest = nav_data[0]
            latest_nav = latest.get("nav")
            nav_date_str = latest.get("date", "")

        results.append({
            "scheme_code": code,
            "scheme_name": scheme_name,
            "fund_house": fund_house,
            "scheme_type": scheme_type,
            "scheme_category": scheme_category,
            "isin_growth": isin,
            "is_active": is_active,
            "category_group": category_group,
            "latest_nav": latest_nav,
            "nav_date": nav_date_str,
            "scrape_date": date.today().isoformat(),
            "data_source": "mfapi.in",
            "ingest_timestamp": ts,
        })

        time.sleep(0.1)  # Rate limit

    logger.info(f"Detail fetch complete: {len(results)} schemes")
    return results


def _load_existing_scheme_codes(manager: OptimizedMacroDataSheetsManager) -> list[int]:
    """Read existing scheme codes from Sheets (for serve-stale fallback)."""
    try:
        spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)
        ws = manager._get_or_create_worksheet(spreadsheet, TAB_NAME)
        records = ws.get_all_values()
        codes = []
        for row in records[1:]:  # Skip header
            if row and row[0].isdigit():
                codes.append(int(row[0]))
        logger.info(f"Loaded {len(codes)} existing scheme codes from Sheets")
        return codes
    except Exception as e:
        logger.warning(f"Could not load existing codes: {e}")
        return []


def _validate(rows: list[dict]) -> dict:
    """Validate extracted rows."""
    issues = []

    if not rows:
        return {"valid": False, "reason": "Zero rows", "issues": ["No data"]}

    if len(rows) < 1000:
        issues.append(f"Suspiciously few schemes: {len(rows)} (expected 10,000+)")

    # Required non-null fields
    null_scheme_code = sum(1 for r in rows if not r["scheme_code"])
    null_scheme_name = sum(1 for r in rows if not r["scheme_name"])
    null_fund_house = sum(1 for r in rows if not r["fund_house"])

    if null_scheme_code > 0:
        issues.append(f"{null_scheme_code} rows with null scheme_code")
    if null_scheme_name > len(rows) * 0.1:
        issues.append(f"{null_scheme_name} rows with null scheme_name (>10%)")
    if null_fund_house > len(rows) * 0.1:
        issues.append(f"{null_fund_house} rows with null fund_house (>10%)")

    # Category group coverage
    groups = {}
    for r in rows:
        g = r["category_group"]
        groups[g] = groups.get(g, 0) + 1
    logger.info(f"Category groups: {groups}")

    active_count = sum(1 for r in rows if r["is_active"])
    logger.info(f"Active: {active_count}, Passive/Index/ETF/FoF: {len(rows) - active_count}")

    return {
        "valid": len(issues) == 0 or all("Suspiciously few" not in i for i in issues),
        "reason": "; ".join(issues) if issues else "OK",
        "issues": issues,
    }


def _write_metadata(manager: OptimizedMacroDataSheetsManager, spreadsheet,
                    row_count: int, status: str, notes: str = ""):
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
                    TAB_NAME, today.isoformat(), next_expected,
                    today.isoformat(), row_count, status, notes,
                ]])
                updated = True
                break
        if not updated:
            ws.append_row([
                TAB_NAME, today.isoformat(), next_expected,
                today.isoformat(), row_count, status, notes,
            ])
        logger.info(f"Metadata updated: status={status}, rows={row_count}")
    except Exception as e:
        logger.error(f"Metadata update failed: {e}")


def ingest():
    """Main entry point."""
    logger.info("=== mfapi.in Fund Universe Ingest ===")

    manager = OptimizedMacroDataSheetsManager()
    session = requests.Session()

    # 1. Try primary paginated endpoint
    scheme_list = None
    path_used = "none"

    try:
        scheme_list = _fetch_paginated(session)
        if scheme_list:
            path_used = "paginated"
    except Exception as e:
        logger.warning(f"Primary path failed: {e}")

    # 2. Fallback A: letter-by-letter search
    if not scheme_list:
        logger.info("Trying search fallback...")
        try:
            scheme_list = _fetch_via_search(session)
            if scheme_list:
                path_used = "search"
        except Exception as e:
            logger.warning(f"Search fallback failed: {e}")

    # 3. Fallback B: use existing codes from Sheets, just refresh NAV
    if not scheme_list:
        logger.info("Trying serve-stale: refreshing NAV for existing codes...")
        existing_codes = _load_existing_scheme_codes(manager)
        if existing_codes:
            scheme_list = [{"schemeCode": c} for c in existing_codes]
            path_used = "serve-stale"
            logger.info(f"Serve-stale: refreshing {len(existing_codes)} existing schemes")

    if not scheme_list:
        logger.error("All paths failed — no scheme data available")
        _write_metadata(manager, manager._get_spreadsheet(SPREADSHEET_ID),
                        0, "error", "All fetch paths failed")
        return False

    # 4. Fetch details (NAV, metadata) for all schemes
    scheme_codes = [s["schemeCode"] for s in scheme_list if s.get("schemeCode")]
    logger.info(f"Fetching details for {len(scheme_codes)} schemes...")
    rows = _fetch_scheme_details(session, scheme_codes)

    # 5. Validate
    validation = _validate(rows)
    logger.info(f"Validation: {validation['reason']}")

    # 6. Write to Sheets (batch: header, _metadata row, data in chunks)
    spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)
    ws = manager._get_or_create_worksheet(spreadsheet, TAB_NAME)
    ws.clear()

    next_expected = (date.today() + timedelta(days=35)).isoformat()
    today_str = date.today().isoformat()
    n_cols = len(HEADER_ROW)

    all_batch_rows = [HEADER_ROW]

    # _metadata row
    meta = [""] * n_cols
    meta[0] = "_metadata"
    meta[-2] = today_str   # as_of
    meta[-1] = next_expected  # next_expected
    all_batch_rows.append(meta)

    # Data rows
    for r in rows:
        row_data = [""] + [  # row_type = ""
            r["scheme_code"], r["scheme_name"], r["fund_house"],
            r["scheme_type"], r["scheme_category"], r["isin_growth"],
            str(r["is_active"]).lower(), r["category_group"],
            str(r["latest_nav"]) if r["latest_nav"] is not None else "",
            r["nav_date"], r["scrape_date"],
            r["data_source"], r["ingest_timestamp"],
            "", ""  # as_of, next_expected
        ]
        all_batch_rows.append(row_data)

    # Chunked batch write (500 rows per update to avoid payload limits)
    CHUNK = 500
    for i in range(0, len(all_batch_rows), CHUNK):
        chunk = all_batch_rows[i:i + CHUNK]
        start_row = i + 1
        end_row = start_row + len(chunk) - 1
        last_col_letter = _col_letter(n_cols)
        ws.update(f"A{start_row}:{last_col_letter}{end_row}", chunk, value_input_option="USER_ENTERED")
        logger.info(f"Wrote rows {start_row}-{end_row}")

    logger.info(f"Wrote header + metadata + {len(rows)} data rows to {TAB_NAME}")

    # 7. Metadata
    status = "healthy" if path_used != "serve-stale" else "stale"
    _write_metadata(manager, spreadsheet, len(rows), status,
                    f"Path: {path_used}, validation: {validation['reason']}")

    logger.info(f"=== mfapi.in Complete: {len(rows)} rows (path={path_used}) ===")
    return True


if __name__ == "__main__":
    success = ingest()
    sys.exit(0 if success else 1)
