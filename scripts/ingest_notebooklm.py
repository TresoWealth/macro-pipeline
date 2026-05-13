#!/usr/bin/env python3
"""Generic NotebookLM PDF ingest for active/passive data pipeline.

Supports: Morningstar Barometer, Bain PE (Global + India), EY IPO Trends.
Each source is configured with notebook ID, extraction prompt, output tab,
and expected row count formula.

Implements the NotebookLM Validation Gate (Implementation Doc Section 7):
  - Schema-validate every row
  - Row count check vs expected — >20% deviation = ABORT
  - Always log deviation for audit

Usage:
  python3 ingest_notebooklm.py --source Morningstar --file report.pdf
  python3 ingest_notebooklm.py --source Bain_Global --file bain_pe_2025.pdf
  python3 ingest_notebooklm.py --source Bain_India --file bain_ivca_2025.pdf
  python3 ingest_notebooklm.py --source EY_IPO --file ey_ipo_q1_2026.pdf
"""

import sys
import io
import csv
import re
import argparse
import json
import logging
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, "/home/ubuntu/clawd/treso_analytics")
from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SPREADSHEET_ID = "10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU"

# =============================================================================
# Source Configuration
# =============================================================================

SOURCE_CONFIG = {
    "Morningstar": {
        "notebook_id": "a007d058-0b51-4f45-8948-642926d15611",
        "tab_name": "Morningstar_Barometer",
        "columns": [
            "report_date", "region", "category", "horizon_years",
            "success_rate_equal_wt", "success_rate_asset_wt",
            "survivorship_rate", "data_source", "ingest_timestamp",
        ],
        "extraction_prompt": (
            "Extract the active vs passive success rate table from this report. "
            "For each region, category (fund type), and time horizon (1, 3, 5, 10, 15, 20 years), return:\n"
            "region, category, horizon_years, success_rate_equal_weighted, success_rate_asset_weighted, survivorship_rate\n\n"
            "Rules:\n"
            "- success_rate_equal_weighted and success_rate_asset_weighted: percentage as number (e.g., 45.2 for 45.2%)\n"
            "- survivorship_rate: percentage as number\n"
            "- horizon_years: integer (1, 3, 5, 10, 15, or 20)\n"
            "- region: 'US', 'Europe', 'Global', 'Asia-Pacific', etc.\n"
            "- Include ALL categories and ALL horizons present in the report\n"
            "- Output as CSV with header row. No markdown formatting, just raw CSV.\n"
            "- If a value is 'N/A' or not reported, use empty string"
        ),
        "expected_row_formula": "num_categories * num_horizons * num_regions",
        "grace_days": 90,
    },
    "Bain_Global": {
        "notebook_id": "3c00a3b6-147a-4f79-b688-1ec32f6e0bfd",
        "tab_name": "PE_Activity_Global",
        "columns": [
            "year", "region", "deal_value_usd_bn", "exit_value_usd_bn",
            "fundraising_usd_bn", "dry_powder_usd_bn", "deal_count",
            "exit_count", "median_ev_ebitda", "data_source", "ingest_timestamp",
        ],
        "extraction_prompt": (
            "Extract all private equity activity data from this Bain Global PE Report. "
            "For each year and region, return:\n"
            "year, region, deal_value_usd_bn, exit_value_usd_bn, fundraising_usd_bn, dry_powder_usd_bn, deal_count, exit_count, median_ev_ebitda\n\n"
            "Rules:\n"
            "- year: 4-digit year\n"
            "- region: 'North America', 'Europe', 'Asia-Pacific', 'Global', etc.\n"
            "- deal_value_usd_bn: buyout deal value in USD billions (e.g., 450.5)\n"
            "- exit_value_usd_bn: exit value in USD billions\n"
            "- fundraising_usd_bn: capital raised in USD billions\n"
            "- dry_powder_usd_bn: available unspent capital in USD billions\n"
            "- deal_count: integer number of deals\n"
            "- exit_count: integer number of exits\n"
            "- median_ev_ebitda: median EV/EBITDA multiple for deals (e.g., 12.5)\n"
            "- Include ALL years and ALL regions present\n"
            "- Output as CSV with header row. No markdown formatting.\n"
            "- If a metric is not available for a region/year, use empty string"
        ),
        "expected_row_formula": "num_years * num_regions",
        "grace_days": 120,
    },
    "Bain_India": {
        "notebook_id": "88b88210-0c3f-466c-9eee-8e2ee3f77c23",
        "tab_name": "PE_Activity_India",
        "columns": [
            "year", "vc_investment_usd_bn", "vc_deal_count",
            "exit_value_usd_bn", "exit_channel_ipo_pct",
            "exit_channel_strategic_pct", "exit_channel_secondary_pct",
            "fundraising_usd_bn", "fintech_investment_usd_bn",
            "saas_investment_usd_bn", "data_source", "ingest_timestamp",
        ],
        "extraction_prompt": (
            "Extract all India venture capital and PE activity data from this Bain-IVCA India VC Report. "
            "For each year, return:\n"
            "year, vc_investment_usd_bn, vc_deal_count, exit_value_usd_bn, exit_channel_ipo_pct, "
            "exit_channel_strategic_pct, exit_channel_secondary_pct, fundraising_usd_bn, "
            "fintech_investment_usd_bn, saas_investment_usd_bn\n\n"
            "Rules:\n"
            "- year: 4-digit year\n"
            "- vc_investment_usd_bn: total VC/PE investment in India in USD billions\n"
            "- vc_deal_count: total number of VC/PE deals\n"
            "- exit_value_usd_bn: total exit value in USD billions\n"
            "- exit_channel_*_pct: percentage split of exits by channel (IPO, strategic, secondary). "
            "Should sum to ~100%. Values as numbers (e.g., 35.5 for 35.5%)\n"
            "- fundraising_usd_bn: capital raised by India-focused funds in USD billions\n"
            "- fintech_investment_usd_bn: fintech sector investment in USD billions\n"
            "- saas_investment_usd_bn: SaaS sector investment in USD billions\n"
            "- Include ALL years present in the report\n"
            "- Output as CSV with header row. No markdown formatting.\n"
            "- If a metric is not available for a year, use empty string"
        ),
        "expected_row_formula": "num_years",
        "grace_days": 120,
    },
    "EY_IPO": {
        "notebook_id": "11c901aa-6bfd-4ec6-9bf3-9eeb0a5c94ea",
        "tab_name": "IPO_Activity",
        "columns": [
            "year", "period", "country", "num_ipos", "proceeds_usd_bn",
            "market_share_volume_pct", "market_share_proceeds_pct",
            "avg_deal_size_usd_m", "data_source", "ingest_timestamp",
        ],
        "extraction_prompt": (
            "Extract all IPO activity data from this EY Global IPO Trends report. "
            "For each year, period (Q1/Q2/Q3/Q4 or FY), and country/region, return:\n"
            "year, period, country, num_ipos, proceeds_usd_bn, market_share_volume_pct, "
            "market_share_proceeds_pct, avg_deal_size_usd_m\n\n"
            "Rules:\n"
            "- year: 4-digit year\n"
            "- period: 'Q1', 'Q2', 'Q3', 'Q4', or 'FY'\n"
            "- country: country or exchange name (e.g., 'India', 'US', 'China', 'Global')\n"
            "- num_ipos: integer number of IPOs\n"
            "- proceeds_usd_bn: total IPO proceeds in USD billions\n"
            "- market_share_volume_pct: share of global IPO volume as percentage (e.g., 12.5)\n"
            "- market_share_proceeds_pct: share of global IPO proceeds as percentage\n"
            "- avg_deal_size_usd_m: average deal size in USD millions\n"
            "- Include ALL countries/regions and ALL periods present\n"
            "- Output as CSV with header row. No markdown formatting.\n"
            "- If a metric is not available, use empty string"
        ),
        "expected_row_formula": "num_periods * num_countries",
        "grace_days": 45,
    },
}


def _col_letter(n: int) -> str:
    """Convert column number (1-based) to letter: 1→A, 27→AA."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _notebooklm_upload(notebook_id: str, pdf_path: str) -> bool:
    """Upload PDF to a NotebookLM notebook."""
    cmd = [
        "notebooklm", "sources", "add-pdf",
        notebook_id, pdf_path,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120,
            env={**__import__("os").environ, "PATH": "/home/ubuntu/.local/bin:" + __import__("os").environ.get("PATH", "")},
        )
        if result.returncode == 0:
            logger.info(f"Uploaded {pdf_path} to notebook {notebook_id}")
            return True
        logger.error(f"Upload failed: {result.stderr}")
        return False
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return False


def _notebooklm_query(notebook_id: str, prompt: str) -> Optional[str]:
    """Query a NotebookLM notebook and return the response text."""
    cmd = [
        "notebooklm", "chat", "ask",
        notebook_id, prompt,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300,
            env={**__import__("os").environ, "PATH": "/home/ubuntu/.local/bin:" + __import__("os").environ.get("PATH", "")},
        )
        if result.returncode == 0:
            return result.stdout
        logger.error(f"Query failed: {result.stderr}")
        return None
    except Exception as e:
        logger.error(f"Query error: {e}")
        return None


def _parse_csv_response(response: str) -> list[list[str]]:
    """Parse CSV from NotebookLM response, handling markdown wrapping."""
    # Strip markdown code fences if present
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```csv or ```) and last line (```)
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    # Try to find CSV content in the response
    csv_start = 0
    for i, line in enumerate(text.split("\n")):
        if "," in line and not line.startswith("#"):
            csv_start = i
            break

    text = "\n".join(text.split("\n")[csv_start:])

    try:
        reader = csv.reader(io.StringIO(text))
        rows = [row for row in reader if row and any(c.strip() for c in row)]
        return rows
    except Exception as e:
        logger.error(f"CSV parse error: {e}")
        return []


def _validate_notebooklm_output(rows: list[list[str]], config: dict) -> dict:
    """NotebookLM Validation Gate — Section 7 of implementation doc."""
    issues = []
    if not rows:
        return {"valid": False, "reason": "Zero rows from NotebookLM", "issues": ["No data"], "rows": []}

    # First row should be header
    header = [h.strip().lower().replace(" ", "_") for h in rows[0]]
    data_rows = rows[1:]

    if not data_rows:
        return {"valid": False, "reason": "Header only, no data rows", "issues": ["No data"], "rows": []}

    # Schema-validate: expected columns
    expected_cols = [c.lower().replace(" ", "_") for c in config["columns"]]
    # Remove metadata cols for validation (data_source, ingest_timestamp)
    expected_data_cols = [c for c in expected_cols if c not in ("data_source", "ingest_timestamp")]

    missing_cols = set(expected_data_cols) - set(header)
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")

    # Build column index map
    col_idx = {h: i for i, h in enumerate(header)}

    # Validate each data row
    valid_rows = []
    for i, row in enumerate(data_rows):
        row_issues = []
        # Required non-null fields
        for req_col in expected_data_cols[:2]:  # First 2 cols are typically the keys
            if req_col in col_idx:
                val = row[col_idx[req_col]] if col_idx[req_col] < len(row) else ""
                if not val.strip():
                    row_issues.append(f"Row {i+1}: null {req_col}")

        if row_issues:
            issues.extend(row_issues)
        else:
            valid_rows.append(row)

    # Row count check
    actual = len(valid_rows)
    # Determine expected based on unique values in key columns
    formula = config.get("expected_row_formula", "")
    expected = _compute_expected(valid_rows, col_idx)
    if expected and expected > 0:
        deviation = abs(actual - expected) / expected * 100
        logger.info(f"Row count: actual={actual}, expected={expected}, deviation={deviation:.1f}%")

        if deviation > 20:
            return {
                "valid": False,
                "reason": f"Row count deviation: {deviation:.1f}% (>20% threshold). Actual={actual}, Expected={expected}",
                "issues": issues,
                "rows": valid_rows,
            }
    else:
        deviation = 0
        logger.info(f"Row count: {actual} (could not compute expected)")

    return {
        "valid": True,
        "reason": "OK" if not issues else "; ".join(issues[-3:]),  # Last 3 issues only
        "issues": issues,
        "rows": valid_rows,
        "deviation_pct": deviation,
    }


def _compute_expected(rows: list[list], col_idx: dict) -> Optional[int]:
    """Compute expected row count based on unique combinations of key columns."""
    if not rows or not col_idx:
        return None

    # Find categorical columns (non-numeric) for cardinality check
    categorical_values = {}
    for col_name, idx in col_idx.items():
        vals = set()
        for row in rows:
            if idx < len(row) and row[idx].strip():
                try:
                    float(row[idx].strip())
                except ValueError:
                    vals.add(row[idx].strip())
        if vals:
            categorical_values[col_name] = len(vals)

    if categorical_values:
        # Expected = product of all categorical cardinalities
        expected = 1
        for n in categorical_values.values():
            expected *= n
        return expected
    return None


def _write_metadata(manager, spreadsheet, tab_name: str, as_of: str,
                    row_count: int, status: str, grace_days: int, notes: str = ""):
    """Update _Metadata tab."""
    try:
        today = date.today()
        next_expected = (today + timedelta(days=grace_days)).isoformat()
        ws = manager._get_or_create_worksheet(spreadsheet, "_Metadata")
        records = ws.get_all_values()
        updated = False
        for i, row in enumerate(records):
            if row and row[0] == tab_name:
                ws.update(f"A{i+1}:G{i+1}", [[
                    tab_name, as_of, next_expected,
                    today.isoformat(), row_count, status, notes,
                ]])
                updated = True
                break
        if not updated:
            ws.append_row([
                tab_name, as_of, next_expected,
                today.isoformat(), row_count, status, notes,
            ])
        logger.info(f"Metadata: {tab_name} status={status}, rows={row_count}")
    except Exception as e:
        logger.error(f"Metadata update failed: {e}")


def ingest(source: str, filepath: str):
    """Main entry point."""
    config = SOURCE_CONFIG.get(source)
    if not config:
        logger.error(f"Unknown source: {source}. Valid: {list(SOURCE_CONFIG.keys())}")
        return False

    logger.info(f"=== NotebookLM Ingest: {source} ===")
    manager = OptimizedMacroDataSheetsManager()

    # 1. Upload PDF to NotebookLM
    logger.info(f"Uploading {filepath} to notebook {config['notebook_id']}")
    if not _notebooklm_upload(config["notebook_id"], filepath):
        logger.error("Upload failed")
        return False

    # 2. Query for structured data
    logger.info("Querying NotebookLM...")
    response = _notebooklm_query(config["notebook_id"], config["extraction_prompt"])
    if not response:
        logger.error("Query returned no response")
        return False

    logger.info(f"Response length: {len(response)} chars")

    # 3. Parse CSV from response
    parsed = _parse_csv_response(response)
    logger.info(f"Parsed {len(parsed)} rows (incl header)")

    # 4. Validation Gate
    validation = _validate_notebooklm_output(parsed, config)
    logger.info(f"Validation: {validation['reason']}")

    if not validation["valid"]:
        logger.error(f"VALIDATION GATE FAILED: {validation['reason']}")
        # Write metadata with failure status
        spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)
        _write_metadata(manager, spreadsheet, config["tab_name"],
                        date.today().isoformat(), 0, "error",
                        config["grace_days"],
                        f"Validation failed: {validation['reason']}")
        return False

    # 5. Write to Sheets
    valid_rows = validation["rows"]
    header = [h.strip().lower().replace(" ", "_") for h in parsed[0]]
    col_idx = {h: i for i, h in enumerate(header)}

    spreadsheet = manager._get_spreadsheet(SPREADSHEET_ID)
    ws = manager._get_or_create_worksheet(spreadsheet, config["tab_name"])
    ws.clear()

    ts = datetime.now().isoformat()
    needs_review = validation.get("deviation_pct", 0) > 0
    today_str = date.today().isoformat()
    next_expected = (date.today() + timedelta(days=config["grace_days"])).isoformat()

    # Build header: row_type + data columns + as_of/next_expected/needs_review
    header = ["row_type"] + list(config["columns"]) + ["as_of", "next_expected", "needs_review"]
    n_cols = len(header)
    all_rows = [header]

    # _metadata row
    meta = [""] * n_cols
    meta[0] = "_metadata"
    meta[-3] = today_str       # as_of
    meta[-2] = next_expected    # next_expected
    meta[-1] = str(needs_review).lower()  # needs_review
    all_rows.append(meta)

    # Data rows
    for row in valid_rows:
        row_data = [""]  # row_type
        for col in config["columns"]:
            if col == "data_source":
                row_data.append(f"NotebookLM - {source}")
            elif col == "ingest_timestamp":
                row_data.append(ts)
            elif col in col_idx and col_idx[col] < len(row):
                row_data.append(row[col_idx[col]])
            else:
                row_data.append("")
        row_data += ["", "", ""]  # as_of, next_expected, needs_review
        all_rows.append(row_data)

    # Batch write
    last_col = _col_letter(n_cols)
    ws.update(f"A1:{last_col}{len(all_rows)}", all_rows, value_input_option="USER_ENTERED")
    logger.info(f"Wrote header + metadata + {len(valid_rows)} data rows to {config['tab_name']}")

    # 6. Update _Metadata ops tab
    status = "needs_review" if needs_review else "healthy"
    _write_metadata(manager, spreadsheet, config["tab_name"],
                    date.today().isoformat(), len(valid_rows),
                    status, config["grace_days"],
                    f"NotebookLM ingest {date.today().isoformat()}"
                    + (f" (deviation {validation.get('deviation_pct', 0):.1f}%)" if needs_review else ""))

    logger.info(f"=== {source} Complete: {len(valid_rows)} rows ===")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NotebookLM PDF Ingest")
    parser.add_argument("--source", "-s", required=True,
                        choices=list(SOURCE_CONFIG.keys()),
                        help="Data source to ingest")
    parser.add_argument("--file", "-f", required=True,
                        help="Path to PDF file")
    args = parser.parse_args()

    success = ingest(args.source, args.file)
    sys.exit(0 if success else 1)
