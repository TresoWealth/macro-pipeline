#!/usr/bin/env python3
"""
One-shot migration: copy 17 macro data tabs from old fund data sheet
to the new Mia Google Workspace macro data sheet.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gspread
from google.oauth2.service_account import Credentials
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

OLD_SHEET = '1fg31jIqeaPTzUp2GiG1z_K4vyy_5myfkiaQKE2PKu5s'
NEW_SHEET = '10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU'
SERVICE_ACCOUNT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'service_account.json')

MACRO_TABS = [
    'global_macro',
    'market_data',
    'rbi_indicators',
    'india_macro',
    'RBI_Data',
    'Inflation_Data',
    'Growth_Data',
    'Regime_Classification',
    'Audit_Log',
    'Exchange_Rates',
    'Exchange_Rates_Monthly',
    'Oil_WTI_Monthly',
    'Oil_Brent_Monthly',
    'US_CPI_Monthly',
    'RBI_Historical_Real',
    'Inflation_Historical_Real',
    'Growth_Historical_Real',
    'NSE_Historical_Real',
]

def main():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT,
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    )
    gc = gspread.authorize(creds)

    old_sh = gc.open_by_key(OLD_SHEET)
    new_sh = gc.open_by_key(NEW_SHEET)
    existing_tabs = {ws.title for ws in new_sh.worksheets()}

    logger.info(f"Old sheet: {old_sh.title}")
    logger.info(f"New sheet: {new_sh.title}")
    logger.info(f"Existing tabs in new sheet: {len(existing_tabs)}")

    migrated = 0
    skipped = 0
    failed = 0

    for tab in MACRO_TABS:
        try:
            old_ws = old_sh.worksheet(tab)
            data = old_ws.get_all_values()
            row_count = len(data)

            if tab in existing_tabs:
                logger.info(f"  ⏭ {tab}: already exists in new sheet ({row_count} rows in old) — skipping")
                skipped += 1
                continue

            # Create worksheet and populate
            if row_count == 0:
                new_sh.add_worksheet(title=tab, rows=100, cols=10)
                logger.info(f"  ✅ {tab}: created empty (was empty in old)")
            else:
                cols = len(data[0]) if data else 1
                new_ws = new_sh.add_worksheet(title=tab, rows=max(row_count + 100, 100), cols=max(cols, 1))
                new_ws.update([data[0]])  # header first
                # batch remaining rows (Google Sheets API limit: ~500 rows per update)
                BATCH = 400
                for i in range(1, row_count, BATCH):
                    chunk = data[i:i + BATCH]
                    new_ws.update(chunk, f'A{i + 1}')
                    logger.info(f"    rows {i}-{min(i + BATCH, row_count)}/{row_count}")

                logger.info(f"  ✅ {tab}: migrated {row_count} rows x {cols} cols")
            migrated += 1

        except Exception as e:
            logger.error(f"  ❌ {tab}: {e}")
            failed += 1

    logger.info(f"\nDone: {migrated} migrated, {skipped} skipped, {failed} failed")


if __name__ == '__main__':
    main()
