#!/usr/bin/env python3
"""
Optimized Google Sheets Integration for TresoWealth Macro Data Pipeline

Reduces API calls by:
- Caching spreadsheet object
- Batching operations
- Minimizing metadata fetches

Author: TresoWealth Analytics Team
Date: March 26, 2026
Version: 2.0 (Optimized)
"""

import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import logging
import pandas as pd
from typing import Dict, List, Optional
import json
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OptimizedMacroDataSheetsManager:
    """
    Optimized Google Sheets manager with reduced API calls

    Key optimizations:
    - Cache spreadsheet object
    - Batch operations
    - Minimize metadata fetches
    """

    def __init__(self, service_account_file: str = None):
        """
        Initialize Google Sheets client

        Args:
            service_account_file: Path to service account JSON file
        """
        if service_account_file is None:
            service_account_file = '/home/ubuntu/clawd/treso_analytics/service_account.json'

        try:
            self.creds = Credentials.from_service_account_file(
                service_account_file,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )

            self.gc = gspread.authorize(self.creds)
            self._spreadsheet_cache = {}

            logger.info("✅ Google Sheets client initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Google Sheets client: {e}")
            raise

    def _get_spreadsheet(self, spreadsheet_id: str):
        """Get spreadsheet from cache or fetch it"""
        if spreadsheet_id not in self._spreadsheet_cache:
            self._spreadsheet_cache[spreadsheet_id] = self.gc.open_by_key(spreadsheet_id)
        return self._spreadsheet_cache[spreadsheet_id]

    def _get_or_create_worksheet(self, spreadsheet, worksheet_name: str):
        """Get worksheet or create it with headers"""
        try:
            # First, list all worksheets to get fresh metadata
            worksheets = spreadsheet.worksheets()

            # Check for exact match (case-insensitive)
            for ws in worksheets:
                if ws.title.lower() == worksheet_name.lower():
                    logger.info(f"Using existing worksheet: {ws.title}")
                    return ws

            # Worksheet doesn't exist, create it
            logger.info(f"Creating worksheet: {worksheet_name}")
            worksheet = spreadsheet.add_worksheet(worksheet_name, rows=1000, cols=20)

            # Add headers
            headers = self._get_headers_for_worksheet(worksheet_name)
            if headers:
                worksheet.append_row(headers)
                # Format header row
                worksheet.format(f"A1:{chr(65 + len(headers) - 1)}1", {
                    'textFormat': {'bold': True},
                    'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                })

            return worksheet

        except Exception as e:
            logger.error(f"Error in _get_or_create_worksheet for {worksheet_name}: {e}")
            raise

    def _get_headers_for_worksheet(self, worksheet_name: str) -> Optional[List]:
        """Get headers for a given worksheet type"""
        headers_map = {
            'RBI_Data': ['Date', 'Repo_Rate', 'Reverse_Repo_Rate', 'GSec_10Y', 'MSF_Rate', 'Bank_Rate', 'Policy_Stance', 'Data_Source', 'Timestamp'],
            'Inflation_Data': ['Date', 'CPI', 'WPI', 'CPI_Trend', 'Core_CPI', 'Food_Inflation', 'Fuel_Inflation', 'Data_Source', 'Timestamp'],
            'Growth_Data': ['Date', 'IIP', 'GDP_Growth', 'Manufacturing', 'Services', 'Agriculture', 'Data_Source', 'Timestamp'],
            'Market_Data': ['Date', 'Nifty_50', 'Nifty_50DMA', 'Nifty_200DMA', 'Cyclicals_Index', 'Defensives_Index', 'VIX', 'Market_Trend', 'Data_Source', 'Timestamp'],
            'Regime_Classification': ['Date', 'Regime', 'Regime_Code', 'Confidence', 'Color', 'GDP_Growth', 'CPI', 'Nifty_Trend', 'Transition_Detected', 'Data_Source', 'Timestamp'],
            'Audit_Log': ['Timestamp', 'Action', 'Worksheet', 'Records_Affected', 'Status', 'Error_Message', 'User'],
            # NEW tabs — Phase 2.8 pipeline wiring
            'Exchange_Rates': ['Date', 'USDINR', 'USDINR_3M_Change_Pct', 'Data_Source', 'Timestamp'],
            'Oil_Brent_Monthly': ['Date', 'Brent_USD', 'Brent_3M_Change_Pct', 'Data_Source', 'Timestamp'],
            'Oil_WTI_Monthly': ['Date', 'WTI_USD', 'WTI_3M_Change_Pct', 'Data_Source', 'Timestamp'],
            'FPI_Flows': ['Date', 'FPI_Equity_Flow', 'FPI_Debt_Flow', 'FPI_Total_Flow', 'Data_Source', 'Timestamp'],
            'Sector_Indices': ['Date', 'Nifty_Bank', 'Nifty_IT', 'Nifty_Fin_Service', 'Nifty_Pharma',
                               'Nifty_Metal', 'Nifty_Realty', 'Nifty_Energy', 'Nifty_Auto', 'Nifty_FMCG',
                               'Nifty_Infra', 'Nifty_Consumer_Durables', 'Nifty_Oil_Gas',
                               'Nifty_Healthcare', 'Nifty_PSE', 'Nifty_Private_Bank', 'Data_Source', 'Timestamp'],
            'PMI_Data': ['Date', 'PMI_Manufacturing', 'PMI_Services', 'PMI_Composite', 'Data_Source', 'Timestamp'],
            'US_Macro': ['Date', 'DGS10', 'FEDFUNDS', 'DTWEXBGS', 'VIXCLS', 'T10YIE', 'Data_Source', 'Timestamp'],
        }

        return headers_map.get(worksheet_name)

    def append_rows_batch(self, spreadsheet_id: str, data_by_worksheet: Dict[str, List[List]]):
        """
        Append multiple rows to multiple worksheets in one batch

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            data_by_worksheet: Dict mapping worksheet names to list of rows
        """
        try:
            spreadsheet = self._get_spreadsheet(spreadsheet_id)

            for worksheet_name, rows in data_by_worksheet.items():
                worksheet = self._get_or_create_worksheet(spreadsheet, worksheet_name)

                for row in rows:
                    worksheet.append_row(row)

                logger.info(f"✅ Added {len(rows)} row(s) to {worksheet_name}")

        except Exception as e:
            logger.error(f"❌ Failed to batch append: {e}")
            raise

    def update_all_macro_data(self, spreadsheet_id: str, macro_data: Dict, regime_result: Dict):
        """
        Update all worksheets with fresh data (optimized batch version)

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            macro_data: Complete macro data dictionary
            regime_result: Regime classification result
        """
        logger.info("=" * 80)
        logger.info(f"Updating Google Sheets: {spreadsheet_id}")
        logger.info("=" * 80)

        try:
            # Prepare all data for batch update
            data_by_worksheet = {
                'RBI_Data': [[
                    macro_data['rbi']['date'],
                    macro_data['rbi']['repo_rate'],
                    macro_data['rbi']['reverse_repo_rate'],
                    macro_data['rbi']['gsec_10y'],
                    macro_data['rbi'].get('msf_rate', ''),
                    macro_data['rbi'].get('bank_rate', ''),
                    '',  # Policy stance (to be calculated)
                    macro_data['rbi']['data_source'],
                    macro_data['rbi']['fetch_timestamp']
                ]],
                'Inflation_Data': [[
                    macro_data['mospi_inflation']['date'],
                    macro_data['mospi_inflation']['cpi'],
                    macro_data['mospi_inflation']['wpi'],
                    macro_data['mospi_inflation']['cpi_trend'],
                    macro_data['mospi_inflation'].get('core_cpi', ''),
                    '',  # Food inflation
                    '',  # Fuel inflation
                    macro_data['mospi_inflation']['data_source'],
                    macro_data['mospi_inflation']['fetch_timestamp']
                ]],
                'Growth_Data': [[
                    macro_data['mospi_growth']['date'],
                    macro_data['mospi_growth']['iip'],
                    macro_data['mospi_growth']['gdp_growth'],
                    macro_data['mospi_growth'].get('manufacturing', ''),
                    macro_data['mospi_growth'].get('services', ''),
                    '',  # Agriculture
                    macro_data['mospi_growth']['data_source'],
                    macro_data['mospi_growth']['fetch_timestamp']
                ]],
                'Market_Data': [[
                    macro_data['nse']['date'],
                    macro_data['nse']['nifty_50'],
                    macro_data['nse']['nifty_50dma'],
                    macro_data['nse']['nifty_200dma'],
                    macro_data['nse']['cyclicals_index'],
                    macro_data['nse']['defensives_index'],
                    macro_data['nse']['vix'],
                    macro_data['nse']['market_trend'],
                    macro_data['nse']['data_source'],
                    macro_data['nse']['fetch_timestamp']
                ]],
                'Regime_Classification': [[
                    regime_result['classification_timestamp'][:10],  # Date only
                    regime_result['regime'],
                    regime_result['regime_code'],
                    regime_result['confidence'],
                    regime_result['color'],
                    macro_data['mospi_growth']['gdp_growth'],
                    macro_data['mospi_inflation']['cpi'],
                    macro_data['nse']['nifty_trend'] if 'nifty_trend' in macro_data['nse'] else macro_data['nse']['market_trend'],
                    '',  # Transition detected (to be calculated)
                    'Regime_Classifier',
                    regime_result['classification_timestamp']
                ]],
                # NEW tabs — Phase 2.8 pipeline wiring
                'Exchange_Rates': [[
                    macro_data.get('fx', {}).get('date', ''),
                    macro_data.get('fx', {}).get('usdinr', ''),
                    macro_data.get('fx', {}).get('usdinr_3m_change_pct', ''),
                    macro_data.get('fx', {}).get('data_source', ''),
                    macro_data.get('fx', {}).get('fetch_timestamp', ''),
                ]] if macro_data.get('fx') else [],
                'Oil_Brent_Monthly': [[
                    macro_data.get('oil', {}).get('date', ''),
                    macro_data.get('oil', {}).get('brent_usd', ''),
                    macro_data.get('oil', {}).get('brent_3m_change_pct', ''),
                    macro_data.get('oil', {}).get('data_source', ''),
                    macro_data.get('oil', {}).get('fetch_timestamp', ''),
                ]] if macro_data.get('oil') else [],
                'Oil_WTI_Monthly': [[
                    macro_data.get('oil', {}).get('date', ''),
                    macro_data.get('oil', {}).get('wti_usd', ''),
                    macro_data.get('oil', {}).get('wti_3m_change_pct', ''),
                    macro_data.get('oil', {}).get('data_source', ''),
                    macro_data.get('oil', {}).get('fetch_timestamp', ''),
                ]] if macro_data.get('oil') else [],
                'FPI_Flows': [[
                    macro_data.get('fpi_flows', {}).get('date', ''),
                    macro_data.get('fpi_flows', {}).get('fpi_equity_flow', ''),
                    macro_data.get('fpi_flows', {}).get('fpi_debt_flow', ''),
                    macro_data.get('fpi_flows', {}).get('fpi_total_flow', ''),
                    macro_data.get('fpi_flows', {}).get('data_source', ''),
                    macro_data.get('fpi_flows', {}).get('fetch_timestamp', ''),
                ]] if macro_data.get('fpi_flows') else [],
                'Sector_Indices': [[
                    macro_data.get('sector_indices', {}).get('date', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY BANK', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY IT', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY FIN SERVICE', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY PHARMA', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY METAL', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY REALTY', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY ENERGY', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY AUTO', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY FMCG', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY INFRA', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY CONSUMER DURABLES', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY OIL AND GAS', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY HEALTHCARE', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY PSE', ''),
                    macro_data.get('sector_indices', {}).get('sectors', {}).get('NIFTY PRIVATE BANK', ''),
                    macro_data.get('sector_indices', {}).get('data_source', ''),
                    macro_data.get('sector_indices', {}).get('fetch_timestamp', ''),
                ]] if macro_data.get('sector_indices') else [],
                'PMI_Data': [[
                    macro_data.get('pmi', {}).get('date', ''),
                    macro_data.get('pmi', {}).get('pmi_manufacturing', ''),
                    macro_data.get('pmi', {}).get('pmi_services', ''),
                    macro_data.get('pmi', {}).get('pmi_composite', ''),
                    macro_data.get('pmi', {}).get('data_source', ''),
                    macro_data.get('pmi', {}).get('fetch_timestamp', ''),
                ]] if macro_data.get('pmi') else [],
                'US_Macro': [[
                    macro_data.get('us_macro', {}).get('date', ''),
                    macro_data.get('us_macro', {}).get('dgs10', ''),
                    macro_data.get('us_macro', {}).get('fedfunds', ''),
                    macro_data.get('us_macro', {}).get('dtwbgs', ''),
                    macro_data.get('us_macro', {}).get('vixcls', ''),
                    macro_data.get('us_macro', {}).get('t10yie', ''),
                    macro_data.get('us_macro', {}).get('data_source', ''),
                    macro_data.get('us_macro', {}).get('fetch_timestamp', ''),
                ]] if macro_data.get('us_macro') else [],
            }

            # Remove empty entries before batch update
            data_by_worksheet = {k: v for k, v in data_by_worksheet.items() if v}

            # Batch update all worksheets
            self.append_rows_batch(spreadsheet_id, data_by_worksheet)

            total_tabs = len(data_by_worksheet)
            # Log success
            audit_worksheet = self._get_or_create_worksheet(
                self._get_spreadsheet(spreadsheet_id),
                'Audit_Log'
            )
            audit_worksheet.append_row([
                datetime.now().isoformat(),
                'UPDATE_ALL_WORKSHEETS',
                'All',
                total_tabs,
                'SUCCESS',
                '',
                'System'
            ])

            logger.info("=" * 80)
            logger.info("✅ All worksheets updated successfully")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"❌ Failed to update worksheets: {e}")
            raise

    def get_latest_regime(self, spreadsheet_id: str) -> Optional[Dict]:
        """
        Get the latest regime classification

        Returns:
            dict: Latest regime data or None if no data
        """
        try:
            spreadsheet = self._get_spreadsheet(spreadsheet_id)
            worksheet = self._get_or_create_worksheet(spreadsheet, 'Regime_Classification')

            # Get all values (more efficient than get_all_records)
            values = worksheet.get_all_values()

            if len(values) > 1:  # Has data beyond header
                # Get the latest record (last row)
                latest = values[-1]

                return {
                    'date': latest[0] if len(latest) > 0 else '',
                    'regime': latest[1] if len(latest) > 1 else '',
                    'regime_code': latest[2] if len(latest) > 2 else '',
                    'confidence': float(latest[3]) if len(latest) > 3 and latest[3] else 0,
                    'color': latest[4] if len(latest) > 4 else ''
                }

            return None

        except Exception as e:
            logger.error(f"❌ Failed to get latest regime: {e}")
            return None

    def check_regime_transition(self, spreadsheet_id: str, new_regime: Dict) -> Dict:
        """
        Check if regime has changed since last classification

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            new_regime: New regime classification result

        Returns:
            dict: Transition detection result
        """
        previous_regime = self.get_latest_regime(spreadsheet_id)

        transition_detected = (
            previous_regime is not None and
            previous_regime.get('regime') != new_regime['regime']
        )

        result = {
            'previous_regime': previous_regime.get('regime') if previous_regime else None,
            'new_regime': new_regime['regime'],
            'transition_detected': transition_detected,
            'timestamp': datetime.now().isoformat()
        }

        if transition_detected:
            logger.warning(f"⚠️ REGIME CHANGE: {result['previous_regime']} → {result['new_regime']}")

        return result
