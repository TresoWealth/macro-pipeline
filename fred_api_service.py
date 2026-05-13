#!/usr/bin/env python3
"""
FRED API Service — single source of truth for global macro series.

Provenance boundary:
  Global series → FRED (this module)
  India-specific → RBI DBIE, MOSPI, NSE, NSDL (macro_data_fetcher_v2.py)

Series covered:
  DCOILBRENTEU   Brent Crude Oil       Daily
  DCOILWTICO     WTI Crude Oil         Daily
  DEXINUS        USD/INR Spot          Daily
  DGS10          US 10Y Treasury       Daily
  FEDFUNDS       Fed Funds Rate        Monthly
  DTWEXBGS       Trade-Weighted USD    Daily
  VIXCLS         US VIX (CBOE)         Daily
  T10YIE         US Breakeven 10Y      Daily

Author: TresoWealth Analytics — Data Pipeline Agent
Date: 2026-05-08
"""

import json
import logging
import time
import urllib.request
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FredAPIClient:
    """Reusable FRED API client for all global macro series."""

    API_KEY = "a5e365e8665773a37811cb005ae6bd6d"
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    # Series we pull from FRED — global + cross-border only
    SERIES = {
        'DCOILBRENTEU':  {'label': 'Brent Crude Oil',          'frequency': 'D'},
        'DCOILWTICO':    {'label': 'WTI Crude Oil',            'frequency': 'D'},
        'DEXINUS':       {'label': 'USD/INR Spot',             'frequency': 'D'},
        'DGS10':         {'label': 'US 10Y Treasury',          'frequency': 'D'},
        'FEDFUNDS':      {'label': 'Fed Funds Rate',           'frequency': 'M'},
        'DTWEXBGS':      {'label': 'Trade-Weighted USD',       'frequency': 'D'},
        'VIXCLS':        {'label': 'US VIX (CBOE)',            'frequency': 'D'},
        'T10YIE':        {'label': 'US Breakeven 10Y',         'frequency': 'D'},
    }

    def __init__(self):
        self._cache = {}

    # ------------------------------------------------------------------
    # Low-level
    # ------------------------------------------------------------------

    def _fetch(self, series_id: str, limit: int = 5, sort_order: str = "desc",
               observation_start: str = None, observation_end: str = None) -> List[Dict]:
        url = (f"{self.BASE_URL}?series_id={series_id}&api_key={self.API_KEY}"
               f"&file_type=json&limit={limit}&sort_order={sort_order}")
        if observation_start:
            url += f"&observation_start={observation_start}"
        if observation_end:
            url += f"&observation_end={observation_end}"
        last_err = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=15) as resp:
                    data = json.loads(resp.read())
                    return data.get('observations', [])
            except Exception as e:
                last_err = e
                if attempt < 2:
                    time.sleep(1.0 * (attempt + 1))
        logger.warning(f"FRED fetch failed for {series_id} after 3 attempts: {last_err}")
        return []

    # ------------------------------------------------------------------
    # Single-value queries
    # ------------------------------------------------------------------

    def get_latest(self, series_id: str) -> Optional[float]:
        """Return the most recent observation value for a series."""
        obs = self._fetch(series_id, limit=1, sort_order="desc")
        if obs:
            try:
                return float(obs[0]['value'])
            except (ValueError, KeyError):
                return None
        return None

    def get_value_on(self, series_id: str, date_str: str) -> Optional[float]:
        """Return the observation value on or closest before a date."""
        obs = self._fetch(series_id, limit=5, sort_order="desc",
                          observation_end=date_str)
        if obs:
            # FRED returns in desc order; take the first that is <= date_str
            for o in obs:
                if o['date'] <= date_str:
                    try:
                        return float(o['value'])
                    except (ValueError, KeyError):
                        continue
        return None

    def get_3m_ago(self, series_id: str, reference_date: str = None) -> Optional[float]:
        """Return the observation value ~3 calendar months before reference_date (or today)."""
        if reference_date is None:
            ref = datetime.now()
        else:
            ref = datetime.strptime(reference_date, '%Y-%m-%d')
        target = ref - timedelta(days=92)
        return self.get_value_on(series_id, target.strftime('%Y-%m-%d'))

    def get_last_n_days(self, series_id: str, n: int) -> List[Dict]:
        """Return the last n daily observations."""
        return self._fetch(series_id, limit=n, sort_order="desc")

    # ------------------------------------------------------------------
    # Bulk / historical
    # ------------------------------------------------------------------

    def get_historical(self, series_id: str, start: str = "2000-01-01",
                       end: str = None) -> pd.DataFrame:
        """Pull full historical series as a DataFrame with 'date' and 'value' columns."""
        if end is None:
            end = datetime.now().strftime('%Y-%m-%d')
        obs = self._fetch(series_id, limit=100000, sort_order="asc",
                          observation_start=start, observation_end=end)
        if not obs:
            return pd.DataFrame(columns=['date', 'value'])
        df = pd.DataFrame(obs)
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'])
        return df[['date', 'value']].dropna(subset=['value'])

    def get_all_latest(self) -> Dict[str, Optional[float]]:
        """Return the latest value for every series in SERIES."""
        result = {}
        for sid, meta in self.SERIES.items():
            val = self.get_latest(sid)
            result[sid] = val
            logger.info(f"  {sid:18s} {meta['label']:30s} = {val}")
        return result


# ------------------------------------------------------------------
# Quick test
# ------------------------------------------------------------------

def main():
    client = FredAPIClient()
    print("=" * 70)
    print("FRED API Service — Live Values")
    print("=" * 70)
    latest = client.get_all_latest()
    print("=" * 70)
    print(f"Fetched {sum(1 for v in latest.values() if v is not None)} / {len(latest)} series successfully")


if __name__ == "__main__":
    main()
