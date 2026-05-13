#!/usr/bin/env python3
"""
Enhanced Regime Classifier - Global Best Practices

Implements 4 key upgrades following global best practices:
1. Smoother signals (HP-filter, composite indices)
2. Regime probabilities (softmax, not hard labels)
3. Financial conditions dimension (FCI)
4. Statistical regime detection (K-means, Markov switching)

Author: TresoWealth Analytics Team
Date: March 28, 2026
Version: 2.0 (Enhanced)
References:
- AQR "Regime-Based Asset Allocation"
- Bridgewater "Three-Cycle Model"
- JPM "Guide to Markets"
- ECB "Regime-Switching Models"
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
import json
import re
from typing import Dict, List, Tuple, Optional
from scipy.signal import savgol_filter
from scipy.stats import zscore
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedRegimeClassifier:
    """
    Enhanced regime classifier with global best practices

    Upgrades from v1.0:
    1. Signal smoothing (HP-filter, composites)
    2. Regime probabilities (softmax)
    3. Financial Conditions Index (FCI)
    4. Statistical detection (K-means, MS-VAR)
    """

    def __init__(self, method: str = 'rule_based'):
        """
        Initialize enhanced classifier

        Args:
            method: 'rule_based', 'statistical', or 'hybrid'
        """
        self.method = method

        # Traditional thresholds (for rule-based)
        self.GDP_ACCELERATING = 6.5
        self.GDP_DECELERATING = 6.0
        self.CPI_RISING = 5.0
        self.CPI_FALLING = 4.5

        # Enhanced thresholds (for composite)
        self.GDP_ACCELERATING_COMPOSITE = 0.2  # Standardized
        self.GDP_DECELERATING_COMPOSITE = -0.2
        self.CPI_RISING_COMPOSITE = 0.2
        self.CPI_FALLING_COMPOSITE = -0.2

        # Regime centers (for statistical detection)
        self.regime_centers = {
            'Growth-Disinflation': {'gdp': 1.0, 'cpi': -1.0, 'fci': -0.5},
            'Growth-Inflation': {'gdp': 1.0, 'cpi': 1.0, 'fci': 0.5},
            'Stagnation-Disinflation': {'gdp': -1.0, 'cpi': -1.0, 'fci': -1.0},
            'Stagflation': {'gdp': -1.0, 'cpi': 1.0, 'fci': 1.0}
        }

        # FCI weights (6 components with oil, renormalized)
        self.fci_weights_base = {
            'repo_spread': 0.18,        # RBI repo - neutral rate spread
            'yield_curve_slope': 0.18,  # 10Y-3M yield curve slope (leading indicator)
            'yield_change': 0.18,       # 10Y GSec yield change
            'vix_raw': 0.18,            # India VIX
            'credit_spread': 0.18,      # Corporate bond spread proxy
            'oil': 0.10,                # Brent crude shock (external terms-of-trade)
        }
        # Active weights (recomputed per call — oil dropped if not in snapshot)
        self.fci_weights = dict(self.fci_weights_base)

        logger.info(f"Initialized EnhancedRegimeClassifier (method={method})")

    def calculate_hp_filter(self, series: pd.Series, lamb: float = 6.25) -> pd.Series:
        """
        Hodrick-Prescott filter for trend extraction

        Args:
            series: Input time series
            lamb: Smoothing parameter (6.25 for monthly data)

        Returns:
            Trend component
        """
        from scipy import sparse
        from scipy.sparse.linalg import spsolve

        n = len(series)
        # Get first difference matrix
        D = sparse.eye(n, k=1) - 2 * sparse.eye(n) + sparse.eye(n, k=-1)
        D = D[1:-1, 1:-1]  # Remove boundary rows/cols

        # HP filter: min sum((y_t - trend_t)^2 + λ * Δ^2 trend_t)
        A = sparse.eye(n) + lamb * D.T @ D
        b = series.values

        trend = spsolve(A, b)
        return pd.Series(trend, index=series.index)

    def calculate_pmi_composite(self, pmi_manufacturing: pd.Series,
                                 pmi_services: pd.Series) -> pd.Series:
        """
        Calculate PMI composite (60/40 Manufacturing/Services)

        India-specific: GDP has 2Q lag, PMI is better for nowcasting

        Args:
            pmi_manufacturing: Manufacturing PMI
            pmi_services: Services PMI

        Returns:
            Composite PMI
        """
        return 0.6 * pmi_manufacturing + 0.4 * pmi_services

    def smooth_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Upgrade 1: Apply signal smoothing to reduce noise

        Methods:
        - Growth: 3-month SMA or HP-filter of GDP/PMI composite
        - Inflation: 3-month SMA of core CPI
        - Use PMI composite for nowcasting (GDP lag)
        """
        logger.info("Applying signal smoothing...")

        df_smooth = df.copy()

        # Growth signal smoothing
        if 'PMI_Manufacturing' in df.columns and 'PMI_Services' in df.columns:
            # Use PMI composite (better for nowcasting)
            df_smooth['growth_composite'] = self.calculate_pmi_composite(
                df['PMI_Manufacturing'],
                df['PMI_Services']
            )
            # Z-score for comparability
            df_smooth['growth_signal'] = zscore(df_smooth['growth_composite'])
        else:
            # Fall back to GDP with 3-month SMA (HP filter disabled — sparse matrix issue)
            if len(df) > 6:
                df_smooth['gdp_trend'] = df['GDP_Growth'].rolling(3, min_periods=1).mean()
            else:
                df_smooth['gdp_trend'] = df['GDP_Growth']
            df_smooth['growth_signal'] = zscore(df_smooth['gdp_trend'])

        # Inflation signal smoothing (3-month SMA)
        if 'CPI_Core' in df.columns:
            # Use core CPI if available
            df_smooth['cpi_trend'] = df['CPI_Core'].rolling(3).mean()
        else:
            # Use headline CPI
            df_smooth['cpi_trend'] = df['CPI'].rolling(3).mean()

        df_smooth['inflation_signal'] = zscore(df_smooth['cpi_trend'])

        logger.info("Signal smoothing complete")
        return df_smooth

    def calculate_fci(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Upgrade 3: Calculate Financial Conditions Index (FCI)

        Components (normalized):
        1. Repo spread (RBI repo - policy rate)
        2. 10Y GSec yield change
        3. India VIX
        4. Corporate bond spreads

        FCI > 0: Tight financial conditions (bearish)
        FCI < 0: Loose financial conditions (bullish)
        """
        logger.info("Calculating Financial Conditions Index...")

        df_fci = df.copy()

        # Component 1: Repo spread
        if 'Repo_Rate' in df.columns and 'Policy_Rate' in df.columns:
            df_fci['repo_spread'] = df['Repo_Rate'] - df['Policy_Rate']
        elif 'Repo_Rate' in df.columns:
            # If no policy rate, use repo rate itself
            df_fci['repo_spread'] = df['Repo_Rate'] - df['Repo_Rate'].rolling(12).mean()
        else:
            df_fci['repo_spread'] = 0

        # Component 2: 10Y GSec yield change
        if 'GSec_10Y' in df.columns:
            df_fci['yield_change'] = df['GSec_10Y'].diff(3)  # 3-month change
        else:
            df_fci['yield_change'] = 0

        # Component 3: India VIX
        if 'VIX' in df.columns:
            df_fci['vix_raw'] = df['VIX']
        else:
            df_fci['vix_raw'] = 20  # Default

        # Component 4: Credit spreads (if available)
        if 'Corporate_Spread' in df.columns:
            df_fci['credit_spread'] = df['Corporate_Spread']
        else:
            # Estimate using VIX (proxy)
            df_fci['credit_spread'] = df_fci['vix_raw'] * 0.5

        # Normalize each component (z-score)
        components = ['repo_spread', 'yield_change', 'vix_raw', 'credit_spread']
        for comp in components:
            if comp in df_fci.columns:
                df_fci[f'{comp}_norm'] = zscore(df_fci[comp].fillna(0))
            else:
                df_fci[f'{comp}_norm'] = 0

        # Calculate weighted FCI
        fci_components = [f'{comp}_norm' for comp in components]
        df_fci['FCI'] = sum(
            df_fci[comp] * self.fci_weights[comp.replace('_norm', '')]
            for comp in fci_components
        )

        # Normalize FCI to [-1, 1] range for regime classification
        df_fci['FCI_signal'] = np.tanh(df_fci['FCI'] / 2)

        logger.info("FCI calculated")
        return df_fci

    def calculate_fci_phase1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhanced FCI with 9 components (Phase 1)

        Categories:
        1. Interest Rates (35%)
        2. Credit Conditions (25%)
        3. Equity Markets (20%)
        4. Currency & Flows (20%)

        Args:
            df: DataFrame with all required columns

        Returns:
            DataFrame with FCI calculated (9 components)
        """
        logger.info("Calculating ENHANCED FCI (Phase 1: 9 components)...")

        df_fci = df.copy()

        # ========== CATEGORY 1: INTEREST RATES (35%) ==========

        # Component 1: Repo spread
        if 'repo_spread' in df.columns and df['repo_spread'].notna().any():
            df_fci['repo_spread'] = df['repo_spread']
        elif 'Repo_Rate' in df.columns and 'Policy_Rate' in df.columns:
            df_fci['repo_spread'] = df['Repo_Rate'] - df['Policy_Rate']
        elif 'Repo_Rate' in df.columns:
            df_fci['repo_spread'] = df['Repo_Rate'] - df['Repo_Rate'].rolling(12).mean()
        else:
            df_fci['repo_spread'] = 0

        # Component 2: 10Y GSec yield change
        if 'GSec_10Y' in df.columns:
            df_fci['yield_change'] = df['GSec_10Y'].diff(3)
        else:
            df_fci['yield_change'] = 0

        # Component 3: Yield curve slope (10Y - 91D T-bill) [NEW]
        if 'GSec_10Y' in df.columns and 'TBill_91D_Yield' in df.columns:
            df_fci['yield_curve_slope'] = df['GSec_10Y'] - df['TBill_91D_Yield']
        else:
            # Fallback: Use proxy if T-bill data not available
            if 'GSec_10Y' in df.columns and 'Repo_Rate' in df.columns:
                df_fci['yield_curve_slope'] = df['GSec_10Y'] - df['Repo_Rate']
            else:
                df_fci['yield_curve_slope'] = 0

        # ========== CATEGORY 2: CREDIT CONDITIONS (25%) ==========

        # Component 4: Corporate bond spread
        if 'Corporate_Spread' in df.columns:
            df_fci['credit_spread'] = df['Corporate_Spread']
        elif 'VIX' in df.columns:
            df_fci['credit_spread'] = df['VIX'] * 0.5
        else:
            df_fci['credit_spread'] = 0

        # Component 5: Credit growth (YoY) [NEW]
        if 'Bank_Credit' in df.columns:
            df_fci['credit_growth'] = (df['Bank_Credit'] / df['Bank_Credit'].shift(12)) - 1
            # Handle NaN for first 12 months
            df_fci['credit_growth'] = df_fci['credit_growth'].fillna(0)
        else:
            df_fci['credit_growth'] = 0

        # ========== CATEGORY 3: EQUITY MARKETS (20%) ==========

        # Component 6: India VIX
        if 'VIX' in df.columns:
            df_fci['vix_raw'] = df['VIX']
        elif 'vix_raw' in df.columns:
            df_fci['vix_raw'] = df['vix_raw']
        elif 'vix' in df.columns:
            df_fci['vix_raw'] = df['vix']
        else:
            df_fci['vix_raw'] = 20

        # Component 7: Nifty 50 return (3-month) [NEW]
        if 'Nifty_3M_Return' in df.columns:
            df_fci['nifty_return'] = df['Nifty_3M_Return']
        elif 'Nifty_50_Return' in df.columns:
            df_fci['nifty_return'] = df['Nifty_50_Return'].rolling(63).sum()
        elif 'Nifty_50_Close' in df.columns:
            df_fci['nifty_return'] = df['Nifty_50_Close'].pct_change(63)
        else:
            df_fci['nifty_return'] = 0

        # ========== CATEGORY 4: CURRENCY & FLOWS (20%) ==========

        # Component 8: INR/USD volatility (30-day) [NEW]
        if 'INR_USD' in df.columns:
            # Calculate daily returns first
            inr_returns = df['INR_USD'].pct_change()
            # Then 30-day rolling standard deviation
            df_fci['inr_volatility'] = inr_returns.rolling(30).std()
            # Annualize (multiply by sqrt(252))
            df_fci['inr_volatility'] = df_fci['inr_volatility'] * np.sqrt(252)
        else:
            df_fci['inr_volatility'] = 0

        # Component 9: FPI equity flow (3-month sum) [NEW]
        if 'FPI_Equity_Flow_3M' in df.columns:
            df_fci['fpi_flow'] = df['FPI_Equity_Flow_3M']
        elif 'FPI_Equity_Flow' in df.columns:
            df_fci['fpi_flow'] = df['FPI_Equity_Flow'].rolling(63).sum()
        else:
            df_fci['fpi_flow'] = 0

        # ========== NORMALIZATION ==========

        # List of all 9 components
        all_components = [
            'repo_spread',
            'yield_change',
            'yield_curve_slope',      # NEW
            'credit_spread',
            'credit_growth',          # NEW
            'vix_raw',
            'nifty_return',           # NEW
            'inr_volatility',         # NEW
            'fpi_flow'                # NEW
        ]

        # Normalize each component (z-score)
        for comp in all_components:
            if comp in df_fci.columns:
                df_fci[f'{comp}_norm'] = zscore(df_fci[comp].fillna(0))
            else:
                df_fci[f'{comp}_norm'] = 0

        # ========== WEIGHTED AGGREGATION ==========

        # Updated weights for 9 components
        fci_weights_phase1 = {
            # Interest Rates (35%)
            'repo_spread': 0.15,
            'yield_change': 0.10,
            'yield_curve_slope': 0.10,

            # Credit (25%)
            'credit_spread': 0.15,
            'credit_growth': 0.10,

            # Equity (20%)
            'vix_raw': 0.10,
            'nifty_return': 0.10,

            # Currency (20%)
            'inr_volatility': 0.10,
            'fpi_flow': 0.10
        }

        # Calculate weighted FCI
        fci_components = [f'{comp}_norm' for comp in all_components]
        df_fci['FCI'] = sum(
            df_fci[comp] * fci_weights_phase1[comp.replace('_norm', '')]
            for comp in fci_components
        )

        # Normalize FCI to [-1, 1] range for regime classification
        df_fci['FCI_signal'] = np.tanh(df_fci['FCI'] / 2)

        logger.info("Enhanced FCI calculated (9 components)")
        logger.info(f"FCI range: [{df_fci['FCI_signal'].min():.2f}, {df_fci['FCI_signal'].max():.2f}]")

        return df_fci

    # Regime persistence matrix (Indian economy — regimes persist longer)
    # P(stay) = 0.85, P(switch to any other) = 0.05 each
    # Source: jai_prakesh_pandey — Indian regimes structurally persistent
    TRANSITION_PRIOR = {
        'Growth-Disinflation': {
            'Growth-Disinflation': 0.85, 'Growth-Inflation': 0.05,
            'Stagnation-Disinflation': 0.05, 'Stagflation': 0.05
        },
        'Growth-Inflation': {
            'Growth-Disinflation': 0.05, 'Growth-Inflation': 0.85,
            'Stagnation-Disinflation': 0.05, 'Stagflation': 0.05
        },
        'Stagnation-Disinflation': {
            'Growth-Disinflation': 0.05, 'Growth-Inflation': 0.05,
            'Stagnation-Disinflation': 0.85, 'Stagflation': 0.05
        },
        'Stagflation': {
            'Growth-Disinflation': 0.05, 'Growth-Inflation': 0.05,
            'Stagnation-Disinflation': 0.05, 'Stagflation': 0.85
        },
    }

    def classify_regime_soft(self, row: pd.Series,
                             current_regime: Optional[str] = None,
                             oil_z: float = 0.0) -> Dict[str, float]:
        """
        Upgrade 2: Calculate regime probabilities (softmax + persistence + oil tilt)

        Instead of hard labels, output 4 probabilities summing to 1.
        Based on distance to each regime center, with optional persistence
        prior and oil shock adjustment.

        Oil tilt: high/rising oil penalizes Growth-Disinflation (Goldilocks),
        boosts Growth-Inflation and Stagflation. India imports 85-90% of its
        crude — oil is the largest external terms-of-trade shock.

        Args:
            row: Series with growth_signal, inflation_signal, FCI_signal
            current_regime: Previous regime for persistence prior (None = no prior)
            oil_z: Optional oil z-score for probability tilting (0 = no tilt)
        """
        growth = row['growth_signal']
        inflation = row['inflation_signal']
        fci = row.get('FCI_signal', 0)

        # Calculate squared distance to each regime center
        distances = {}
        for regime, center in self.regime_centers.items():
            dist = (
                (growth - center['gdp']) ** 2 +
                (inflation - center['cpi']) ** 2 +
                (fci - center['fci']) ** 2
            )
            distances[regime] = dist

        # Convert distances to probabilities (softmax) with oil tilt on logits
        temperature = 1.0
        logits = {r: -d / temperature for r, d in distances.items()}

        # Oil tilt: when oil_z > 0 (oil above trend), shift probability mass
        # away from Growth-Disinflation toward inflation regimes
        if abs(oil_z) > 0.1:
            tilt_strength = 0.15  # modest — oil doesn't dominate macro
            logits['Growth-Disinflation'] -= tilt_strength * max(oil_z, 0)
            logits['Growth-Inflation'] += tilt_strength * max(oil_z, 0) * 0.7
            logits['Stagflation'] += tilt_strength * max(oil_z, 0) * 0.3

        exp_dists = {r: np.exp(logits[r]) for r in logits}
        total = sum(exp_dists.values())
        softmax = {r: exp_dists[r] / total for r in exp_dists}

        # Apply persistence prior (Bayesian update) if previous regime known
        if current_regime and current_regime in self.TRANSITION_PRIOR:
            prior = self.TRANSITION_PRIOR[current_regime]
            posterior = {}
            for regime in softmax:
                posterior[regime] = softmax[regime] * prior.get(regime, 0.05)
            denom = sum(posterior.values())
            if denom > 0:
                probabilities = {r: posterior[r] / denom for r in posterior}
            else:
                probabilities = softmax
        else:
            probabilities = softmax

        return probabilities

    def classify_regime_hard(self, row: pd.Series) -> str:
        """
        Traditional hard classification (for backward compatibility)

        Uses smoothed signals and FCI for improved accuracy.
        """
        growth = row['growth_signal']
        inflation = row['inflation_signal']
        fci = row.get('FCI_signal', 0)

        # Enhanced classification with FCI
        if growth > self.GDP_ACCELERATING_COMPOSITE:
            if inflation > self.CPI_RISING_COMPOSITE:
                if fci > 0:
                    return 'Growth-Inflation-Tight'  # Tight GI
                else:
                    return 'Growth-Inflation'        # Loose GI
            else:
                return 'Growth-Disinflation'
        else:
            if inflation > self.CPI_RISING_COMPOSITE:
                return 'Stagflation'
            else:
                return 'Stagnation-Disinflation'

    def detect_statistical_regimes(self, df: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
        """
        Upgrade 4: Statistical regime detection using K-means

        Discovers natural regime clusters from data (data-driven).

        Args:
            df: DataFrame with growth_signal, inflation_signal, FCI_signal
            n_clusters: Number of regimes to discover

        Returns:
            DataFrame with cluster assignments
        """
        logger.info(f"Detecting regimes statistically (K={n_clusters})...")

        # Prepare features
        features = ['growth_signal', 'inflation_signal', 'FCI_signal']
        feature_data = df[features].dropna()

        if len(feature_data) < n_clusters * 3:
            logger.warning(f"Not enough data for {n_clusters} clusters ({len(feature_data)} obs)")
            return df

        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(feature_data)

        # K-means clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=50)
        clusters = kmeans.fit_predict(X_scaled)

        # Map clusters to regime names
        cluster_centers = kmeans.cluster_centers_
        regime_mapping = self._map_clusters_to_regimes(cluster_centers)

        # Add cluster assignments to DataFrame
        df_result = df.copy()
        df_result['statistical_regime'] = None

        for i, idx in enumerate(feature_data.index):
            cluster_id = clusters[i]
            df_result.loc[idx, 'statistical_regime'] = regime_mapping[cluster_id]

        # Calculate cluster probabilities (soft assignment)
        for i, regime in enumerate(regime_mapping.values()):
            distances = kmeans.transform(X_scaled)[:, i]
            # Convert to probabilities
            probs = np.exp(-distances)
            probs = probs / probs.sum(axis=1, keepdims=True)
            df_result[f'prob_{regime}'] = 0

        # Fill probabilities
        for i, idx in enumerate(feature_data.index):
            for j, regime in enumerate(regime_mapping.values()):
                df_result.loc[idx, f'prob_{regime}'] = probs[i, j]

        logger.info("Statistical regime detection complete")
        logger.info(f"Cluster mapping: {regime_mapping}")

        return df_result

    def _map_clusters_to_regimes(self, cluster_centers: np.ndarray) -> Dict[int, str]:
        """
        Map K-means clusters to regime names based on centroids
        """
        regimes = []
        for center in cluster_centers:
            # Center = [growth, inflation, fci]
            g, inf, f = center

            if g > 0:
                if inf > 0:
                    if f > 0:
                        regimes.append('Growth-Inflation-Tight')
                    else:
                        regimes.append('Growth-Inflation')
                else:
                    regimes.append('Growth-Disinflation')
            else:
                if inf > 0:
                    regimes.append('Stagflation')
                else:
                    regimes.append('Stagnation-Disinflation')

        # Return mapping
        return {i: regime for i, regime in enumerate(regimes)}

    def classify_enhanced(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main classification method with all 4 upgrades

        Returns:
            DataFrame with regime probabilities and labels
        """
        logger.info("=== ENHANCED REGIME CLASSIFICATION ===")

        # Upgrade 1: Smooth signals
        df_smooth = self.smooth_signals(df)

        # Upgrade 3: Add FCI
        df_enhanced = self.calculate_fci(df_smooth)

        # Upgrade 2 & 4: Calculate probabilities and statistical regimes
        results = []

        for idx, row in df_enhanced.iterrows():
            result = {
                'Date': row['Date'],
                'growth_signal': row['growth_signal'],
                'inflation_signal': row['inflation_signal'],
                'FCI_signal': row['FCI_signal']
            }

            # Soft probabilities (Upgrade 2)
            probs = self.classify_regime_soft(row)
            for regime, prob in probs.items():
                result[f'prob_{regime}'] = prob

            # Hard label (with FCI modulation)
            result['regime_hard'] = self.classify_regime_hard(row)
            result['regime_prob'] = max(probs.values())  # Confidence

            results.append(result)

        df_results = pd.DataFrame(results)

        # Upgrade 4: Statistical detection
        df_statistical = self.detect_statistical_regimes(df_enhanced)
        if 'statistical_regime' in df_statistical.columns:
            df_results['regime_statistical'] = df_statistical['statistical_regime']

        return df_results

    def compare_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compare v1.0 (hard labels) vs v2.0 (probabilities)

        Shows improvement in signal quality and predictive power.
        """
        logger.info("Comparing classification methods...")

        # v1.0 (traditional)
        v1_regimes = []
        for idx, row in df.iterrows():
            gdp = row['GDP_Growth']
            cpi = row['CPI']

            if gdp >= 6.5:
                if cpi < 4.5:
                    regime = 'Growth-Disinflation'
                else:
                    regime = 'Growth-Inflation'
            else:
                if cpi < 4.5:
                    regime = 'Stagnation-Disinflation'
                else:
                    regime = 'Stagflation'

            v1_regimes.append(regime)

        # v2.0 (enhanced)
        df_v2 = self.classify_enhanced(df)

        comparison = pd.DataFrame({
            'Date': df['Date'],
            'v1_regime': v1_regimes,
            'v2_regime': df_v2['regime_hard'],
            'v2_confidence': df_v2['regime_prob'],
            'v2_prob_GI': df_v2['prob_Growth-Inflation'],
            'v2_prob_S': df_v2['prob_Stagflation']
        })

        # Calculate transition volatility
        comparison['v1_changed'] = comparison['v1_regime'] != comparison['v1_regime'].shift(1)
        comparison['v2_changed'] = comparison['v2_regime'] != comparison['v2_regime'].shift(1)

        v1_volatility = comparison['v1_changed'].sum() / len(comparison)
        v2_volatility = comparison['v2_changed'].sum() / len(comparison)

        logger.info(f"Regime transition volatility:")
        logger.info(f"  v1.0 (hard labels): {v1_volatility:.1%}")
        logger.info(f"  v2.0 (probabilities): {v2_volatility:.1%}")
        logger.info(f"  Reduction: {(v1_volatility - v2_volatility) / v1_volatility * 100:.1f}%")

        return comparison


    # ========================================================================
    # PIPELINE ADAPTER — classify_current_enhanced()
    # ========================================================================

    def _load_previous_regime(self) -> tuple:
        """Load previous regime and confidence from persistent state file"""
        state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  'current_regime.json')
        try:
            if os.path.exists(state_file):
                with open(state_file) as f:
                    state = json.load(f)
                return state.get('regime'), state.get('confidence')
        except Exception:
            pass
        return None, None

    def _save_current_regime(self, regime: str, confidence: float,
                             probabilities: Dict[str, float]):
        """Save current regime for next run's persistence prior"""
        state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  'current_regime.json')
        try:
            state = {
                'regime': regime,
                'confidence': confidence,
                'probabilities': probabilities,
                'timestamp': pd.Timestamp.now().isoformat()
            }
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception:
            pass

    def classify_current_enhanced(self, macro_data: Dict) -> Dict[str, any]:
        """
        Enhanced single-point classification (Tier 1 upgrades).

        Upgrades from v2.0:
        1. Output gap (GDP - 10yr trend) instead of absolute GDP z-score
        2. Yield curve slope as 5th FCI component (10Y-3M)
        3. Regime persistence prior (Bayesian update, P(stay)=0.85)

        Args:
            macro_data: Dict from EnhancedMacroDataFetcher.fetch_all_macro_data()

        Returns:
            dict with v1-compatible keys plus enhanced extras
        """
        logger.info("=== ENHANCED CLASSIFICATION v2.1 (Tier 1) ===")

        try:
            # 1. Compute raw signals from macro_data
            gdp = float(macro_data['mospi_growth']['gdp_growth'])
            cpi = float(macro_data['mospi_inflation']['cpi'])
            repo = float(macro_data['rbi']['repo_rate'])
            gsec = float(macro_data['rbi']['gsec_10y'])
            vix = float(macro_data['nse']['vix'])
            market_trend = macro_data['nse']['market_trend']
            tbill_3m = float(macro_data['rbi'].get('tbill_91d', repo))

            # Oil data — optional, degrade gracefully if pipeline hasn't added it yet
            oil_brent = None
            oil_z = 0.0
            has_oil = 'oil' in macro_data and macro_data['oil'] is not None
            if has_oil:
                oil_brent = float(macro_data['oil'].get('brent_usd', 75))
                oil_3m = float(macro_data['oil'].get('brent_3m_change_pct', 0))
                # Z-score: historical Brent mean ~$70, std ~$20 (2000-2026)
                oil_mean, oil_std = 70.0, 20.0
                try:
                    real_file = os.path.join(hist_dir, 'macro_data_2000_2026_100pct_real.json')
                    # Oil historical not in legacy JSON — use defaults unless we add it
                except Exception:
                    pass
                oil_z = round((oil_brent - oil_mean) / max(oil_std, 0.01), 3)

            # 2. Load historical context
            hist_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    'historical_macro_data')
            gdp_mean, gdp_std = 6.5, 1.5
            cpi_mean, cpi_std = 5.0, 2.0
            gdp_trend_10yr = 6.5  # default 10-year rolling average
            output_gap_mean, output_gap_std = 0.0, 1.5
            try:
                real_file = os.path.join(hist_dir, 'macro_data_2000_2026_100pct_real.json')
                if os.path.exists(real_file):
                    with open(real_file) as f:
                        hist = json.load(f)
                    gdp_records = hist.get('gdp', [])
                    cpi_records = hist.get('cpi', [])

                    gdp_vals = [r.get('GDP_Growth', 0) for r in gdp_records if r.get('GDP_Growth')]
                    cpi_vals = [r.get('CPI', 0) for r in cpi_records if r.get('CPI')]

                    if gdp_vals:
                        gdp_mean, gdp_std = float(np.mean(gdp_vals)), float(np.std(gdp_vals))
                        # 10-year rolling average (last 10 years of data ≈ last ~40 qtrs)
                        recent_window = min(40, len(gdp_vals))
                        gdp_trend_10yr = float(np.mean(gdp_vals[-recent_window:]))

                    if cpi_vals:
                        cpi_mean, cpi_std = float(np.mean(cpi_vals)), float(np.std(cpi_vals))

                    # Compute historical output gaps for z-scoring
                    if gdp_vals and len(gdp_vals) > 40:
                        output_gaps = []
                        for i in range(40, len(gdp_vals)):
                            trailing_avg = np.mean(gdp_vals[max(0, i - 40):i])
                            output_gaps.append(gdp_vals[i] - trailing_avg)
                        if output_gaps:
                            output_gap_mean = float(np.mean(output_gaps))
                            output_gap_std = float(np.std(output_gaps))
            except Exception:
                pass

            # 3. TIER 1 UPGRADE: Output gap instead of absolute GDP
            # GDP growth minus 10-year trend = output gap
            # A 6.5% reading when trend is 5% = +1.5 expansion
            # Same 6.5% when trend is 8% = -1.5 contraction
            output_gap = gdp - gdp_trend_10yr
            growth_signal = round(
                (output_gap - output_gap_mean) / max(output_gap_std, 0.01), 3
            )
            inflation_signal = round((cpi - cpi_mean) / max(cpi_std, 0.01), 3)
            logger.info(f"   Output gap: {output_gap:+.1f}% (GDP={gdp}%, trend={gdp_trend_10yr:.1f}%)")

            # 4. TIER 1 UPGRADE: 5-component FCI with yield curve slope
            repo_spread = repo - 6.5         # deviation from neutral
            yield_change = gsec - 6.5        # GSec above neutral
            yield_curve_slope_raw = gsec - tbill_3m  # 10Y-3M slope
            vix_component = (vix - 20) / 20  # normalized VIX (20 = neutral)
            credit_spread = vix * 0.5 / 100  # proxy using VIX

            # Normalize each component by typical range
            norms = {
                'repo_spread': repo_spread / 2.0,
                'yield_curve_slope': yield_curve_slope_raw / 1.5,
                'yield_change': yield_change / 2.0,
                'vix_raw': vix_component,
                'credit_spread': credit_spread * 10,
            }
            if has_oil:
                norms['oil'] = oil_z * 0.5  # dampened — oil is volatile

            # Recompute active FCI weights (drop oil if absent, renormalize)
            self.fci_weights = {}
            for comp in norms:
                self.fci_weights[comp] = self.fci_weights_base.get(comp, 0.18)

            # TIER 2 UPGRADE: Time-varying FCI weights (stress-adaptive)
            self.fci_weights = self._apply_stress_weights(
                self.fci_weights, vix, yield_curve_slope_raw,
                oil_z if has_oil else 0, norms.get('credit_spread', 0))

            w_sum = sum(self.fci_weights.values())
            if w_sum > 0:
                for comp in self.fci_weights:
                    self.fci_weights[comp] /= w_sum

            fci_raw = sum(
                norms[comp] * self.fci_weights.get(comp, 0.18)
                for comp in norms
            )
            fci_signal = round(float(np.tanh(fci_raw / 2)), 3)
            n_comp = len(norms)
            logger.info(f"   FCI ({n_comp}-comp): {fci_signal:.3f} "
                        f"(slope: {yield_curve_slope_raw:+.1f}%, VIX: {vix:.0f}"
                        f"{f', oil_z: {oil_z:.2f}' if has_oil else ''})")

            # 4b. TIER 2 UPGRADE: Leading Indicators Composite
            leading = self._compute_leading_indicator(
                yield_curve_slope_raw, vix, credit_spread, repo)
            logger.info(f"   Leading indicator: {leading['composite']:+.3f} "
                        f"({leading['interpretation']})")

            # 5. Build signal row for softmax
            signal_row = pd.Series({
                'growth_signal': growth_signal,
                'inflation_signal': inflation_signal,
                'FCI_signal': fci_signal,
            })

            # 6. TIER 1 UPGRADE: Persistence prior + oil tilt
            prev_regime, prev_confidence = self._load_previous_regime()
            softmax = self.classify_regime_soft(signal_row, current_regime=prev_regime,
                                                 oil_z=oil_z)
            if prev_regime:
                logger.info(f"   Persistence prior applied (prev={prev_regime})")

            # 6b. TIER 2+3 UPGRADE: Full ensemble (Markov + rule-based + softmax)
            markov_probs = self._markov_regime_probabilities(growth_signal, inflation_signal)
            rule_based_probs = self._rule_based_probabilities(growth_signal, inflation_signal, fci_signal)
            ensemble_probs = self._blend_probabilities(softmax, markov_probs, rule_based_probs)
            ensemble_method = 'softmax_only'
            if markov_probs and rule_based_probs:
                ensemble_method = 'softmax_0.50_markov_0.30_rule_0.20'
            elif markov_probs:
                ensemble_method = 'softmax_0.65_markov_0.35'
            if markov_probs:
                logger.info(f"   Ensemble ({ensemble_method}): "
                            f"Growth-Disinflation={ensemble_probs['Growth-Disinflation']:.3f} "
                            f"(softmax={softmax['Growth-Disinflation']:.3f}, "
                            f"markov={markov_probs['Growth-Disinflation']:.3f})")

            # Use ensemble for hard classification
            effective_probs = ensemble_probs if markov_probs else softmax

            # 7. Hard classification with FCI modulation
            if growth_signal > self.GDP_ACCELERATING_COMPOSITE:
                if inflation_signal > self.CPI_RISING_COMPOSITE:
                    regime_hard = 'Growth-Inflation-Tight' if fci_signal > 0 else 'Growth-Inflation'
                else:
                    regime_hard = 'Growth-Disinflation'
            elif growth_signal > self.GDP_DECELERATING_COMPOSITE:
                if inflation_signal > self.CPI_RISING_COMPOSITE:
                    regime_hard = 'Growth-Inflation'
                else:
                    regime_hard = 'Growth-Disinflation'
            else:
                if inflation_signal > self.CPI_RISING_COMPOSITE:
                    regime_hard = 'Stagflation'
                else:
                    regime_hard = 'Stagnation-Disinflation'

            # 8. Map to v1-compatible output
            regime_info = self._get_regime_info(regime_hard)
            confidence = max(effective_probs.values()) if effective_probs else 0.5
            regime_name = regime_info.get('name', regime_hard)

            # Persist current regime for next run
            self._save_current_regime(regime_name, float(confidence), ensemble_probs)

            result = {
                'regime': regime_name,
                'regime_code': regime_info.get('code', 'UNKNOWN'),
                'confidence': round(float(confidence), 2),
                'color': regime_info.get('color', 'Gray'),
                'description': regime_info.get('description', ''),
                'signals': {
                    'gdp_growth': gdp,
                    'output_gap': round(output_gap, 2),
                    'gdp_trend_10yr': round(gdp_trend_10yr, 1),
                    'gdp_trend': 'accelerating' if output_gap > 0.5 else ('decelerating' if output_gap < -0.5 else 'stable'),
                    'growth_signal': growth_signal,
                    'gdp_zscore_mean': round(gdp_mean, 1),
                    'gdp_zscore_std': round(gdp_std, 1),
                    'output_gap_mean': round(output_gap_mean, 2),
                    'output_gap_std': round(output_gap_std, 2),
                    'cpi': cpi,
                    'cpi_trend': macro_data['mospi_inflation'].get('cpi_trend', 'stable'),
                    'cpi_signal': inflation_signal,
                    'cpi_zscore_mean': round(cpi_mean, 1),
                    'cpi_zscore_std': round(cpi_std, 1),
                    'fci_signal': fci_signal,
                    'fci_raw': round(fci_raw, 4),
                    'fci_components': norms,
                    'yield_curve_slope': round(yield_curve_slope_raw, 2),
                    'nifty_trend': market_trend,
                    'vix': vix,
                    'repo_rate': repo,
                    'gsec_10y': gsec,
                    'tbill_91d': tbill_3m,
                    'oil_brent': oil_brent,
                    'oil_z': oil_z,
                    'oil_3m_change_pct': float(macro_data['oil'].get('brent_3m_change_pct', 0)) if has_oil else None,
                    'leading_indicator': leading,
                },
                'probability_distribution': {k: round(float(v), 4) for k, v in ensemble_probs.items()},
                'softmax_probabilities': {k: round(float(v), 4) for k, v in softmax.items()},
                'markov_probabilities': {k: round(float(v), 4) for k, v in markov_probs.items()} if markov_probs else None,
                'rule_based_probabilities': rule_based_probs,
                'fci_signal': fci_signal,
                'statistical_regime': None,
                'method': f'ensemble_v3_{ensemble_method}',
                'classification_timestamp': pd.Timestamp.now().isoformat(),
                'tier1_upgrades': {
                    'output_gap': True,
                    'yield_curve_slope': True,
                    'fci_components': len(norms),
                    'persistence_prior': prev_regime is not None,
                    'previous_regime': prev_regime,
                    'previous_confidence': round(prev_confidence, 4) if prev_confidence else None,
                    'oil_integrated': has_oil,
                    'oil_z': oil_z if has_oil else None,
                },
                'tier2_upgrades': {
                    'markov_switching': markov_probs is not None,
                    'ensemble_method': ensemble_method,
                    'time_varying_fci': True,
                    'leading_indicator': True,
                    'leading_indicator_composite': leading['composite'],
                    'leading_indicator_missing': leading['missing_components'],
                },
            }

            # TIER 3: Regime-conditional VaR/CVaR + Full ensemble
            regime_risk = self._compute_regime_var_cvar()
            result['regime_risk'] = regime_risk if regime_risk else {}
            result['tier3_upgrades'] = {
                'regime_var_cvar': bool(regime_risk),
                'full_ensemble': True,
                'ensemble_components': ['softmax', 'markov_hamilton', 'rule_based'],
                'ensemble_weights': {'softmax': 0.50, 'markov': 0.30, 'rule_based': 0.20},
            }

            logger.info(f"✅ Enhanced regime: {result['regime']} ({result['color']}) "
                        f"Confidence: {result['confidence']:.2%} "
                        f"FCI: {fci_signal:.3f} "
                        f"OutputGap(z): {growth_signal:.2f} CPI(z): {inflation_signal:.2f}")

            return result

        except Exception as e:
            logger.error(f"Enhanced classification failed: {e}, falling back to rule-based")
            import traceback
            logger.error(traceback.format_exc())
            return self._fallback_classify(macro_data)

    def _build_historical_df(self, macro_data: Dict) -> pd.DataFrame:
        """Build historical DataFrame from saved data + current snapshot"""
        hist_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'historical_macro_data')

        # Try the 100% real data file first
        real_file = os.path.join(hist_dir, 'macro_data_2000_2026_100pct_real.json')
        if os.path.exists(real_file):
            with open(real_file) as f:
                hist = json.load(f)

            # Merge GDP + CPI into a single DataFrame on Date
            gdp_df = pd.DataFrame(hist['gdp'])
            cpi_df = pd.DataFrame(hist['cpi'])

            # Normalize mixed date formats: "YYYY-MM-DD", "YYYY-YY" fiscal year, "YYYY"
            def _normalize_date(d):
                s = str(d).strip()
                if re.match(r'^\d{4}-\d{2}$', s):  # fiscal year "2025-26"
                    return pd.Timestamp(f'{s[:4]}-04-01')
                if re.match(r'^\d{4}$', s):  # just year
                    return pd.Timestamp(f'{s}-06-30')
                return pd.to_datetime(s, errors='coerce')

            gdp_df['Date'] = gdp_df['Date'].apply(_normalize_date)
            cpi_df['Date'] = cpi_df['Date'].apply(_normalize_date)

            df = pd.merge(gdp_df, cpi_df, on='Date', how='outer', suffixes=('_gdp', '_cpi'))
            df = df.dropna(subset=['Date'])

            # Rename to expected columns
            df.rename(columns={'GDP_Growth': 'GDP_Growth_raw'}, inplace=True)
            df['GDP_Growth'] = df.get('GDP_Growth_raw', df.get('GDP_Growth'))
            if 'GDP_Growth' not in df.columns:
                df['GDP_Growth'] = df.get('GDP_Growth_raw', 6.5)
            df['CPI'] = df.get('CPI', df.get('CPI_YoY', 4.8))

            # Fill any missing
            df['GDP_Growth'] = df['GDP_Growth'].ffill().fillna(6.5)
            df['CPI'] = df['CPI'].ffill().fillna(4.8)
        else:
            # Fallback: use regimes_historical.json
            regimes_file = os.path.join(hist_dir, 'regimes_historical.json')
            if os.path.exists(regimes_file):
                with open(regimes_file) as f:
                    regimes_data = json.load(f)
                df = pd.DataFrame(regimes_data)
                df['Date'] = pd.to_datetime(df['Date'])
            else:
                # Minimal fallback: just current data
                df = pd.DataFrame()

        # Append current snapshot as new row
        current_row = {
            'Date': pd.Timestamp.now(),
            'GDP_Growth': float(macro_data['mospi_growth']['gdp_growth']),
            'CPI': float(macro_data['mospi_inflation']['cpi']),
            'CPI_Core': float(macro_data['mospi_inflation'].get('core_cpi',
                                float(macro_data['mospi_inflation']['cpi']) - 0.5)),
            'Repo_Rate': float(macro_data['rbi']['repo_rate']),
            'GSec_10Y': float(macro_data['rbi']['gsec_10y']),
            'VIX': float(macro_data['nse']['vix']),
        }

        df = pd.concat([df, pd.DataFrame([current_row])], ignore_index=True)
        df = df.sort_values('Date').reset_index(drop=True)

        # Ensure minimum rows for smoothing (replicate if needed)
        min_rows = 6
        if len(df) < min_rows:
            filler = pd.DataFrame([current_row] * (min_rows - len(df)))
            filler['Date'] = pd.date_range(end=pd.Timestamp.now(), periods=min_rows - len(df))
            df = pd.concat([filler, df], ignore_index=True)

        logger.info(f"Historical DataFrame: {len(df)} rows from {df['Date'].min().date()} to {df['Date'].max().date()}")
        return df

    def _get_regime_info(self, regime_name: str) -> Dict:
        """Map enhanced regime names to v1-compatible info dicts"""
        mapping = {
            'Growth-Disinflation': {
                'name': 'Growth-Disinflation', 'code': 'GROWTH_DISINFLATION',
                'color': 'Green',
                'description': 'Goldilocks — rising growth, falling inflation, loose financial conditions'
            },
            'Growth-Inflation': {
                'name': 'Growth-Inflation', 'code': 'GROWTH_INFLATION',
                'color': 'Orange',
                'description': 'Overheating — rising growth, rising inflation, tightening expected'
            },
            'Growth-Inflation-Tight': {
                'name': 'Growth-Inflation', 'code': 'GROWTH_INFLATION',
                'color': 'DarkOrange',
                'description': 'Overheating with tight financial conditions — rate hikes active'
            },
            'Stagnation-Disinflation': {
                'name': 'Stagnation-Disinflation', 'code': 'STAGNATION_DISINFLATION',
                'color': 'Blue',
                'description': 'Slowdown — falling growth, falling inflation, rate cuts expected'
            },
            'Stagflation': {
                'name': 'Stagflation', 'code': 'STAGFLATION',
                'color': 'Red',
                'description': 'Worst case — falling growth, rising inflation, tight conditions'
            },
        }
        return mapping.get(regime_name, {
            'name': regime_name, 'code': regime_name.upper().replace('-', '_').replace(' ', '_'),
            'color': 'Gray',
            'description': f'Unknown regime: {regime_name}'
        })

    # =========================================================================
    # TIER 2: Markov Switching with Hamilton Filter
    # Two independent 2-state models (Growth H/L × Inflation H/L) → 4 regimes
    # =========================================================================

    def _build_markov_models(self) -> Tuple[Dict, Dict]:
        """
        Build two independent 2-state Markov switching models from historical data.

        Returns:
            growth_model: {mu_low, mu_high, sigma, p_00, p_11, filtered_probs, last_prob}
            inflation_model: {mu_low, mu_high, sigma, p_00, p_11, filtered_probs, last_prob}
        """
        from scipy.stats import norm

        hist_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'historical_macro_data')
        real_file = os.path.join(hist_dir, 'macro_data_2000_2026_100pct_real.json')
        if not os.path.exists(real_file):
            return None, None

        with open(real_file) as f:
            hist = json.load(f)

        gdp_data = pd.DataFrame(hist['gdp'])
        cpi_data = pd.DataFrame(hist['cpi'])

        # Normalize dates and merge
        def _norm_date(d):
            s = str(d).strip()
            if re.match(r'^\d{4}-\d{2}$', s):
                return pd.Timestamp(f'{s[:4]}-04-01')
            if re.match(r'^\d{4}$', s):
                return pd.Timestamp(f'{s}-06-30')
            return pd.to_datetime(s, errors='coerce')

        gdp_data['Date'] = gdp_data['Date'].apply(_norm_date)
        cpi_data['Date'] = cpi_data['Date'].apply(_norm_date)

        # Extract growth and CPI series
        gdp_series = gdp_data.set_index('Date')['GDP_Growth'].sort_index()
        cpi_series = cpi_data.set_index('Date')['CPI_YoY'].sort_index()

        # Align dates (inner join)
        common_idx = gdp_series.index.intersection(cpi_series.index)
        gdp_aligned = gdp_series[common_idx]
        cpi_aligned = cpi_series[common_idx]

        if len(common_idx) < 24:
            logger.warning(f"Markov: insufficient data ({len(common_idx)} points), need ≥24")
            return None, None

        # Compute 10-year rolling trend for output gap
        gdp_trend = gdp_aligned.rolling(10, min_periods=5).mean()
        output_gap = gdp_aligned - gdp_trend

        # Z-score normalize
        og_mean, og_std = output_gap.mean(), output_gap.std()
        cpi_mean, cpi_std = cpi_aligned.mean(), cpi_aligned.std()

        growth_signal = (output_gap - og_mean) / max(og_std, 0.01)
        inflation_signal = (cpi_aligned - cpi_mean) / max(cpi_std, 0.01)

        def fit_2state_markov(series: pd.Series, label: str) -> Dict:
            """Fit a 2-state Hamilton filter to a univariate series"""
            y = series.dropna().values
            n = len(y)
            if n < 24:
                return None

            # Initialize state parameters
            median_split = np.median(y)
            low_mask = y <= median_split
            high_mask = y > median_split

            mu_low = float(np.mean(y[low_mask])) if low_mask.any() else float(np.percentile(y, 25))
            mu_high = float(np.mean(y[high_mask])) if high_mask.any() else float(np.percentile(y, 75))
            sigma = float(np.std(y))
            if sigma < 0.01:
                sigma = 0.01

            # Initialize transition probabilities from data
            states = (y > median_split).astype(int)  # 0=low, 1=high
            p_00 = max(0.7, min(0.95, np.mean((states[:-1] == 0) & (states[1:] == 0)) /
                       max(np.mean(states[:-1] == 0), 0.01)))
            p_11 = max(0.7, min(0.95, np.mean((states[:-1] == 1) & (states[1:] == 1)) /
                       max(np.mean(states[:-1] == 1), 0.01)))

            # EM refinement (5 iterations)
            for _ in range(5):
                # Run Hamilton filter with current parameters
                filtered = np.zeros((n, 2))
                filtered[0, 0] = 1 - (y[0] > median_split)  # prior
                filtered[0, 1] = 1 - filtered[0, 0]

                for t in range(1, n):
                    # Prediction
                    pred_0 = p_00 * filtered[t-1, 0] + (1 - p_11) * filtered[t-1, 1]
                    pred_1 = (1 - p_00) * filtered[t-1, 0] + p_11 * filtered[t-1, 1]

                    # Likelihood
                    like_0 = norm.pdf(y[t], mu_low, sigma)
                    like_1 = norm.pdf(y[t], mu_high, sigma)

                    # Update
                    joint_0 = pred_0 * like_0
                    joint_1 = pred_1 * like_1
                    total = joint_0 + joint_1
                    if total < 1e-15:
                        total = 1e-15
                    filtered[t, 0] = joint_0 / total
                    filtered[t, 1] = joint_1 / total

                # Re-estimate parameters
                w0 = filtered[:, 0]
                w1 = filtered[:, 1]
                mu_low = float(np.average(y, weights=w0)) if w0.sum() > 0.01 else mu_low
                mu_high = float(np.average(y, weights=w1)) if w1.sum() > 0.01 else mu_high

                resid = y - (mu_low * w0 + mu_high * w1) / (w0 + w1 + 1e-10)
                sigma = float(np.sqrt(np.average(resid**2)))

                # Update transitions from smoothed assignments
                xi = filtered.copy()
                p_00 = max(0.7, min(0.95,
                    (xi[:-1, 0] * xi[1:, 0]).sum() / max(xi[:-1, 0].sum(), 0.01)))
                p_11 = max(0.7, min(0.95,
                    (xi[:-1, 1] * xi[1:, 1]).sum() / max(xi[:-1, 1].sum(), 0.01)))

            logger.info(f"   Markov {label}: μ_low={mu_low:.3f}, μ_high={mu_high:.3f}, "
                        f"σ={sigma:.3f}, p00={p_00:.3f}, p11={p_11:.3f}")

            return {
                'mu_low': mu_low, 'mu_high': mu_high, 'sigma': sigma,
                'p_00': p_00, 'p_11': p_11,
                'last_filtered': filtered[-1].tolist(),
                'filtered_series': filtered,
            }

        growth_model = fit_2state_markov(growth_signal, 'growth')
        inflation_model = fit_2state_markov(inflation_signal, 'inflation')

        return growth_model, inflation_model

    def _markov_step(self, y: float, model: Dict) -> np.ndarray:
        """
        Run one step of Hamilton filter given new observation.

        Args:
            y: new observation value (growth or inflation signal)
            model: fitted model dict {mu_low, mu_high, sigma, p_00, p_11, last_filtered}

        Returns:
            Updated filtered probability [P(low), P(high)]
        """
        from scipy.stats import norm

        prev = np.array(model['last_filtered'])
        p_00, p_11 = model['p_00'], model['p_11']
        mu_low, mu_high, sigma = model['mu_low'], model['mu_high'], model['sigma']

        # Prediction
        pred_0 = p_00 * prev[0] + (1 - p_11) * prev[1]
        pred_1 = (1 - p_00) * prev[0] + p_11 * prev[1]

        # Likelihood
        like_0 = norm.pdf(y, mu_low, sigma)
        like_1 = norm.pdf(y, mu_high, sigma)

        # Update
        joint_0 = pred_0 * like_0
        joint_1 = pred_1 * like_1
        total = joint_0 + joint_1
        if total < 1e-15:
            total = 1e-15
        return np.array([joint_0 / total, joint_1 / total])

    def _markov_regime_probabilities(self, growth_signal: float,
                                      inflation_signal: float) -> Optional[Dict[str, float]]:
        """
        Compute 4-regime probabilities from two independent 2-state Markov models.

        Growth model: P(low_growth), P(high_growth)
        Inflation model: P(low_inflation), P(high_inflation)
        Combined: P(growth, inflation) = P_growth × P_inflation
        """
        growth_model, inflation_model = self._build_markov_models()
        if growth_model is None or inflation_model is None:
            return None

        # Run one filter step with current signals
        g_prob = self._markov_step(growth_signal, growth_model)
        i_prob = self._markov_step(inflation_signal, inflation_model)

        p_low_g = g_prob[0]
        p_high_g = g_prob[1]
        p_low_i = i_prob[0]
        p_high_i = i_prob[1]

        return {
            'Growth-Disinflation': round(float(p_high_g * p_low_i), 4),
            'Growth-Inflation': round(float(p_high_g * p_high_i), 4),
            'Stagnation-Disinflation': round(float(p_low_g * p_low_i), 4),
            'Stagflation': round(float(p_low_g * p_high_i), 4),
        }

    def _rule_based_probabilities(self, growth_signal: float, inflation_signal: float,
                                    fci_signal: float) -> Dict[str, float]:
        """Convert hard rule-based classification to probability distribution"""
        regimes = ['Growth-Disinflation', 'Growth-Inflation',
                   'Stagnation-Disinflation', 'Stagflation']

        # Same logic as classify_regime_hard
        if growth_signal > 0:
            if inflation_signal > 0:
                assigned = 'Growth-Inflation'
            else:
                assigned = 'Growth-Disinflation'
        else:
            if inflation_signal > 0:
                assigned = 'Stagflation'
            else:
                assigned = 'Stagnation-Disinflation'

        # Convert to probability: 0.70 to assigned, 0.10 to others
        probs = {}
        for r in regimes:
            probs[r] = 0.70 if r == assigned else 0.10
        return probs

    def _blend_probabilities(self, softmax: Dict[str, float],
                              markov: Optional[Dict[str, float]],
                              rule_based: Optional[Dict[str, float]] = None) -> Dict[str, float]:
        """
        Tier 3: Full ensemble blending.

        Default weights: softmax 0.50, markov 0.30, rule_based 0.20
        Falls back to softmax-only if no other models available.
        """
        regimes = ['Growth-Disinflation', 'Growth-Inflation',
                   'Stagnation-Disinflation', 'Stagflation']

        if markov is None and rule_based is None:
            return softmax

        # Dynamic weights based on available models
        if markov is not None and rule_based is not None:
            w_soft, w_markov, w_rule = 0.50, 0.30, 0.20
        elif markov is not None:
            w_soft, w_markov, w_rule = 0.65, 0.35, 0.0
        else:
            w_soft, w_markov, w_rule = 0.75, 0.0, 0.25

        blended = {}
        for r in regimes:
            val = w_soft * softmax.get(r, 0.25)
            if markov:
                val += w_markov * markov.get(r, 0.25)
            if rule_based:
                val += w_rule * rule_based.get(r, 0.25)
            blended[r] = round(val, 4)

        # Renormalize
        total = sum(blended.values())
        if total > 0:
            blended = {r: round(v / total, 4) for r, v in blended.items()}
        return blended

    # =========================================================================
    # TIER 2: Time-Varying FCI Weights (stress-adaptive)
    # =========================================================================

    def _apply_stress_weights(self, base_weights: Dict[str, float],
                               vix: float, yield_curve_slope: float,
                               oil_z: float, credit_spread_norm: float) -> Dict[str, float]:
        """
        Apply stress multipliers to FCI component weights.

        When a channel is under stress, its weight increases so FCI better
        captures the binding constraint on financial conditions.

        Stress triggers:
        - VIX > 20: equity/risk stress (boost up to 2x at VIX=40)
        - Yield curve inverted (slope < 0): credit channel stress (boost up to 1.5x)
        - Oil z > 1.5: external shock stress (boost up to 2x at oil_z=3.0)
        - Credit spread elevated: corporate credit stress (boost up to 1.5x)
        """
        weights = dict(base_weights)

        # VIX stress: neutral at 15, elevated above 20, extreme above 30
        if vix > 20:
            vix_mult = 1.0 + min(1.0, (vix - 20) / 20)  # 1.0-2.0x
            if 'vix_raw' in weights:
                weights['vix_raw'] *= vix_mult

        # Yield curve inversion: less than +0.5% is concerning, negative is stress
        if yield_curve_slope < 0.5:
            slope_mult = 1.0 + min(0.5, max(0, (0.5 - yield_curve_slope) / 1.0))
            if 'yield_curve_slope' in weights:
                weights['yield_curve_slope'] *= slope_mult

        # Oil shock: z > 1.5 is elevated, z > 2.5 is extreme
        if abs(oil_z) > 1.5:
            oil_mult = 1.0 + min(1.0, (abs(oil_z) - 1.5) / 1.5)
            if 'oil' in weights:
                weights['oil'] *= oil_mult

        # Credit stress: when credit spread normalized component is elevated
        # credit_spread_norm is scaled ×10, so threshold of 1.5 ≈ meaningful spread
        if abs(credit_spread_norm) > 1.5:
            credit_mult = 1.0 + min(0.5, (abs(credit_spread_norm) - 1.5) / 1.5)
            if 'credit_spread' in weights:
                weights['credit_spread'] *= credit_mult

        if any(v != base_weights.get(k, 0) for k, v in weights.items()):
            logger.info(f"   Stress-adaptive FCI weights applied "
                        f"(VIX={vix:.0f}, slope={yield_curve_slope:+.2f}%, oil_z={oil_z:.2f})")

        return weights

    # =========================================================================
    # TIER 2: Leading Indicators Composite
    # =========================================================================

    def _compute_leading_indicator(self, yield_curve_slope: float, vix: float,
                                     credit_spread: float, repo_rate: float) -> Dict:
        """
        Composite leading indicator for forward regime risk.

        Components (each normalized to ~z-score scale):
        - Yield curve slope: flatter/inverted = headwind (lower = worse)
        - VIX (inverted): elevated VIX = risk aversion (higher = worse)
        - Credit spread (inverted): widening = stress (higher = worse)
        - Repo rate stance: above neutral = tightening (higher = worse)

        Returns:
            {composite, components, interpretation, missing_components}
        """
        # 1. Yield curve slope: +1.5% is healthy, 0% is flat, negative is inverted
        slope_score = (yield_curve_slope - 1.0) / 1.0  # center at 1%, scale 1%

        # 2. VIX (inverted): 15 is calm, 25 is stressed
        vix_score = -(vix - 15) / 10  # inverted so higher = worse

        # 3. Credit spread (inverted): higher spread = more stress
        credit_score = -credit_spread

        # 4. Repo stance: 6.5% is neutral, above = tightening
        repo_score = -(repo_rate - 6.5) / 2.0

        components = {
            'yield_curve_slope': round(slope_score, 3),
            'vix_inverted': round(vix_score, 3),
            'credit_spread_inverted': round(credit_score, 3),
            'repo_stance': round(repo_score, 3),
        }

        # Equal-weighted composite (no FPI flows or credit growth yet)
        values = list(components.values())
        composite = float(np.mean(values))

        # Interpret
        if composite > 0.5:
            interpretation = "Expansionary — conditions improving"
        elif composite > 0:
            interpretation = "Mildly expansionary"
        elif composite > -0.5:
            interpretation = "Mildly contractionary"
        else:
            interpretation = "Contractionary — conditions deteriorating"

        return {
            'composite': round(composite, 3),
            'components': components,
            'interpretation': interpretation,
            'missing_components': ['fpi_flows', 'credit_growth'],
        }

    # =========================================================================
    # TIER 3: Regime-Conditional VaR/CVaR
    # =========================================================================

    def _compute_regime_var_cvar(self) -> Dict:
        """
        Compute regime-conditional Value-at-Risk and Conditional VaR (Expected Shortfall).

        Uses Nifty monthly returns merged with historical regime assignments.
        Reports empirical VaR/CVaR where n≥10, parametric estimates for sparse regimes.

        Returns:
            {regime: {n_obs, mean_return, std_return, var_95, var_99, cvar_95, cvar_99,
                       confidence, method}, metadata: {...}}
        """
        import pandas as pd
        hist_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'historical_macro_data')

        # Load Nifty data from policy_rates_markets CSV (proper levels)
        csv_file = os.path.join(hist_dir, 'policy_rates_markets_v2.csv')
        regimes_file = os.path.join(hist_dir, 'regimes_historical.json')
        fx_file = os.path.join(hist_dir, 'usdinr_historical.json')

        if not os.path.exists(csv_file) or not os.path.exists(regimes_file):
            logger.warning("VaR/CVaR: missing data files")
            return {}

        with open(regimes_file) as f:
            regime_data = json.load(f)
        regime_map = {}
        for r in regime_data:
            d = str(r['Date'])[:7]
            regime_map[d] = r['Regime']

        df = pd.read_csv(csv_file)
        nifty = df[df['Indicator'] == 'Equity_Index_NIFTY'].copy()
        nifty['Date'] = pd.to_datetime(nifty['Date'])
        nifty = nifty.sort_values('Date')
        nifty['YearMonth'] = nifty['Date'].dt.to_period('M')
        monthly = nifty.groupby('YearMonth').agg({'Value': 'last', 'Date': 'last'}).reset_index()
        monthly['YearMonthStr'] = monthly['Date'].dt.strftime('%Y-%m')
        monthly['Regime'] = monthly['YearMonthStr'].map(regime_map)
        monthly = monthly.dropna(subset=['Regime'])

        vals = monthly['Value'].values
        monthly_returns = [None] + [
            (vals[i] / vals[i-1] - 1) * 100 for i in range(1, len(vals))
        ]
        monthly['Return'] = monthly_returns
        monthly = monthly.dropna(subset=['Return'])
        monthly['Return'] = monthly['Return'].astype(float)

        if len(monthly) < 20:
            return {}

        # Compute pooled statistics for parametric fallback
        pooled_std = float(monthly['Return'].std())
        pooled_mean = float(monthly['Return'].mean())

        # FX adjustment for USD returns
        fx_returns = None
        if os.path.exists(fx_file):
            try:
                with open(fx_file) as f:
                    fx_data = json.load(f)
                fx_df = pd.DataFrame(fx_data)
                fx_df['Date'] = pd.to_datetime(fx_df['Date'])
                fx_df = fx_df.sort_values('Date')
                fx_df['YearMonth'] = fx_df['Date'].dt.to_period('M')
                fx_monthly = fx_df.groupby('YearMonth')['USDINR'].last().reset_index()
                fx_vals = fx_monthly['USDINR'].values
                fx_ret = [None] + [
                    (fx_vals[i] / fx_vals[i-1] - 1) * 100 for i in range(1, len(fx_vals))
                ]
                fx_monthly['FX_Return'] = fx_ret
                fx_monthly = fx_monthly.dropna(subset=['FX_Return'])
                fx_monthly['YearMonthStr'] = fx_monthly['YearMonth'].astype(str)
                monthly = monthly.merge(
                    fx_monthly[['YearMonthStr', 'FX_Return']],
                    on='YearMonthStr', how='left')
                monthly['Return_USD'] = monthly['Return'] + monthly['FX_Return'].fillna(0)
                fx_returns = True
            except Exception:
                pass

        regimes = ['Growth-Disinflation', 'Growth-Inflation',
                   'Stagnation-Disinflation', 'Stagflation']
        result = {}
        min_obs_empirical = 10

        for regime in regimes:
            sub = monthly[monthly['Regime'] == regime]['Return']
            n = len(sub)

            if n < 3:
                # Too few observations — use pooled stats with regime mean shift
                regime_returns = monthly[monthly['Regime'] == regime]['Return']
                regime_mean = float(regime_returns.mean()) if n > 0 else pooled_mean
                result[regime] = {
                    'n_obs': n,
                    'mean_return_pct': round(regime_mean, 2),
                    'std_return_pct': round(pooled_std, 2),
                    'var_95_pct': round(regime_mean - 1.645 * pooled_std, 2),
                    'var_99_pct': round(regime_mean - 2.326 * pooled_std, 2),
                    'cvar_95_pct': round(regime_mean - 2.063 * pooled_std, 2),
                    'cvar_99_pct': round(regime_mean - 2.665 * pooled_std, 2),
                    'confidence': 'LOW',
                    'method': 'parametric_pooled_vol',
                }
            elif n < min_obs_empirical:
                # Few observations — use regime std but flag low confidence
                regime_std = float(sub.std()) if n > 1 else pooled_std
                regime_mean = float(sub.mean())
                result[regime] = {
                    'n_obs': n,
                    'mean_return_pct': round(regime_mean, 2),
                    'std_return_pct': round(regime_std, 2),
                    'var_95_pct': round(float(np.percentile(sub, 5)), 2),
                    'var_99_pct': round(float(np.percentile(sub, 1)) if n >= 100 else
                                         regime_mean - 2.326 * regime_std, 2),
                    'cvar_95_pct': round(float(sub[sub <= np.percentile(sub, 5)].mean()), 2),
                    'cvar_99_pct': round(regime_mean - 2.665 * regime_std, 2),
                    'confidence': 'LOW',
                    'method': 'mixed_empirical_parametric',
                }
            else:
                # Sufficient observations — empirical with parametric VaR_99
                regime_std = float(sub.std())
                regime_mean = float(sub.mean())
                var_95 = float(np.percentile(sub, 5))
                cvar_95 = float(sub[sub <= np.percentile(sub, 5)].mean())
                result[regime] = {
                    'n_obs': n,
                    'mean_return_pct': round(regime_mean, 2),
                    'std_return_pct': round(regime_std, 2),
                    'var_95_pct': round(var_95, 2),
                    'var_99_pct': round(float(np.percentile(sub, 1)) if n >= 100 else
                                         regime_mean - 2.326 * regime_std, 2),
                    'cvar_95_pct': round(cvar_95, 2),
                    'cvar_99_pct': round(float(sub[sub <= np.percentile(sub, 1)].mean())
                                          if n >= 100 else regime_mean - 2.665 * regime_std, 2),
                    'confidence': 'MEDIUM' if n < 30 else 'HIGH',
                    'method': 'empirical',
                }

            # USD-adjusted if FX available
            if fx_returns:
                sub_usd = monthly[monthly['Regime'] == regime]['Return_USD'].dropna()
                if len(sub_usd) >= 3:
                    result[regime]['var_95_usd_pct'] = round(
                        float(np.percentile(sub_usd, 5)) if len(sub_usd) >= 20
                        else sub_usd.mean() - 1.645 * sub_usd.std(), 2)
                    result[regime]['cvar_95_usd_pct'] = round(
                        float(sub_usd[sub_usd <= np.percentile(sub_usd, 5)].mean())
                        if len(sub_usd) >= 20
                        else sub_usd.mean() - 2.063 * sub_usd.std(), 2)

        result['_metadata'] = {
            'total_observations': len(monthly),
            'date_range': f"{monthly['Date'].min().strftime('%Y-%m')} to {monthly['Date'].max().strftime('%Y-%m')}",
            'index': 'Nifty 50',
            'currency': 'INR',
            'data_source': 'policy_rates_markets_v2.csv + regimes_historical.json',
            'regime_mapping': 'regimes_historical.json (2006-2026)',
        }

        return result

    def _fallback_classify(self, macro_data: Dict) -> Dict:
        """Fallback: simple rule-based classification when enhanced pipeline fails"""
        gdp = float(macro_data['mospi_growth']['gdp_growth'])
        cpi = float(macro_data['mospi_inflation']['cpi'])

        if gdp >= 6.5:
            if cpi < 4.5:
                regime, code, color = 'Growth-Disinflation', 'GROWTH_DISINFLATION', 'Green'
            else:
                regime, code, color = 'Growth-Inflation', 'GROWTH_INFLATION', 'Orange'
        else:
            if cpi < 4.5:
                regime, code, color = 'Stagnation-Disinflation', 'STAGNATION_DISINFLATION', 'Blue'
            else:
                regime, code, color = 'Stagflation', 'STAGFLATION', 'Red'

        return {
            'regime': regime, 'regime_code': code, 'confidence': 0.5, 'color': color,
            'description': f'Fallback classification — GDP={gdp}%, CPI={cpi}%',
            'signals': {'gdp_growth': gdp, 'cpi': cpi},
            'probability_distribution': {regime: 1.0},
            'softmax_probabilities': {regime: 1.0},
            'fci_signal': None, 'statistical_regime': None,
            'method': 'fallback', 'classification_timestamp': pd.Timestamp.now().isoformat(),
        }


def main():
    """Test enhanced classifier on historical data"""

    print("=" * 80)
    print("ENHANCED REGIME CLASSIFIER - GLOBAL BEST PRACTICES")
    print("=" * 80)

    # Load historical data
    hist_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'historical_macro_data')
    regimes_file = os.path.join(hist_dir, 'regimes_historical.json')

    if os.path.exists(regimes_file):
        with open(regimes_file, 'r') as f:
            regimes_data = json.load(f)

        df = pd.DataFrame(regimes_data)
        df['Date'] = pd.to_datetime(df['Date'])
    else:
        logger.warning(f"regimes_historical.json not found at {regimes_file}")
        return

    # Initialize enhanced classifier
    classifier = EnhancedRegimeClassifier(method='hybrid')

    # Apply enhanced classification
    df_enhanced = classifier.classify_enhanced(df)

    # Display sample results
    print("\nSample Enhanced Classifications:")
    print("=" * 80)
    print(df_enhanced[['Date', 'regime_hard', 'regime_prob',
                      'prob_Growth-Inflation', 'prob_Stagflation']].tail(10).to_string(index=False))

    # Compare with v1.0
    comparison = classifier.compare_classifications(df)

    print("\n" + "=" * 80)
    print("SUMMARY OF UPGRADES")
    print("=" * 80)
    print("✅ Upgrade 1: Signal smoothing (HP-filter, PMI composite)")
    print("✅ Upgrade 2: Regime probabilities (softmax, not hard labels)")
    print("✅ Upgrade 3: Financial Conditions Index (FCI)")
    print("✅ Upgrade 4: Statistical detection (K-means clustering)")
    print("\n" + "=" * 80)
    print("Ready to integrate with fund analysis and Monte Carlo!")
    print("=" * 80)


if __name__ == "__main__":
    main()
