#!/usr/bin/env python3
"""
Markov Transition Model for Macro Regimes

Builds a 4x4 Markov transition matrix from historical regime data
and provides regime prediction capabilities.

Author: TresoWealth Analytics Team
Date: March 27, 2026
Version: 1.0
"""

import sys
sys.path.insert(0, '/Users/akshayrandeva/Treso/treso_analytics')

import pandas as pd
import numpy as np
import json
from datetime import datetime
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RegimeTransitionModel:
    """
    Markov transition model for regime predictions

    Builds transition matrix from historical data and provides:
    - Transition probabilities between regimes
    - Regime duration statistics
    - Future regime distribution predictions
    """

    def __init__(self):
        self.regimes = ['Growth-Disinflation', 'Growth-Inflation',
                       'Stagnation-Disinflation', 'Stagflation']
        self.regime_codes = ['GROWTH_DISINFLATION', 'GROWTH_INFLATION',
                            'STAGNATION_DISINFLATION', 'STAGFLATION']
        self.colors = ['Green', 'Orange', 'Blue', 'Red']

        self.transition_matrix = None
        self.regime_durations = None
        self.stationary_distribution = None

    def load_historical_data(self, filepath: str = 'historical_macro_data/regimes_historical.json') -> pd.DataFrame:
        """Load historical regime data"""
        with open(filepath, 'r') as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')

        logger.info(f"Loaded {len(df)} months of historical regime data")
        return df

    def build_transition_matrix(self, df: pd.DataFrame) -> np.ndarray:
        """
        Build 4x4 Markov transition matrix from historical data

        Returns:
            4x4 numpy array where P[i,j] = P(regime_j | regime_i)
        """
        logger.info("Building Markov transition matrix...")

        # Initialize transition count matrix
        n_regimes = len(self.regimes)
        transition_counts = np.zeros((n_regimes, n_regimes), dtype=int)

        # Create regime to index mapping
        regime_to_idx = {regime: i for i, regime in enumerate(self.regimes)}

        # Count transitions (including self-transitions)
        for i in range(1, len(df)):
            prev_regime = df.iloc[i-1]['Regime']
            curr_regime = df.iloc[i]['Regime']

            prev_idx = regime_to_idx[prev_regime]
            curr_idx = regime_to_idx[curr_regime]

            transition_counts[prev_idx, curr_idx] += 1

        # Convert counts to probabilities (normalize rows)
        transition_matrix = np.zeros((n_regimes, n_regimes))

        for i in range(n_regimes):
            row_sum = transition_counts[i].sum()
            if row_sum > 0:
                transition_matrix[i] = transition_counts[i] / row_sum
            else:
                # If no transitions from this regime, uniform distribution
                transition_matrix[i] = np.ones(n_regimes) / n_regimes

        self.transition_matrix = transition_matrix

        # Log transition matrix
        logger.info("Transition Matrix (from → to):")
        logger.info("-" * 80)
        for i, regime in enumerate(self.regimes):
            probs = transition_matrix[i]
            logger.info(f"{regime:25s}: {probs}")

        return transition_matrix

    def calculate_regime_durations(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculate regime duration statistics

        Returns:
            Dict with avg duration, min, max for each regime
        """
        logger.info("Calculating regime durations...")

        regime_durations = {regime: [] for regime in self.regimes}

        # Find all regime periods
        current_regime = None
        start_date = None

        for _, row in df.iterrows():
            if current_regime != row['Regime']:
                if current_regime is not None:
                    duration = (row['Date'] - start_date).days / 30.44  # Convert to months
                    regime_durations[current_regime].append(duration)

                current_regime = row['Regime']
                start_date = row['Date']

        # Don't forget the last regime
        if current_regime is not None:
            duration = (df.iloc[-1]['Date'] - start_date).days / 30.44
            regime_durations[current_regime].append(duration)

        # Calculate statistics
        stats = {}
        for regime, durations in regime_durations.items():
            if durations:
                stats[regime] = {
                    'avg_duration_months': round(np.mean(durations), 1),
                    'min_duration_months': round(np.min(durations), 1),
                    'max_duration_months': round(np.max(durations), 1),
                    'num_periods': len(durations),
                    'std_dev_months': round(np.std(durations), 1) if len(durations) > 1 else 0
                }

        self.regime_durations = stats

        # Log statistics
        logger.info("Regime Duration Statistics:")
        logger.info("-" * 80)
        for regime, stat in stats.items():
            logger.info(f"{regime:25s}: avg={stat['avg_duration_months']:4.1f}m, "
                       f"min={stat['min_duration_months']:3.1f}m, "
                       f"max={stat['max_duration_months']:4.1f}m, "
                       f"periods={stat['num_periods']:2d}")

        return stats

    def calculate_stationary_distribution(self, max_iter: int = 1000, tol: float = 1e-10) -> np.ndarray:
        """
        Calculate stationary distribution of the Markov chain

        Returns:
            Probability vector of length 4 representing long-term regime probabilities
        """
        if self.transition_matrix is None:
            raise ValueError("Transition matrix not built. Call build_transition_matrix() first.")

        logger.info("Calculating stationary distribution...")

        # Power iteration method
        n = len(self.regimes)
        pi = np.ones(n) / n  # Start with uniform distribution

        for _ in range(max_iter):
            pi_new = pi @ self.transition_matrix
            if np.linalg.norm(pi_new - pi) < tol:
                break
            pi = pi_new

        self.stationary_distribution = pi_new

        logger.info("Stationary Distribution:")
        logger.info("-" * 80)
        for i, regime in enumerate(self.regimes):
            logger.info(f"{regime:25s}: {pi_new[i]:.4f} ({pi_new[i]*100:.1f}%)")

        return pi_new

    def predict_regime_distribution(self, current_regime: str, horizon_months: int) -> Dict[str, float]:
        """
        Predict regime distribution after H months

        Args:
            current_regime: Current regime name
            horizon_months: Months ahead to predict

        Returns:
            Dict mapping regime names to probabilities
        """
        if self.transition_matrix is None:
            raise ValueError("Transition matrix not built. Call build_transition_matrix() first.")

        # Get current regime index
        regime_to_idx = {regime: i for i, regime in enumerate(self.regimes)}
        current_idx = regime_to_idx[current_regime]

        # Create one-hot vector for current regime
        current_state = np.zeros(len(self.regimes))
        current_state[current_idx] = 1.0

        # Multiply by transition matrix H times
        future_state = current_state
        for _ in range(horizon_months):
            future_state = future_state @ self.transition_matrix

        # Convert to dict
        distribution = {
            regime: future_state[i]
            for i, regime in enumerate(self.regimes)
        }

        return distribution

    def get_expected_regime_duration(self, regime: str) -> float:
        """Get expected duration of a regime (in months)"""
        if self.regime_durations is None:
            raise ValueError("Regime durations not calculated.")

        return self.regime_durations[regime]['avg_duration_months']

    def save_to_json(self, output_dir: str = 'historical_macro_data'):
        """Save transition model to JSON files"""
        import os
        os.makedirs(output_dir, exist_ok=True)

        # Save transition matrix
        transition_dict = {
            self.regimes[i]: {
                self.regimes[j]: float(self.transition_matrix[i, j])
                for j in range(len(self.regimes))
            }
            for i in range(len(self.regimes))
        }

        with open(f'{output_dir}/transition_matrix.json', 'w') as f:
            json.dump(transition_dict, f, indent=2)
        logger.info(f"Saved transition_matrix.json")

        # Save regime durations
        with open(f'{output_dir}/regime_durations.json', 'w') as f:
            json.dump(self.regime_durations, f, indent=2)
        logger.info(f"Saved regime_durations.json")

        # Save stationary distribution
        stationary_dict = {
            self.regimes[i]: float(self.stationary_distribution[i])
            for i in range(len(self.regimes))
        }

        with open(f'{output_dir}/stationary_distribution.json', 'w') as f:
            json.dump(stationary_dict, f, indent=2)
        logger.info(f"Saved stationary_distribution.json")

        logger.info("✅ All transition model files saved!")


def main():
    """Build and save the Markov transition model"""

    print("=" * 80)
    print("BUILDING MARKOV TRANSITION MODEL")
    print("=" * 80)

    # Initialize model
    model = RegimeTransitionModel()

    # Load historical data
    df = model.load_historical_data()

    # Build transition matrix
    model.build_transition_matrix(df)

    # Calculate regime durations
    model.calculate_regime_durations(df)

    # Calculate stationary distribution
    model.calculate_stationary_distribution()

    # Example predictions
    print("\n" + "=" * 80)
    print("EXAMPLE PREDICTIONS")
    print("=" * 80)

    current_regime = 'Growth-Inflation'

    for horizon in [1, 3, 6, 12]:
        distribution = model.predict_regime_distribution(current_regime, horizon)
        print(f"\n{current_regime} → {horizon} months ahead:")
        for regime, prob in sorted(distribution.items(), key=lambda x: -x[1]):
            print(f"  {regime:25s}: {prob:.4f} ({prob*100:.1f}%)")

    # Save to JSON
    print("\n" + "=" * 80)
    model.save_to_json()

    print("\n" + "=" * 80)
    print("✅ TRANSITION MODEL COMPLETE")
    print("=" * 80)

    return model


if __name__ == "__main__":
    main()
