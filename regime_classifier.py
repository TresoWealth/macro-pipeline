#!/usr/bin/env python3
"""
⚠️ DEPRECATED as of 2026-05-07 — use enhanced_regime_classifier.py instead.

Regime Classification Engine for TresoWealth Analytics

This v1 classifier is retained as a reference and fallback only.
The pipeline now uses EnhancedRegimeClassifier which adds:
- FCI (Financial Conditions Index) — 4 components
- Softmax probability distribution (not hard labels)
- Z-score normalization against historical distribution (2000-2026)
- Historical context from macro_data_2000_2026_100pct_real.json

To migrate: replace `RegimeClassifier().classify_regime(macro_data)` with
`EnhancedRegimeClassifier(method='hybrid').classify_current_enhanced(macro_data)`

Classifies current market regime into 4 categories:
1. Growth-Disinflation (Green - "Goldilocks")
2. Growth-Inflation (Orange - "Overheating")
3. Stagnation-Disinflation (Blue - "Slowdown")
4. Stagflation (Red - "Worst Case")

Author: TresoWealth Analytics Team
Date: March 26, 2026
Version: 1.0 (4-Regime Framework)
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RegimeClassifier:
    """
    4-Regime Classification Engine

    Uses rule-based algorithm with confidence scoring to classify
    the current market regime based on macroeconomic indicators.

    Regimes:
    1. Growth-Disinflation: GDP ↑, CPI ↓
    2. Growth-Inflation: GDP ↑, CPI ↑
    3. Stagnation-Disinflation: GDP ↓, CPI ↓
    4. Stagflation: GDP ↓, CPI ↑
    """

    def __init__(self):
        # Thresholds for classification
        self.GDP_ACCELERATING = 6.5  # % growth
        self.GDP_DECELERATING = 6.0  # % growth
        self.CPI_RISING = 5.0  # % inflation
        self.CPI_FALLING = 4.5  # % inflation
        self.CPI_HIGH = 6.0  # % inflation

        # Market thresholds
        self.PMI_EXPANSION = 55.0
        self.PMI_SLOWDOWN = 52.0
        self.PMI_CONTRACTION = 50.0
        self.VIX_HIGH = 25.0

    # ========================================================================
    # REGIME CLASSIFICATION
    # ========================================================================

    def classify_regime(self, macro_data: Dict) -> Dict[str, any]:
        """
        Classify the current market regime

        Args:
            macro_data: Dict with keys:
                - rbi: {repo_rate, gsec_10y}
                - mospi_inflation: {cpi, cpi_trend, core_cpi}
                - mospi_growth: {iip, gdp_growth}
                - nse: {nifty_50, market_trend, vix}

        Returns:
            dict: {
                'regime': str,
                'regime_code': str,
                'confidence': float (0-1),
                'color': str,
                'description': str,
                'signals': dict,
                'probability_distribution': dict,
                'classification_timestamp': str
            }
        """
        logger.info("Classifying market regime...")

        # Extract indicators
        indicators = self._extract_indicators(macro_data)

        # Classify based on GDP and CPI
        regime = self._classify_by_growth_and_inflation(indicators)

        # Calculate confidence
        confidence = self._calculate_confidence(indicators, regime)

        # Get probability distribution across all regimes
        prob_distribution = self._get_probability_distribution(indicators)

        result = {
            'regime': regime['name'],
            'regime_code': regime['code'],
            'confidence': confidence,
            'color': regime['color'],
            'description': regime['description'],
            'signals': indicators,
            'probability_distribution': prob_distribution,
            'classification_timestamp': datetime.now().isoformat()
        }

        logger.info(f"✅ Regime classified: {result['regime']} ({result['color']}) - Confidence: {result['confidence']:.2%}")

        return result

    def _extract_indicators(self, macro_data: Dict) -> Dict[str, any]:
        """Extract and normalize indicators from macro data"""

        try:
            # Extract GDP indicators
            gdp_growth = macro_data['mospi_growth']['gdp_growth']
            iip = macro_data['mospi_growth']['iip']

            # Determine GDP trend
            if gdp_growth > self.GDP_ACCELERATING:
                gdp_trend = 'accelerating'
                gdp_signal = 1  # Strong positive
            elif gdp_growth < self.GDP_DECELERATING:
                gdp_trend = 'decelerating'
                gdp_signal = -1  # Strong negative
            else:
                gdp_trend = 'stable'
                gdp_signal = 0  # Neutral

            # Extract inflation indicators
            cpi = macro_data['mospi_inflation']['cpi']
            cpi_trend = macro_data['mospi_inflation'].get('cpi_trend', 'stable')

            # Normalize CPI trend
            if cpi > self.CPI_HIGH:
                inflation_signal = 1  # High inflation
            elif cpi < self.CPI_FALLING:
                inflation_signal = -1  # Low inflation
            else:
                inflation_signal = 0  # Moderate inflation

            # Market indicators
            nifty_trend = macro_data['nse']['market_trend']
            vix = macro_data['nse']['vix']
            nifty_50dma = macro_data['nse']['nifty_50dma']
            nifty_200dma = macro_data['nse']['nifty_200dma']

            # Market signal
            market_bullish = 1 if nifty_trend == 'bullish' else -1
            market_stress = 1 if vix > self.VIX_HIGH else 0

            # Calculate sector rotation (Cyclicals vs Defensives)
            cyclicals = macro_data['nse']['cyclicals_index']
            defensives = macro_data['nse']['defensives_index']
            sector_rotation = (cyclicals - defensives) / defensives  # Positive = cyclicals leading

            return {
                # Growth indicators
                'gdp_growth': gdp_growth,
                'gdp_trend': gdp_trend,
                'gdp_signal': gdp_signal,
                'iip': iip,

                # Inflation indicators
                'cpi': cpi,
                'cpi_trend': cpi_trend,
                'cpi_signal': inflation_signal,
                'core_cpi': macro_data['mospi_inflation']['core_cpi'],

                # Market indicators
                'nifty_trend': nifty_trend,
                'market_signal': market_bullish,
                'vix': vix,
                'market_stress': market_stress,
                'nifty_50dma_vs_200dma': nifty_50dma / nifty_200dma,

                # Sector rotation
                'sector_rotation': sector_rotation,
                'cyclicals_outperforming': sector_rotation > 0,

                # Rates
                'repo_rate': macro_data['rbi']['repo_rate'],
                'gsec_10y': macro_data['rbi']['gsec_10y']
            }

        except Exception as e:
            logger.error(f"❌ Error extracting indicators: {e}")
            # Return default indicators
            return {
                'gdp_growth': 6.5,
                'gdp_trend': 'stable',
                'gdp_signal': 0,
                'iip': 5.0,
                'cpi': 4.8,
                'cpi_trend': 'stable',
                'cpi_signal': 0,
                'core_cpi': 4.3,
                'nifty_trend': 'bullish',
                'market_signal': 1,
                'vix': 13.5,
                'market_stress': 0,
                'nifty_50dma_vs_200dma': 1.06,
                'sector_rotation': 0.0,
                'cyclicals_outperforming': False,
                'repo_rate': 6.5,
                'gsec_10y': 6.8
            }

    def _classify_by_growth_and_inflation(self, indicators: Dict) -> Dict:
        """
        Classify regime based on GDP and CPI directions

        Decision Matrix:
        - GDP ↑, CPI ↓ → Growth-Disinflation (Green)
        - GDP ↑, CPI ↑ → Growth-Inflation (Orange)
        - GDP ↓, CPI ↓ → Stagnation-Disinflation (Blue)
        - GDP ↓, CPI ↑ → Stagflation (Red)
        """

        gdp_signal = indicators['gdp_signal']
        inflation_signal = indicators['cpi_signal']

        # Classification logic
        if gdp_signal >= 0 and inflation_signal < 0:
            return {
                'name': 'Growth-Disinflation',
                'code': 'GROWTH_DISINFLATION',
                'color': 'Green',
                'description': 'Goldilocks scenario - Rising growth, falling inflation, stable rates'
            }

        elif gdp_signal >= 0 and inflation_signal >= 0:
            return {
                'name': 'Growth-Inflation',
                'code': 'GROWTH_INFLATION',
                'color': 'Orange',
                'description': 'Overheating economy - Rising growth, rising inflation, rate hikes expected'
            }

        elif gdp_signal < 0 and inflation_signal < 0:
            return {
                'name': 'Stagnation-Disinflation',
                'code': 'STAGNATION_DISINFLATION',
                'color': 'Blue',
                'description': 'Economic slowdown - Falling growth, falling inflation, rate cuts expected'
            }

        elif gdp_signal < 0 and inflation_signal >= 0:
            return {
                'name': 'Stagflation',
                'code': 'STAGFLATION',
                'color': 'Red',
                'description': 'Worst case - Falling growth, rising inflation, high volatility'
            }

        else:
            # Edge case: stable growth and inflation
            return {
                'name': 'Growth-Disinflation',  # Default to Goldilocks
                'code': 'GROWTH_DISINFLATION',
                'color': 'Green',
                'description': 'Stable growth and inflation - Goldilocks scenario'
            }

    def _calculate_confidence(self, indicators: Dict, regime: Dict) -> float:
        """
        Calculate confidence level for the classification (0-1)

        Higher confidence when:
        - GDP and CPI signals are strong (clearly accelerating/decelerating)
        - Market data aligns with macro indicators
        - No conflicting signals
        """

        confidence_factors = []

        # Factor 1: Strength of GDP signal
        gdp_strength = abs(indicators['gdp_signal'])  # 0, 1, or -1
        confidence_factors.append(gdp_strength)

        # Factor 2: Strength of inflation signal
        cpi_strength = abs(indicators['cpi_signal'])
        confidence_factors.append(cpi_strength)

        # Factor 3: Market alignment
        # If regime is growth-oriented and market is bullish, increase confidence
        if regime['code'] in ['GROWTH_DISINFLATION', 'GROWTH_INFLATION']:
            market_alignment = 1.0 if indicators['market_signal'] > 0 else 0.5
        else:
            market_alignment = 1.0 if indicators['market_signal'] < 0 else 0.5
        confidence_factors.append(market_alignment)

        # Factor 4: Market stress check
        # If VIX is very high (>25), reduce confidence (uncertain environment)
        stress_penalty = max(0, (25 - indicators['vix']) / 25)
        confidence_factors.append(stress_penalty)

        # Factor 5: Sector rotation consistency
        # Cyclicals should outperform in growth regimes
        if regime['code'] in ['GROWTH_DISINFLATION', 'GROWTH_INFLATION']:
            sector_consistency = 1.0 if indicators['cyclicals_outperforming'] else 0.7
        else:
            sector_consistency = 1.0 if not indicators['cyclicals_outperforming'] else 0.7
        confidence_factors.append(sector_consistency)

        # Calculate average confidence
        confidence = np.mean(confidence_factors)

        return round(confidence, 2)

    def _get_probability_distribution(self, indicators: Dict) -> Dict[str, float]:
        """
        Calculate probability distribution across all 4 regimes

        Uses a simple scoring function based on how well the
        indicators match each regime's profile
        """

        scores = {}

        # Growth-Disinflation score
        scores['Growth-Disinflation'] = (
            (1 if indicators['gdp_signal'] >= 0 else 0) * 0.4 +
            (1 if indicators['cpi_signal'] < 0 else 0) * 0.4 +
            (1 if indicators['market_signal'] > 0 else 0) * 0.2
        )

        # Growth-Inflation score
        scores['Growth-Inflation'] = (
            (1 if indicators['gdp_signal'] >= 0 else 0) * 0.4 +
            (1 if indicators['cpi_signal'] >= 0 else 0) * 0.4 +
            (1 if indicators['market_signal'] > 0 else 0) * 0.2
        )

        # Stagnation-Disinflation score
        scores['Stagnation-Disinflation'] = (
            (1 if indicators['gdp_signal'] < 0 else 0) * 0.4 +
            (1 if indicators['cpi_signal'] < 0 else 0) * 0.4 +
            (1 if indicators['market_signal'] < 0 else 0) * 0.2
        )

        # Stagflation score
        scores['Stagflation'] = (
            (1 if indicators['gdp_signal'] < 0 else 0) * 0.4 +
            (1 if indicators['cpi_signal'] >= 0 else 0) * 0.4 +
            (1 if indicators['market_stress'] > 0 else 0) * 0.2
        )

        # Normalize to sum to 1
        total = sum(scores.values())
        if total > 0:
            scores = {k: v/total for k, v in scores.items()}
        else:
            # Equal probabilities if no signal
            scores = {k: 0.25 for k in scores.keys()}

        return scores

    # ========================================================================
    # REGIME CHARACTERISTICS
    # ========================================================================

    def get_regime_info(self, regime_code: str) -> Dict[str, any]:
        """Get detailed information about a regime"""

        regimes = {
            'GROWTH_DISINFLATION': {
                'name': 'Growth-Disinflation',
                'color': 'Green',
                'description': 'Goldilocks - Rising growth, falling inflation, stable rates',
                'characteristics': {
                    'gdp': 'Rising (above 6.5%)',
                    'inflation': 'Falling (below 4.5%)',
                    'rates': 'Stable to falling',
                    'market': 'Bullish (50DMA > 200DMA)',
                    'volatility': 'Low to moderate (VIX < 20)'
                },
                'best_assets': ['Equities', 'Growth stocks', 'Financials', 'Autos', 'Cyclicals'],
                'avoid_assets': ['Bonds', 'Gold', 'Cash'],
                'recommended_allocation': {
                    'equities': '70-80%',
                    'bonds': '10-20%',
                    'cash': '0-10%'
                },
                'multipliers': {
                    'Growth': 1.2,
                    'Value': 0.8,
                    'Defensive': 0.7
                },
                'duration': 'Average 9-12 months',
                'example_period': '2023-2024'
            },

            'GROWTH_INFLATION': {
                'name': 'Growth-Inflation',
                'color': 'Orange',
                'description': 'Overheating - Rising growth, rising inflation, rate hikes',
                'characteristics': {
                    'gdp': 'Rising (above 6.5%)',
                    'inflation': 'Rising (above 5.0%)',
                    'rates': 'Rising',
                    'market': 'Near all-time highs',
                    'volatility': 'Moderate'
                },
                'best_assets': ['Commodities', 'Real Estate', 'Infrastructure', 'Value stocks'],
                'avoid_assets': ['Long-duration bonds', 'Growth stocks'],
                'recommended_allocation': {
                    'equities': '60%',
                    'real_assets': '20%',
                    'commodities': '15%',
                    'cash': '5%'
                },
                'multipliers': {
                    'Growth': 1.1,
                    'Value': 0.9,
                    'Defensive': 0.8
                },
                'duration': 'Average 6-8 months',
                'example_period': '2021-2022'
            },

            'STAGNATION_DISINFLATION': {
                'name': 'Stagnation-Disinflation',
                'color': 'Blue',
                'description': 'Slowdown - Falling growth, falling inflation, rate cuts',
                'characteristics': {
                    'gdp': 'Falling (below 6.0%)',
                    'inflation': 'Falling (below 4.5%)',
                    'rates': 'Falling',
                    'market': 'Range-bound to bearish',
                    'volatility': 'Moderate'
                },
                'best_assets': ['Quality bonds', 'Large-cap defensives', 'Cash'],
                'avoid_assets': ['Smallcaps', 'Cyclicals'],
                'recommended_allocation': {
                    'equities': '40%',
                    'bonds': '40%',
                    'cash': '20%'
                },
                'multipliers': {
                    'Growth': 0.7,
                    'Value': 1.0,
                    'Defensive': 1.2,
                    'Cyclicals': 0.7
                },
                'duration': 'Average 4-6 months',
                'example_period': '2015-2016'
            },

            'STAGFLATION': {
                'name': 'Stagflation',
                'color': 'Red',
                'description': 'Worst case - Falling growth, rising inflation, high volatility',
                'characteristics': {
                    'gdp': 'Falling (below 6.0%)',
                    'inflation': 'Rising (above 6.0%)',
                    'rates': 'Rising or high',
                    'market': 'Bearish with high volatility',
                    'volatility': 'High (VIX > 25)'
                },
                'best_assets': ['Gold', 'Cash', 'Short-duration bonds', 'Defensive stocks'],
                'avoid_assets': ['Equities', 'Long-duration bonds', 'Real Estate'],
                'recommended_allocation': {
                    'equities': '20%',
                    'gold': '30%',
                    'cash': '40%',
                    'short_duration_bonds': '10%'
                },
                'multipliers': {
                    'Growth': 0.7,
                    'Value': 0.8,
                    'Defensive': 1.3,
                    'Cyclicals': 0.7
                },
                'duration': 'Average 10-14 months',
                'example_period': '2008, 2020, 2022'
            }
        }

        return regimes.get(regime_code, {})

    # ========================================================================
    # TRANSITION DETECTION
    # ========================================================================

    def detect_regime_transition(self, current_regime: Dict, previous_regime: Optional[Dict]) -> Dict:
        """
        Detect if regime has changed and assess transition significance

        Returns:
            dict: {
                'transition_detected': bool,
                'previous_regime': str or None,
                'new_regime': str,
                'transition_type': str,
                'significance': str,
                'recommendation': str
            }
        """

        if previous_regime is None:
            return {
                'transition_detected': False,
                'previous_regime': None,
                'new_regime': current_regime['regime'],
                'transition_type': None,
                'significance': 'Initial classification',
                'recommendation': 'Build historical data'
            }

        transition_detected = current_regime['regime'] != previous_regime.get('regime')

        if transition_detected:
            # Determine transition type
            transition_map = {
                'Growth-Disinflation': {
                    'Growth-Inflation': 'Deterioration (overheating)',
                    'Stagnation-Disinflation': 'Deterioration (slowing)',
                    'Stagflation': 'Major Deterioration (crisis)'
                },
                'Growth-Inflation': {
                    'Growth-Disinflation': 'Improvement (disinflation)',
                    'Stagnation-Disinflation': 'Minor Deterioration',
                    'Stagflation': 'Major Deterioration (crisis)'
                },
                'Stagnation-Disinflation': {
                    'Growth-Disinflation': 'Improvement (recovery)',
                    'Growth-Inflation': 'Improvement (reflation)',
                    'Stagflation': 'Deterioration (crisis)'
                },
                'Stagflation': {
                    'Growth-Disinflation': 'Major Improvement (recovery)',
                    'Growth-Inflation': 'Minor Improvement (reflation)',
                    'Stagnation-Disinflation': 'Minor Improvement (stabilization)'
                }
            }

            transition_type = transition_map.get(
                previous_regime.get('regime'),
                {}
            ).get(current_regime['regime'], 'Unknown')

            # Determine significance
            if 'crisis' in transition_type.lower() or 'major' in transition_type.lower():
                significance = 'HIGH'
                recommendation = 'IMMEDIATE ACTION: Rebalance portfolio, reduce risk'
            elif 'deterioration' in transition_type.lower():
                significance = 'MEDIUM'
                recommendation = 'Monitor closely, prepare defensive positioning'
            else:  # Improvement
                significance = 'MEDIUM'
                recommendation = 'Increase risk gradually, monitor sustainability'

            logger.warning(f"⚠️ REGIME CHANGE DETECTED: {previous_regime.get('regime')} → {current_regime['regime']}")
            logger.warning(f"   Transition Type: {transition_type}")
            logger.warning(f"   Significance: {significance}")
            logger.warning(f"   Recommendation: {recommendation}")

        else:
            transition_type = None
            significance = 'No change'
            recommendation = 'Maintain current positioning'

        return {
            'transition_detected': transition_detected,
            'previous_regime': previous_regime.get('regime') if previous_regime else None,
            'new_regime': current_regime['regime'],
            'transition_type': transition_type,
            'significance': significance,
            'recommendation': recommendation,
            'detection_timestamp': datetime.now().isoformat()
        }


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Test the regime classifier with sample data"""

    # Sample macro data (current scenario: March 2026)
    sample_macro_data = {
        'rbi': {
            'repo_rate': 6.50,
            'reverse_repo_rate': 3.35,
            'gsec_10y': 6.80,
            'data_source': 'rbi.org.in'
        },
        'mospi_inflation': {
            'cpi': 4.8,
            'wpi': 2.1,
            'cpi_trend': 'falling',
            'core_cpi': 4.3,
            'data_source': 'mospi.gov.in'
        },
        'mospi_growth': {
            'iip': 5.2,
            'gdp_growth': 6.3,  # Slightly below threshold
            'manufacturing': 5.8,
            'services': 7.5,
            'data_source': 'mospi.gov.in'
        },
        'nse': {
            'nifty_50': 22500.0,
            'nifty_50dma': 22050.0,
            'nifty_200dma': 20700.0,
            'cyclicals_index': 21375.0,
            'defensives_index': 23625.0,
            'vix': 13.5,
            'market_trend': 'bullish',
            'data_source': 'nseindia.com'
        }
    }

    # Create classifier
    classifier = RegimeClassifier()

    # Classify regime
    result = classifier.classify_regime(sample_macro_data)

    # Print results
    print("\n" + "=" * 80)
    print("REGIME CLASSIFICATION RESULT")
    print("=" * 80)
    print(f"Regime: {result['regime']} ({result['color']})")
    print(f"Code: {result['regime_code']}")
    print(f"Confidence: {result['confidence']:.2%}")
    print(f"\nDescription: {result['description']}")
    print(f"\nSignals:")
    print(f"  GDP Growth: {result['signals']['gdp_growth']}% ({result['signals']['gdp_trend']})")
    print(f"  CPI: {result['signals']['cpi']}% ({result['signals']['cpi_trend']})")
    print(f"  Nifty Trend: {result['signals']['nifty_trend']}")
    print(f"  VIX: {result['signals']['vix']}")
    print(f"  Sector Rotation: {result['signals']['sector_rotation']:.2%} ({'Cyclicals' if result['signals']['cyclicals_outperforming'] else 'Defensives'})")

    print(f"\nProbability Distribution:")
    for regime, prob in result['probability_distribution'].items():
        print(f"  {regime}: {prob:.2%}")

    # Get detailed regime info
    regime_info = classifier.get_regime_info(result['regime_code'])
    if regime_info:
        print(f"\nBest Assets: {', '.join(regime_info['best_assets'])}")
        print(f"Avoid: {', '.join(regime_info['avoid_assets'])}")
        print(f"Allocation: {regime_info['recommended_allocation']}")
        print(f"Multipliers: {regime_info['multipliers']}")

    print("=" * 80)


if __name__ == "__main__":
    main()
