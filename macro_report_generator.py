#!/usr/bin/env python3
"""
Macro Regime Report Generator

Generates a weekly India Macro Regime Report with charts:
  Page 1 — Current Regime Snapshot
    1. Regime probability bars
    2. Signal dashboard (output gap, CPI z, FCI, yield curve, oil)
    3. FCI component decomposition
  Page 2 — Historical Context
    4. Regime timeline ribbon (2000–present)
    5. Equity returns by regime (INR)
    6. Equity returns by regime (USD, FX-adjusted)

Author: TresoWealth Analytics Team
Date: May 8, 2026
Version: 1.0
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import logging
from datetime import datetime
from typing import Dict, Optional
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import FancyBboxPatch
import matplotlib.colors as mcolors

# Regime colors (consistent with classifier)
REGIME_COLORS = {
    'Growth-Disinflation': '#2ECC40',       # Green
    'Growth-Inflation': '#FF851B',           # Orange
    'Stagnation-Disinflation': '#0074D9',    # Blue
    'Stagflation': '#FF4136',                # Red
}
REGIME_ORDER = ['Growth-Disinflation', 'Growth-Inflation',
                'Stagnation-Disinflation', 'Stagflation']

plt.style.use('dark_background')


class MacroReportGenerator:
    """Generate weekly India Macro Regime Report with charts"""

    def __init__(self, output_dir: str = None):
        self.pipeline_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = output_dir or os.path.join(self.pipeline_dir, 'reports')
        self.hist_dir = os.path.join(self.pipeline_dir, 'historical_macro_data')
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Report output dir: {self.output_dir}")

    # ========================================================================
    # CHART 1: Regime Probability Bars
    # ========================================================================

    def _chart_regime_probabilities(self, probs: Dict[str, float],
                                     regime_name: str, confidence: float,
                                     save_path: str):
        """Horizontal bar chart of regime probabilities"""
        fig, ax = plt.subplots(figsize=(8, 3.5))

        regimes = REGIME_ORDER
        values = [probs.get(r, 0) * 100 for r in regimes]
        colors = [REGIME_COLORS[r] for r in regimes]

        bars = ax.barh(regimes, values, color=colors, height=0.55, edgecolor='white', linewidth=0.5)
        ax.set_xlim(0, 100)
        ax.set_xlabel('Probability (%)', fontsize=10, color='#aaa')
        ax.set_title(f'Current Regime: {regime_name}  |  Confidence: {confidence:.0%}',
                     fontsize=13, fontweight='bold', color='white', pad=12)

        # Value labels
        for bar, val in zip(bars, values):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                    f'{val:.1f}%', va='center', fontsize=10, color='white', fontweight='bold')

        # Highlight current regime
        for i, r in enumerate(regimes):
            if r == regime_name:
                bars[i].set_edgecolor('white')
                bars[i].set_linewidth(2.5)

        ax.tick_params(colors='#aaa', labelsize=9)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#444')
        ax.spines['bottom'].set_color('#444')

        plt.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)
        logger.info(f"  Chart 1 saved: {save_path}")

    # ========================================================================
    # CHART 2: Signal Dashboard
    # ========================================================================

    def _chart_signal_dashboard(self, signals: Dict, save_path: str):
        """Compact signal dashboard: output gap, CPI z, FCI, yield curve, oil"""
        fig, axes = plt.subplots(1, 5, figsize=(14, 3.2))
        fig.suptitle('Signal Dashboard', fontsize=13, fontweight='bold',
                     color='white', y=1.02)

        metrics = [
            ('Output Gap', signals.get('output_gap', 0), '%',
             ['#FF4136', '#FF851B', '#2ECC40', '#FF851B', '#FF4136'],
             [-3, -0.5, 0.5, 3]),
            ('CPI Z-Score', signals.get('cpi_signal', 0), 'σ',
             ['#2ECC40', '#FF851B', '#FF4136', '#FF4136', '#FF4136'],
             [-2, -0.5, 0.5, 2]),
            ('FCI', signals.get('fci_signal', 0), '',
             ['#2ECC40', '#FF851B', '#FF4136', '#FF4136', '#FF4136'],
             [-1, -0.3, 0.3, 1]),
            ('Yield Curve', signals.get('yield_curve_slope', 0), '%',
             ['#FF4136', '#FF851B', '#2ECC40', '#2ECC40', '#2ECC40'],
             [-1.0, 0.0, 0.5, 2.0]),
            ('Oil (Brent)', signals.get('oil_brent', 75), '$',
             ['#2ECC40', '#FF851B', '#FF4136', '#FF4136', '#FF4136'],
             [50, 70, 85, 120]),
        ]

        for ax, (label, value, unit, colors, thresholds) in zip(axes, metrics):
            # Determine color based on thresholds
            if value <= thresholds[0]:
                color = colors[0]
            elif value <= thresholds[1]:
                color = colors[1]
            elif value <= thresholds[2]:
                color = colors[2]
            elif value <= thresholds[3]:
                color = colors[3]
            else:
                color = colors[4]

            # Draw gauge-like indicator
            ax.bar(0, value, width=0.5, color=color, edgecolor='white', linewidth=1.2)
            ax.axhline(y=0, color='#555', linewidth=1)
            if len(thresholds) >= 4:
                ax.axhline(y=thresholds[1], color='#555', linewidth=0.5, linestyle='--')
                ax.axhline(y=thresholds[2], color='#555', linewidth=0.5, linestyle='--')

            ax.set_ylim(min(thresholds) - 0.5, max(thresholds) + 0.5)
            ax.set_title(label, fontsize=10, color='#aaa', pad=8)
            ax.set_xticks([])
            ax.tick_params(colors='#aaa', labelsize=8)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_visible(False)
            ax.spines['left'].set_color('#444')

            # Value annotation
            val_str = f'{value:+.1f}{unit}' if value != 0 else f'{value:.1f}{unit}'
            ax.text(0, value, f' {val_str}', va='center', fontsize=11,
                    color='white', fontweight='bold',
                    transform=ax.get_yaxis_transform())

        plt.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)
        logger.info(f"  Chart 2 saved: {save_path}")

    # ========================================================================
    # CHART 3: FCI Decomposition
    # ========================================================================

    def _chart_fci_decomposition(self, fci_components: Dict, fci_signal: float,
                                  save_path: str):
        """Waterfall/bar chart of FCI components"""
        fig, ax = plt.subplots(figsize=(8, 3.5))

        labels = list(fci_components.keys())
        # Friendly labels
        label_map = {
            'repo_spread': 'Repo Spread',
            'yield_curve_slope': 'Yield Curve\n(10Y-3M)',
            'yield_change': 'Yield Change',
            'vix_raw': 'VIX',
            'credit_spread': 'Credit Spread',
        }
        display_labels = [label_map.get(l, l) for l in labels]
        values = list(fci_components.values())

        colors = ['#2ECC40' if v <= 0 else '#FF4136' for v in values]
        bars = ax.bar(range(len(labels)), values, color=colors, width=0.6,
                      edgecolor='white', linewidth=0.5)

        ax.axhline(y=0, color='white', linewidth=1)
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(display_labels, fontsize=8, color='#aaa')
        ax.set_ylabel('Normalized Contribution', fontsize=10, color='#aaa')
        ax.set_title(f'FCI Decomposition  |  Composite: {fci_signal:+.3f}  '
                     f'({"TIGHT" if fci_signal > 0 else "LOOSE"})',
                     fontsize=13, fontweight='bold', color='white', pad=12)

        for bar, val in zip(bars, values):
            y_pos = bar.get_height() + 0.02 if val >= 0 else bar.get_height() - 0.08
            ax.text(bar.get_x() + bar.get_width() / 2, y_pos,
                    f'{val:+.2f}', ha='center', fontsize=9,
                    color='white', fontweight='bold')

        ax.tick_params(colors='#aaa', labelsize=8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#444')
        ax.spines['bottom'].set_color('#444')

        plt.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)
        logger.info(f"  Chart 3 saved: {save_path}")

    # ========================================================================
    # CHART 4: Regime Timeline Ribbon
    # ========================================================================

    def _chart_regime_timeline(self, save_path: str):
        """Historical regime timeline ribbon (2000–present)"""
        regimes_file = os.path.join(self.hist_dir, 'regimes_historical.json')
        nse_file = os.path.join(self.hist_dir, 'nse_historical.json')

        if not os.path.exists(regimes_file):
            logger.warning("No regimes_historical.json, skipping timeline")
            return

        with open(regimes_file) as f:
            regime_data = json.load(f)

        df = pd.DataFrame(regime_data)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')

        # Map regimes to numeric for plotting
        regime_map = {r: i for i, r in enumerate(REGIME_ORDER)}
        df['regime_num'] = df['Regime'].map(regime_map)

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 5.5),
                                        gridspec_kw={'height_ratios': [2.5, 1]},
                                        sharex=True)
        fig.suptitle('India Macro Regime History (2000–Present)', fontsize=14,
                     fontweight='bold', color='white', y=0.98)

        # Top panel: regime ribbon
        colors_list = [REGIME_COLORS.get(r, '#888') for r in df['Regime']]
        ax1.scatter(df['Date'], [1] * len(df), c=colors_list, s=60,
                    marker='s', edgecolors='none', alpha=0.9)
        ax1.set_ylim(0.5, 1.5)
        ax1.set_yticks([])
        ax1.set_ylabel('Regime', fontsize=9, color='#aaa')
        ax1.spines['top'].set_visible(False)
        ax1.spines['right'].set_visible(False)
        ax1.spines['left'].set_visible(False)
        ax1.spines['bottom'].set_color('#444')
        ax1.tick_params(colors='#aaa', labelsize=8)

        # Legend
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor=REGIME_COLORS[r], label=r) for r in REGIME_ORDER]
        ax1.legend(handles=legend_elements, loc='upper right', fontsize=8,
                   ncol=4, framealpha=0.3, facecolor='#333', edgecolor='#555')

        # Bottom panel: Nifty overlay
        if os.path.exists(nse_file):
            with open(nse_file) as f:
                nse_data = json.load(f)
            nse_df = pd.DataFrame(nse_data)
            nse_df['Date'] = pd.to_datetime(nse_df['Date'])
            nse_df = nse_df.sort_values('Date')
            ax2.fill_between(nse_df['Date'], nse_df['Nifty_50'], alpha=0.3, color='#0074D9')
            ax2.plot(nse_df['Date'], nse_df['Nifty_50'], color='#0074D9', linewidth=1.2)
            ax2.set_ylabel('Nifty 50', fontsize=9, color='#aaa')
            ax2.set_yscale('log')
            ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
                lambda x, _: f'{x:,.0f}'))

        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.spines['left'].set_color('#444')
        ax2.spines['bottom'].set_color('#444')
        ax2.tick_params(colors='#aaa', labelsize=8)

        plt.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)
        logger.info(f"  Chart 4 saved: {save_path}")

    # ========================================================================
    # CHART 5 & 6: Equity Returns by Regime (INR & USD)
    # ========================================================================

    def _chart_regime_returns(self, save_path_inr: str, save_path_usd: str):
        """Nifty returns by regime — INR and USD-adjusted"""
        regimes_file = os.path.join(self.hist_dir, 'regimes_historical.json')
        nse_file = os.path.join(self.hist_dir, 'nse_historical.json')
        usdinr_file = os.path.join(self.hist_dir, 'usdinr_historical.json')

        if not all(os.path.exists(f) for f in [regimes_file, nse_file, usdinr_file]):
            logger.warning("Missing historical files for regime returns chart")
            return

        with open(regimes_file) as f:
            regime_data = json.load(f)
        with open(nse_file) as f:
            nse_data = json.load(f)
        with open(usdinr_file) as f:
            usdinr_data = json.load(f)

        # Build monthly returns by regime
        rdf = pd.DataFrame(regime_data)
        rdf['Date'] = pd.to_datetime(rdf['Date'])

        ndf = pd.DataFrame(nse_data)
        ndf['Date'] = pd.to_datetime(ndf['Date'])
        ndf = ndf.sort_values('Date')
        ndf['Nifty_Return'] = ndf['Nifty_50'].pct_change()

        udf = pd.DataFrame(usdinr_data)
        udf['Date'] = pd.to_datetime(udf['Date'])
        udf = udf.sort_values('Date')
        udf['USDINR_Return'] = udf['USDINR'].pct_change()

        # Merge on month
        ndf['Month'] = ndf['Date'].dt.to_period('M')
        udf['Month'] = udf['Date'].dt.to_period('M')
        rdf['Month'] = rdf['Date'].dt.to_period('M')

        monthly_nifty = ndf.groupby('Month')['Nifty_Return'].apply(
            lambda x: (1 + x).prod() - 1).reset_index()
        monthly_fx = udf.groupby('Month')['USDINR_Return'].apply(
            lambda x: (1 + x).prod() - 1).reset_index()

        merged = rdf.merge(monthly_nifty, on='Month', how='inner')
        merged = merged.merge(monthly_fx, on='Month', how='inner')

        # Nifty return in USD = (1 + INR_return) / (1 + FX_return) - 1
        merged['Nifty_USD'] = (1 + merged['Nifty_Return']) / (1 + merged['USDINR_Return']) - 1

        # Annualize by regime
        regime_stats = []
        for regime in REGIME_ORDER:
            sub = merged[merged['Regime'] == regime]
            if len(sub) == 0:
                continue
            n_months = len(sub)
            avg_monthly_inr = sub['Nifty_Return'].mean()
            avg_monthly_usd = sub['Nifty_USD'].mean()
            ann_inr = (1 + avg_monthly_inr) ** 12 - 1
            ann_usd = (1 + avg_monthly_usd) ** 12 - 1
            regime_stats.append({
                'regime': regime,
                'ann_return_inr': ann_inr * 100,
                'ann_return_usd': ann_usd * 100,
                'months': n_months,
                'color': REGIME_COLORS[regime],
            })

        # --- INR Chart ---
        fig, ax = plt.subplots(figsize=(8, 4))
        regimes = [s['regime'] for s in regime_stats]
        inr_vals = [s['ann_return_inr'] for s in regime_stats]
        colors = [s['color'] for s in regime_stats]
        bars = ax.bar(regimes, inr_vals, color=colors, width=0.55, edgecolor='white', linewidth=0.8)
        ax.axhline(y=0, color='white', linewidth=1)
        ax.set_ylabel('Annualized Return (%)', fontsize=10, color='#aaa')
        ax.set_title('Nifty 50 Returns by Regime (INR)', fontsize=13,
                     fontweight='bold', color='white', pad=12)
        for bar, val in zip(bars, inr_vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + (1 if val >= 0 else -3),
                    f'{val:+.1f}%', ha='center', fontsize=10, color='white', fontweight='bold')
        ax.tick_params(colors='#aaa', labelsize=9)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#444')
        ax.spines['bottom'].set_color('#444')
        plt.tight_layout()
        fig.savefig(save_path_inr, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)

        # --- USD Chart ---
        fig, ax = plt.subplots(figsize=(8, 4))
        usd_vals = [s['ann_return_usd'] for s in regime_stats]
        bars = ax.bar(regimes, usd_vals, color=colors, width=0.55, edgecolor='white', linewidth=0.8)
        ax.axhline(y=0, color='white', linewidth=1)
        ax.set_ylabel('Annualized Return (%)', fontsize=10, color='#aaa')
        ax.set_title('Nifty 50 Returns by Regime (USD, FX-Adjusted)', fontsize=13,
                     fontweight='bold', color='white', pad=12)
        for bar, val in zip(bars, usd_vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + (1 if val >= 0 else -3),
                    f'{val:+.1f}%', ha='center', fontsize=10, color='white', fontweight='bold')
        ax.tick_params(colors='#aaa', labelsize=9)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#444')
        ax.spines['bottom'].set_color('#444')
        plt.tight_layout()
        fig.savefig(save_path_usd, dpi=150, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)

        logger.info(f"  Chart 5 saved: {save_path_inr}")
        logger.info(f"  Chart 6 saved: {save_path_usd}")

        return regime_stats

    # ========================================================================
    # REPORT ASSEMBLY
    # ========================================================================

    def generate_report(self, macro_data: Dict, regime_result: Dict,
                        report_date: str = None) -> str:
        """
        Generate the complete weekly regime report with charts.

        Args:
            macro_data: From EnhancedMacroDataFetcher.fetch_all_macro_data()
            regime_result: From EnhancedRegimeClassifier.classify_current_enhanced()

        Returns:
            Path to the generated markdown report
        """
        if report_date is None:
            report_date = datetime.now().strftime('%Y-%m-%d')

        report_dir = os.path.join(self.output_dir, report_date)
        os.makedirs(report_dir, exist_ok=True)

        logger.info(f"Generating regime report for {report_date}...")
        logger.info(f"  Output: {report_dir}")

        signals = regime_result.get('signals', {})
        probs = regime_result.get('probability_distribution', {})
        fci_comp = signals.get('fci_components', {})
        fci = signals.get('fci_signal', 0)

        # Add oil to signals for dashboard
        if 'oil' in macro_data:
            signals['oil_brent'] = macro_data['oil']['brent_usd']
            signals['oil_3m_change'] = macro_data['oil']['brent_3m_change_pct']

        # --- Generate Charts ---
        chart_paths = {}

        chart_paths['probabilities'] = os.path.join(report_dir, 'chart1_regime_probs.png')
        self._chart_regime_probabilities(
            probs, regime_result.get('regime', 'Unknown'),
            regime_result.get('confidence', 0.5), chart_paths['probabilities'])

        chart_paths['signals'] = os.path.join(report_dir, 'chart2_signals.png')
        self._chart_signal_dashboard(signals, chart_paths['signals'])

        chart_paths['fci'] = os.path.join(report_dir, 'chart3_fci.png')
        self._chart_fci_decomposition(fci_comp, fci, chart_paths['fci'])

        chart_paths['timeline'] = os.path.join(report_dir, 'chart4_timeline.png')
        self._chart_regime_timeline(chart_paths['timeline'])

        chart_paths['returns_inr'] = os.path.join(report_dir, 'chart5_returns_inr.png')
        chart_paths['returns_usd'] = os.path.join(report_dir, 'chart6_returns_usd.png')
        regime_returns = self._chart_regime_returns(
            chart_paths['returns_inr'], chart_paths['returns_usd'])

        # --- Write Markdown Report ---
        report_path = os.path.join(report_dir, f'macro_regime_report_{report_date}.md')
        report_md = self._build_markdown(macro_data, regime_result, signals,
                                          probs, chart_paths, regime_returns, report_date)
        with open(report_path, 'w') as f:
            f.write(report_md)

        # Also write latest symlink
        latest_link = os.path.join(self.output_dir, 'LATEST_REPORT.md')
        with open(latest_link, 'w') as f:
            f.write(report_md)

        logger.info(f"Report saved: {report_path}")
        logger.info(f"Latest symlink: {latest_link}")
        return report_path

    def _build_markdown(self, macro_data, regime_result, signals, probs,
                        chart_paths, regime_returns, report_date):
        """Assemble the markdown report"""

        regime = regime_result.get('regime', 'Unknown')
        confidence = regime_result.get('confidence', 0.5)
        color = regime_result.get('color', 'Gray')
        fci = signals.get('fci_signal', 0)
        fci_stance = 'TIGHT' if fci > 0.1 else ('LOOSE' if fci < -0.1 else 'NEUTRAL')

        gdp = signals.get('gdp_growth', 'N/A')
        output_gap = signals.get('output_gap', 0)
        cpi = signals.get('cpi', 'N/A')
        yield_slope = signals.get('yield_curve_slope', 0)
        vix = signals.get('vix', 'N/A')
        repo = signals.get('repo_rate', 'N/A')
        gsec = signals.get('gsec_10y', 'N/A')

        oil_brent = macro_data.get('oil', {}).get('brent_usd', 'N/A')
        oil_change = macro_data.get('oil', {}).get('brent_3m_change_pct', 'N/A')

        md = f"""# India Macro Regime Report — {report_date}

## Regime Summary

| | |
|---|---|
| **Current Regime** | {regime} |
| **Color** | {color} |
| **Confidence** | {confidence:.0%} |
| **FCI Stance** | {fci_stance} ({fci:+.3f}) |

### Key Signals

| Signal | Value | Interpretation |
|--------|-------|----------------|
| GDP Growth | {gdp}% | Output gap: {output_gap:+.1f}% vs 10yr trend |
| CPI Inflation | {cpi}% | {signals.get('cpi_trend', 'stable').title()} |
| Yield Curve (10Y-3M) | {yield_slope:+.1f}% | {'Normal' if yield_slope > 0.3 else ('Flat' if yield_slope > -0.1 else 'Inverted')} |
| India VIX | {vix} | {'Elevated' if signals.get('vix', 0) > 25 else 'Normal'} |
| Repo Rate | {repo}% | RBI Policy Rate |
| GSEC 10Y | {gsec}% | Benchmark Yield |
| Oil (Brent) | ${oil_brent} | {oil_change:+.1f}% 3-month change |

---

## Page 1: Current Regime Snapshot

### Chart 1: Regime Probabilities

![Regime Probabilities](chart1_regime_probs.png)

### Chart 2: Signal Dashboard

![Signal Dashboard](chart2_signals.png)

### Chart 3: FCI Decomposition

![FCI Decomposition](chart3_fci.png)

---

## Page 2: Historical Context

### Chart 4: Regime Timeline (2000–Present)

![Regime Timeline](chart4_timeline.png)

### Chart 5: Nifty 50 Returns by Regime (INR)

![Returns INR](chart5_returns_inr.png)

### Chart 6: Nifty 50 Returns by Regime (USD, FX-Adjusted)

![Returns USD](chart6_returns_usd.png)

"""
        # Regime return stats table
        if regime_returns:
            md += """### Historical Returns by Regime

| Regime | Ann. Return (INR) | Ann. Return (USD) | Months Observed |
|--------|-------------------|-------------------|-----------------|
"""
            for s in regime_returns:
                md += (f"| {s['regime']} | {s['ann_return_inr']:+.1f}% "
                       f"| {s['ann_return_usd']:+.1f}% | {s['months']} |\n")

        md += f"""---
### Regime Probabilities (Full)

| Regime | Probability |
|--------|-------------|
"""
        for r in REGIME_ORDER:
            prob = probs.get(r, 0)
            bar = '█' * int(prob * 30)
            md += f"| {r} | {prob:.1%} {bar} |\n"

        md += f"""---
*Generated by TresoWealth Macro Regime Engine v2.1 (Tier 1)*
*Timestamp: {regime_result.get('classification_timestamp', 'N/A')}*
*Method: {regime_result.get('method', 'N/A')}*
"""
        return md

    # ========================================================================
    # PIPELINE INTEGRATION
    # ========================================================================

    # ========================================================================
    # MONTHLY DEEP-DIVE REPORT
    # ========================================================================

    def generate_monthly_report(self, macro_data: Dict, regime_result: Dict,
                                  report_month: str = None) -> str:
        """
        Generate a monthly deep-dive report with:
          - Regime evolution narrative (trailing 12 months)
          - Oil shock analysis
          - Policy stance history
          - Foreign investor positioning matrix
        """
        if report_month is None:
            report_month = datetime.now().strftime('%Y-%m')

        report_dir = os.path.join(self.output_dir, f'monthly_{report_month}')
        os.makedirs(report_dir, exist_ok=True)

        logger.info(f"Generating MONTHLY deep-dive report for {report_month}...")

        signals = regime_result.get('signals', {})
        probs = regime_result.get('probability_distribution', {})
        regime_risk = regime_result.get('regime_risk', {})
        leading = regime_result.get('leading_indicator', {})
        tier2 = regime_result.get('tier2_upgrades', {})
        tier3 = regime_result.get('tier3_upgrades', {})

        # --- Generate monthly-specific charts ---
        chart_paths = {}
        chart_paths['regime_evolution'] = os.path.join(report_dir, 'monthly_regime_evolution.png')
        self._chart_monthly_regime_evolution(chart_paths['regime_evolution'])

        chart_paths['policy_stance'] = os.path.join(report_dir, 'monthly_policy_stance.png')
        self._chart_policy_stance_history(chart_paths['policy_stance'])

        chart_paths['oil_impact'] = os.path.join(report_dir, 'monthly_oil_impact.png')
        self._chart_oil_impact_decomposition(chart_paths['oil_impact'])

        # --- Reuse weekly charts ---
        chart_paths['probabilities'] = os.path.join(report_dir, 'chart1_regime_probs.png')
        self._chart_regime_probabilities(
            probs, regime_result.get('regime', 'Unknown'),
            regime_result.get('confidence', 0.5), chart_paths['probabilities'])

        chart_paths['timeline'] = os.path.join(report_dir, 'chart4_timeline.png')
        self._chart_regime_timeline(chart_paths['timeline'])

        chart_paths['returns_inr'] = os.path.join(report_dir, 'chart5_returns_inr.png')
        chart_paths['returns_usd'] = os.path.join(report_dir, 'chart6_returns_usd.png')
        regime_returns = self._chart_regime_returns(
            chart_paths['returns_inr'], chart_paths['returns_usd'])

        # --- Build report ---
        report_path = os.path.join(report_dir, f'monthly_macro_deep_dive_{report_month}.md')
        md = self._build_monthly_markdown(macro_data, regime_result, signals, probs,
                                           chart_paths, regime_returns, regime_risk,
                                           leading, tier2, tier3, report_month)
        with open(report_path, 'w') as f:
            f.write(md)

        latest_link = os.path.join(self.output_dir, 'LATEST_MONTHLY_REPORT.md')
        with open(latest_link, 'w') as f:
            f.write(md)

        logger.info(f"Monthly report saved: {report_path}")
        return report_path

    # -------------------------------------------------------------------
    # Monthly Charts
    # -------------------------------------------------------------------

    def _chart_monthly_regime_evolution(self, save_path: str):
        """12-month regime probability evolution (area chart)."""
        import pandas as pd
        data_file = os.path.join(self.hist_dir, 'macro_data_2000_2026_100pct_real.json')
        if not os.path.exists(data_file):
            logger.warning("Historical data not found for regime evolution chart")
            return
        with open(data_file) as f:
            hist = json.load(f)

        gdp_records = hist.get('gdp', [])
        cpi_records = hist.get('cpi', [])
        if not gdp_records or not cpi_records:
            logger.warning("No GDP/CPI records for regime evolution")
            return

        # Build monthly GDP and CPI series from last 12 months
        gdp_df = pd.DataFrame(gdp_records)
        cpi_df = pd.DataFrame(cpi_records)

        # Last 12 data points
        recent_gdp = gdp_df.tail(12)
        recent_cpi = cpi_df.tail(12)

        months = [str(r.get('Date', '')) for _, r in recent_gdp.iterrows()]
        timeline_labels = [m[-7:] if len(m) > 7 else m for m in months]

        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        # GDP trend
        ax = axes[0]
        gdp_vals = recent_gdp['GDP_Growth'].values.astype(float) if 'GDP_Growth' in recent_gdp.columns else []
        if len(gdp_vals) > 0:
            ax.fill_between(range(len(gdp_vals)), gdp_vals, alpha=0.3, color='#2ECC40')
            ax.plot(gdp_vals, color='#2ECC40', linewidth=2, marker='o')
            ax.axhline(y=7.0, color='white', linestyle='--', alpha=0.5, label='7% trend')
            ax.set_ylabel('GDP Growth %', color='#2ECC40', fontsize=11)
            ax.legend(loc='upper right', fontsize=9)
            ax.grid(alpha=0.2)

        # CPI trend
        ax2 = axes[1]
        cpi_vals = recent_cpi['CPI_YoY'].values.astype(float) if 'CPI_YoY' in recent_cpi.columns else []
        if len(cpi_vals) > 0:
            ax2.fill_between(range(len(cpi_vals)), cpi_vals, alpha=0.3, color='#FF851B')
            ax2.plot(cpi_vals, color='#FF851B', linewidth=2, marker='o')
            ax2.axhline(y=4.0, color='white', linestyle='--', alpha=0.5, label='4% RBI target')
            ax2.set_ylabel('CPI YoY %', color='#FF851B', fontsize=11)
            ax2.legend(loc='upper right', fontsize=9)
            ax2.grid(alpha=0.2)

        if timeline_labels:
            ax2.set_xticks(range(len(timeline_labels)))
            ax2.set_xticklabels(timeline_labels, rotation=45, ha='right', fontsize=8)

        fig.suptitle('Macro Trend Evolution (Trailing 12 Months)', fontsize=14, fontweight='bold', y=0.98)
        plt.tight_layout()
        fig.savefig(save_path, dpi=120, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)
        logger.info(f"  Regime evolution chart: {save_path}")

    def _chart_policy_stance_history(self, save_path: str):
        """RBI repo rate + GSEC 10Y history (trailing 24 months)."""
        import pandas as pd
        policy_file = os.path.join(self.hist_dir, 'policy_rates_markets_v2.csv')
        if not os.path.exists(policy_file):
            logger.warning("Policy rates file not found")
            return
        df = pd.read_csv(policy_file)
        if 'Date' not in df.columns:
            logger.warning("No Date column in policy rates")
            return

        recent = df.tail(24)
        dates = pd.to_datetime(recent['Date'])
        labels = [d.strftime('%b %y') for d in dates]

        fig, ax1 = plt.subplots(figsize=(12, 5))

        repo = recent.get('Repo_Rate', pd.Series([0]*len(recent))).values.astype(float)
        gsec = recent.get('GSEC_10Y', pd.Series([0]*len(recent))).values.astype(float)
        spread = gsec - repo if len(repo) > 0 and len(gsec) > 0 else []

        color_repo = '#0074D9'
        ax1.step(range(len(repo)), repo, where='mid', color=color_repo, linewidth=2.5, label='Repo Rate')
        ax1.plot(gsec, color='#FF851B', linewidth=2, marker='s', markersize=5, label='GSEC 10Y')
        ax1.set_ylabel('Rate (%)', fontsize=11)
        ax1.legend(loc='upper left', fontsize=9)
        ax1.grid(alpha=0.2)

        if len(spread) > 0:
            ax2 = ax1.twinx()
            ax2.fill_between(range(len(spread)), spread, alpha=0.15, color='white', label='Term Spread')
            ax2.plot(spread, color='white', linewidth=1, linestyle=':', alpha=0.7)
            ax2.set_ylabel('Spread (bps)', fontsize=10)

        ax1.set_xticks(range(len(labels)))
        ax1.set_xticklabels(labels, rotation=45, ha='right', fontsize=8)

        plt.title('RBI Policy Stance: Repo Rate & GSEC 10Y (24 Months)', fontsize=13, fontweight='bold')
        plt.tight_layout()
        fig.savefig(save_path, dpi=120, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)
        logger.info(f"  Policy stance chart: {save_path}")

    def _chart_oil_impact_decomposition(self, save_path: str):
        """Oil price vs Nifty overlay (trailing 36 months)."""
        import pandas as pd
        oil_file = os.path.join(self.hist_dir, 'oil_historical.json')
        nse_file = os.path.join(self.hist_dir, 'nse_historical.json')
        if not os.path.exists(oil_file):
            logger.warning("Oil historical not found")
            return

        with open(oil_file) as f:
            oil_data = json.load(f)

        if isinstance(oil_data, list):
            oil_records = oil_data
        else:
            oil_records = oil_data.get('records', [])
        if not oil_records:
            logger.warning("No oil records")
            return

        df = pd.DataFrame(oil_records).tail(36)
        dates = [str(r.get('Date', r.get('date', ''))) for _, r in df.iterrows()]
        brent_col = 'Brent_USD' if 'Brent_USD' in df.columns else 'brent'
        brent = df.get(brent_col, pd.Series([0]*len(df))).values.astype(float)

        fig, ax1 = plt.subplots(figsize=(12, 5))
        ax1.fill_between(range(len(brent)), brent, alpha=0.25, color='#FF4136')
        ax1.plot(brent, color='#FF4136', linewidth=2, marker='o', markersize=4)
        ax1.set_ylabel('Brent Crude ($/bbl)', color='#FF4136', fontsize=11)
        ax1.axhline(y=80, color='white', linestyle='--', alpha=0.3, label='$80 threshold')
        ax1.legend(loc='upper left', fontsize=9)
        ax1.grid(alpha=0.2)

        # Overlay Nifty if available
        if os.path.exists(nse_file):
            with open(nse_file) as f:
                nse_data = json.load(f)
            nse_records = nse_data if isinstance(nse_data, list) else nse_data.get('records', [])
            if nse_records:
                nse_df = pd.DataFrame(nse_records).tail(36)
                nse_col = 'Nifty_50' if 'Nifty_50' in nse_df.columns else 'nifty'
                nse_vals = nse_df.get(nse_col, pd.Series([0]*len(nse_df))).values.astype(float)
                if len(nse_vals) == len(brent):
                    ax2 = ax1.twinx()
                    ax2.plot(nse_vals, color='#2ECC40', linewidth=1.5, linestyle='--', alpha=0.6, label='Nifty 50')
                    ax2.set_ylabel('Nifty 50', color='#2ECC40', fontsize=10)

        labels = [d[-7:] if len(d) > 7 else d for d in dates]
        ax1.set_xticks(range(0, len(labels), max(1, len(labels)//12)))
        ax1.set_xticklabels([labels[i] for i in range(0, len(labels), max(1, len(labels)//12))],
                            rotation=45, ha='right', fontsize=8)

        plt.title('Oil Price Impact: Brent Crude vs Nifty 50 (36 Months)', fontsize=13, fontweight='bold')
        plt.tight_layout()
        fig.savefig(save_path, dpi=120, bbox_inches='tight', facecolor='#1a1a2e')
        plt.close(fig)
        logger.info(f"  Oil impact chart: {save_path}")

    # -------------------------------------------------------------------
    # Monthly Markdown Builder
    # -------------------------------------------------------------------

    def _build_monthly_markdown(self, macro_data, regime_result, signals, probs,
                                  chart_paths, regime_returns, regime_risk,
                                  leading, tier2, tier3, report_month):
        """Assemble the monthly deep-dive markdown report."""

        regime = regime_result.get('regime', 'Unknown')
        confidence = regime_result.get('confidence', 0.5)
        color = regime_result.get('color', 'Gray')
        fci = signals.get('fci_signal', 0)
        fci_stance = 'TIGHT' if fci > 0.1 else ('LOOSE' if fci < -0.1 else 'NEUTRAL')
        timestamp = regime_result.get('classification_timestamp', 'N/A')

        gdp = signals.get('gdp_growth', 'N/A')
        output_gap = signals.get('output_gap', 0)
        cpi = signals.get('cpi', 'N/A')
        yield_slope = signals.get('yield_curve_slope', 0)
        vix = signals.get('vix', 'N/A')
        repo = signals.get('repo_rate', 'N/A')
        gsec = signals.get('gsec_10y', 'N/A')
        oil_brent = macro_data.get('oil', {}).get('brent_usd', 'N/A')
        oil_change = macro_data.get('oil', {}).get('brent_3m_change_pct', 'N/A')
        usdinr = macro_data.get('fx', {}).get('usdinr', 'N/A')
        fpi = macro_data.get('fpi', {}).get('equity_net_mn', signals.get('fpi_equity_flows', 'N/A'))

        # --- Executive Summary ---
        md = f"""# India Macro Regime Monthly Deep-Dive — {report_month}

> **TresoWealth Macro Strategy | For Foreign Investors | {timestamp}**

---

## Executive Summary

**Current Regime: {regime} ({color}) — Confidence: {confidence:.0%}**

"""
        # Regime narrative
        if regime == 'Growth-Disinflation':
            md += ("India is in a **Goldilocks** regime: above-trend growth with well-behaved inflation. "
                   f"GDP at {gdp}% with output gap near zero ({output_gap:+.1f}pp vs 10yr trend). "
                   f"CPI at {cpi}% remains within RBI's tolerance band. "
                   "This is the most favourable backdrop for Indian equities historically.\n\n")
        elif regime == 'Growth-Inflation':
            md += ("India is in a **Overheating** regime: growth is strong but inflation pressures are building. "
                   f"CPI at {cpi}% is above RBI's comfort zone. "
                   "Equity returns remain positive but with higher volatility. "
                   "Commodity producers and inflation-pass-through sectors tend to outperform.\n\n")
        elif regime == 'Stagnation-Disinflation':
            md += ("India is in a **Slowdown** regime: growth is below trend with contained inflation. "
                   "This typically favours duration (bonds) over equities. "
                   "Defensive sectors (consumer staples, pharma, IT services) historically outperform. "
                   "RBI has room to cut rates if growth weakness persists.\n\n")
        elif regime == 'Stagflation':
            md += ("India is in a **Stagflation** regime: the most challenging macro backdrop — "
                   "weak growth with persistent inflation. "
                   "Equity returns are historically negative in this regime. "
                   "Gold, commodities, and USD exposure provide the best hedges. "
                   "Defensive positioning is warranted.\n\n")

        # --- Section 1: Regime Evolution ---
        md += f"""---

## 1. Regime Evolution (Trailing 12 Months)

![Regime Evolution](monthly_regime_evolution.png)

### Current Signal Dashboard

| Signal | Value | Z-Score | Interpretation |
|--------|-------|---------|----------------|
| GDP Growth | {gdp}% | {signals.get('gdp_zscore_mean', 'N/A')} | Output gap: {output_gap:+.2f}pp vs trend |
| CPI Inflation | {cpi}% | {signals.get('cpi_zscore_mean', 'N/A')} | {'Above target' if isinstance(cpi, (int, float)) and cpi > 4 else 'Near/within target'} |
| FCI (Financial Conditions) | {fci:+.3f} | {signals.get('fci_signal', 'N/A')} | Stance: **{fci_stance}** |
| Yield Curve (10Y-3M) | {yield_slope:+.1f}% | — | {'Normal' if yield_slope > 0.3 else ('Flat' if yield_slope > -0.1 else 'Inverted')} |
| India VIX | {vix} | — | {'Elevated' if isinstance(vix, (int, float)) and vix > 25 else 'Moderate' if isinstance(vix, (int, float)) and vix > 15 else 'Low'} |
| Oil (Brent) | ${oil_brent} | {signals.get('oil_z', 'N/A')} | {oil_change:+.1f}% 3-month change |
| USDINR | {usdinr} | — | FX impact on USD returns |
| FPI Equity Flows | {fpi} | — | Foreign portfolio flows ($mn) |

### Leading Indicator Composite: {tier2.get('leading_indicator_composite', 'N/A')}

"""
        if leading.get('components'):
            md += "| Component | Value | Signal |\n|-----------|-------|--------|\n"
            for comp, val in leading.get('components', {}).items():
                signal_dir = 'Expansionary' if val > 0 else 'Contractionary'
                md += f"| {comp} | {val:+.3f} | {signal_dir} |\n"

        if tier2.get('leading_indicator_missing'):
            md += f"\n*Missing inputs: {', '.join(tier2['leading_indicator_missing'])}*\n"

        # --- Section 2: Oil Shock Analysis ---
        md += f"""---

## 2. Oil Shock Analysis

Oil is India's most important external vulnerability — every $10/bbl move impacts:
- **Fiscal deficit** by ~0.3% of GDP (subsidy + excise offset)
- **Current account deficit** by ~$15bn (~0.5% of GDP)
- **CPI inflation** by ~30-50bps with a 2-3 month lag

### Current Assessment

| Metric | Value | Assessment |
|--------|-------|-------------|
| Brent Crude | ${oil_brent} | {'Above $80 — stress zone' if isinstance(oil_brent, (int, float)) and oil_brent > 80 else 'Below $80 — manageable'} |
| 3-Month Change | {oil_change:+.1f}% | {'Rising sharply — watch CAD' if isinstance(oil_change, (int, float)) and oil_change > 10 else ('Declining — tailwind' if isinstance(oil_change, (int, float)) and oil_change < -5 else 'Stable')} |
| Oil Z-Score | {signals.get('oil_z', 'N/A')} | {'>2σ — extreme shock' if isinstance(signals.get('oil_z'), (int, float)) and abs(signals['oil_z']) > 2 else 'Within normal range'} |
| FCI Oil Component | {signals.get('fci_components', {}).get('oil', 'N/A')} | Oil contribution to FCI |

![Oil Impact](monthly_oil_impact.png)

### Historical Oil Shock Episodes

| Period | Oil Move | India Impact | Regime Shift |
|--------|----------|--------------|--------------|
| 2008 H1 | $50→$147 | CPI +400bps, CAD to -4.2% | → Stagflation |
| 2014 H2 | $115→$50 | CPI -300bps, CAD improvement | → Growth-Disinflation |
| 2020 Q1 | $60→$20 | Temporary disinflation | Brief stag → Growth-Disinflation |
| 2022 H1 | $80→$130 | CPI +250bps, INR -10% | → Growth-Inflation |

"""

        # --- Section 3: Policy Stance ---
        md += f"""---

## 3. RBI Policy Stance & Financial Conditions

![Policy Stance](monthly_policy_stance.png)

### Current Settings

| Instrument | Current | Last Change | Direction |
|------------|---------|-------------|-----------|
| Repo Rate | {repo}% | {'February 2026: -25bps' if isinstance(repo, (int, float)) and repo < 6.5 else 'Hold'} | {'Easing' if isinstance(repo, (int, float)) and repo < 6.5 else 'On Hold'} |
| GSEC 10Y | {gsec}% | Market-determined | — |
| Term Spread | {yield_slope:+.1f}% | — | {'Accommodative' if yield_slope > 0.5 else 'Neutral' if yield_slope > 0.2 else 'Restrictive'} |
| FCI (6-component) | {fci:+.3f} | — | **{fci_stance}** |

### FCI Decomposition

| Component | Weight | Current Value | Contribution |
|-----------|--------|---------------|--------------|
"""
        fci_comp = signals.get('fci_components', {})
        comp_weights = {
            'repo_spread': 0.20, 'yield_curve_slope': 0.20, 'yield_change': 0.15,
            'vix_raw': 0.15, 'credit_spread': 0.15, 'oil': 0.15,
        }
        total = sum(abs(fci_comp.get(k, 0)) * w for k, w in comp_weights.items())
        for comp_name, w in comp_weights.items():
            v = fci_comp.get(comp_name, 0)
            c = v * w
            if isinstance(v, (int, float)):
                md += f"| {comp_name} | {w:.0%} | {v:+.3f} | {c:+.4f} |\n"

        # --- Section 4: Positioning Matrix ---
        md += f"""---

## 4. Foreign Investor Positioning Matrix

### Regime-Conditional Asset Allocation

| Regime | Equity | Bonds | Gold | Cash | USD Hedge | Historical Hit Rate |
|--------|--------|-------|------|------|-----------|---------------------|
"""
        for r in REGIME_ORDER:
            risk = regime_risk.get(r, {})
            eq_weight = risk.get('equity_allocation', 'N/A')
            if isinstance(eq_weight, str):
                eq_str = eq_weight
            elif isinstance(eq_weight, (int, float)):
                eq_str = f"{eq_weight:.0%}"
            else:
                eq_str = 'N/A'

            bond_weight = 1.0 - float(eq_weight) if isinstance(eq_weight, (int, float)) else 0.5
            gold_weight = 0.10
            cash_weight = max(0, 1.0 - float(eq_weight) - bond_weight - gold_weight) if isinstance(eq_weight, (int, float)) else 0.1

            hit_rate = risk.get('hit_rate', 'N/A')
            n_months = risk.get('months', 0)
            if isinstance(n_months, (int, float)) and n_months > 0:
                hit_rate_str = f"{risk.get('positive_months', 0)/n_months:.0%} ({n_months}m)"
            else:
                hit_rate_str = str(hit_rate)

            md += (f"| **{r}** | {eq_str} | {bond_weight:.0%} | {gold_weight:.0%} | "
                   f"{cash_weight:.0%} | {'Yes' if r == 'Stagflation' else 'Partial' if r == 'Growth-Inflation' else 'None'} "
                   f"| {hit_rate_str} |\n")

        md += f"""
### Current Positioning Recommendation: **{regime}**

"""
        # Regime risk table
        if regime_risk:
            curr_risk = regime_risk.get(regime, {})
            md += f"""| Risk Metric | INR | USD (FX-Adjusted) |
|-------------|-----|-------------------|
| Annualized Return | {curr_risk.get('ann_return_inr', 'N/A')} | {curr_risk.get('ann_return_usd', 'N/A')} |
| Volatility | {curr_risk.get('volatility_inr', 'N/A')} | {curr_risk.get('volatility_usd', 'N/A')} |
| VaR 95% (monthly) | {curr_risk.get('var_95_inr', 'N/A')} | {curr_risk.get('var_95_usd', 'N/A')} |
| CVaR 95% (monthly) | {curr_risk.get('cvar_95_inr', 'N/A')} | {curr_risk.get('cvar_95_usd', 'N/A')} |
| Sharpe Ratio | {curr_risk.get('sharpe_inr', 'N/A')} | {curr_risk.get('sharpe_usd', 'N/A')} |
| Max Drawdown | {curr_risk.get('max_drawdown_inr', 'N/A')} | {curr_risk.get('max_drawdown_usd', 'N/A')} |
| Positive Months | {curr_risk.get('positive_months', 'N/A')}/{curr_risk.get('months', 'N/A')} | — |

"""

        # --- Section 5: Charts ---
        md += f"""---

## 5. Regime Probability Distribution

![Regime Probabilities](chart1_regime_probs.png)

### Full Ensemble Probabilities (softmax × 0.50 + markov × 0.30 + rule-based × 0.20)

| Regime | Softmax | Markov | Ensemble |
|--------|---------|--------|----------|
"""
        softmax_probs = regime_result.get('softmax_probabilities', {})
        markov_probs = regime_result.get('markov_probabilities', {})
        for r in REGIME_ORDER:
            s = softmax_probs.get(r, 0)
            m = markov_probs.get(r, 0)
            e = probs.get(r, 0)
            md += f"| {r} | {s:.1%} | {m:.1%} | {e:.1%} |\n"

        md += f"""
---

## 6. Historical Context

![Regime Timeline](chart4_timeline.png)

![Returns INR](chart5_returns_inr.png)

![Returns USD](chart6_returns_usd.png)

### Historical Returns by Regime

| Regime | Ann. Return (INR) | Ann. Return (USD) | Months Observed |
|--------|-------------------|-------------------|-----------------|
"""
        if regime_returns:
            for s in regime_returns:
                md += (f"| {s['regime']} | {s['ann_return_inr']:+.1f}% "
                       f"| {s['ann_return_usd']:+.1f}% | {s['months']} |\n")

        # --- Section 7: Risk Scenarios ---
        md += f"""---

## 7. Forward-Looking Risk Scenarios

### Scenario 1: Oil Sustains Above $100 (30-day probability: ~{15 if isinstance(oil_brent, (int, float)) and oil_brent < 85 else 35}%)
- CAD widens to ~2.0% of GDP
- INR depreciates 3-5% vs USD
- Likely regime shift: Growth-Inflation or Stagflation
- **Action**: Increase USD hedge, reduce equity exposure, add energy sector

### Scenario 2: RBI Cuts 50bps on Growth Concerns (30-day probability: ~{25 if isinstance(cpi, (int, float)) and cpi < 5 else 10}%)
- GSEC yields fall, bond prices rally
- Financials and rate-sensitives outperform
- Likely regime: Growth-Disinflation (reinforced)
- **Action**: Extend duration, add financials and real estate

### Scenario 3: Global Risk-Off (VIX > 30) (30-day probability: ~15%)
- FPI outflows accelerate ($2-5bn/week)
- INR under pressure regardless of domestic fundamentals
- Correlation across assets rises
- **Action**: Increase cash, reduce all risk positions, buy VIX hedges

---

## 8. Methodology & Engine Status

| Tier | Feature | Status |
|------|---------|--------|
| Tier 1 | Base classification (softmax) | ✅ Active |
| Tier 1 | Persistence prior, output gap, FCI z-score | ✅ Active |
| Tier 2 | Markov regime switching (2-state Hamilton) | {'✅ Active' if tier2.get('markov_switching') else '❌ Inactive'} |
| Tier 2 | Time-varying FCI weights (stress multipliers) | {'✅ Active' if tier2.get('time_varying_fci') else '❌ Inactive'} |
| Tier 2 | Leading indicators composite | {'✅ Active' if tier2.get('leading_indicator') else '❌ Inactive'} |
| Tier 3 | Ensemble classifier (3-way blend) | {'✅ Active' if tier3.get('full_ensemble') else '❌ Inactive'} |
| Tier 3 | Regime-conditional VaR/CVaR | {'✅ Active' if tier3.get('regime_var_cvar') else '❌ Inactive'} |
| Pipeline | FPI flows, credit growth, PMI composite | ⏳ Pending Pipeline Agent |

**Ensemble Weights**: {tier3.get('ensemble_weights', tier2.get('ensemble_method', 'N/A'))}
**Missing Indicators**: {', '.join(tier2.get('leading_indicator_missing', [])) if tier2.get('leading_indicator_missing') else 'None'}

---
*Generated by TresoWealth Macro Regime Engine v2.1 (Tiers 1-3)*
*Timestamp: {timestamp}*
*This report is for informational purposes only and does not constitute investment advice.*
"""
        return md

    def generate_monthly_from_pipeline(self) -> Optional[str]:
        """Run the full fetch → classify → monthly report pipeline."""
        from macro_data_fetcher_v2 import EnhancedMacroDataFetcher
        from enhanced_regime_classifier import EnhancedRegimeClassifier

        logger.info("=" * 60)
        logger.info("AUTO-GENERATING MONTHLY MACRO DEEP-DIVE REPORT")
        logger.info("=" * 60)

        fetcher = EnhancedMacroDataFetcher(use_browserbase=False)
        macro_data = fetcher.fetch_all_macro_data()

        classifier = EnhancedRegimeClassifier(method='hybrid')
        regime_result = classifier.classify_current_enhanced(macro_data)

        report_month = datetime.now().strftime('%Y-%m')
        report_path = self.generate_monthly_report(macro_data, regime_result, report_month)

        return report_path


    def generate_from_pipeline(self) -> Optional[str]:
        """Run the full fetch → classify → report pipeline"""
        from macro_data_fetcher_v2 import EnhancedMacroDataFetcher
        from enhanced_regime_classifier import EnhancedRegimeClassifier

        logger.info("=" * 60)
        logger.info("AUTO-GENERATING WEEKLY MACRO REGIME REPORT")
        logger.info("=" * 60)

        # Fetch
        fetcher = EnhancedMacroDataFetcher(use_browserbase=False)
        macro_data = fetcher.fetch_all_macro_data()

        # Classify
        classifier = EnhancedRegimeClassifier(method='hybrid')
        regime_result = classifier.classify_current_enhanced(macro_data)

        # Generate report
        report_date = datetime.now().strftime('%Y-%m-%d')
        report_path = self.generate_report(macro_data, regime_result, report_date)

        return report_path


# ============================================================================
# MAIN
# ============================================================================

def main():
    generator = MacroReportGenerator()
    report_path = generator.generate_from_pipeline()
    if report_path:
        print(f"\nReport generated: {report_path}")
    else:
        print("\nReport generation failed.")


if __name__ == "__main__":
    main()
