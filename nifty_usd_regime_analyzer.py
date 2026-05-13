#!/usr/bin/env python3
"""
Multi-Currency Regime-Equity Performance Analyzer

Analyzes Nifty performance in both INR and USD terms by regime
Shows impact of INR depreciation on offshore investor returns

Author: TresoWealth Analytics Team
Date: March 27, 2026
Version: 3.0 (Multi-Currency)
"""

import sys
sys.path.insert(0, '/Users/akshayrandeva/Treso/treso_analytics')

import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from typing import Dict, List, Tuple
import logging

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MultiCurrencyRegimeAnalyzer:
    """
    Analyzes equity market performance by regime in multiple currencies
    Creates visualizations with currency comparison
    """

    def __init__(self):
        self.regime_colors = {
            'Growth-Disinflation': '#22c55e',      # Green
            'Growth-Inflation': '#f97316',          # Orange
            'Stagnation-Disinflation': '#3b82f6',    # Blue
            'Stagflation': '#ef4444'                 # Red
        }

        self.regime_order = ['Growth-Disinflation', 'Growth-Inflation',
                              'Stagnation-Disinflation', 'Stagflation']

    def load_data(self):
        """Load regime, Nifty, and USDINR data"""
        # Load regimes
        with open('historical_macro_data/regimes_historical.json', 'r') as f:
            regimes_data = json.load(f)

        regimes_df = pd.DataFrame(regimes_data)
        regimes_df['Date'] = pd.to_datetime(regimes_df['Date'])

        # Load Nifty 50 TRI
        with open('historical_macro_data/nifty_50_tri_real.json', 'r') as f:
            nifty_data = json.load(f)

        nifty_df = pd.DataFrame(nifty_data)
        nifty_df['Date'] = pd.to_datetime(nifty_df['Date'])
        # Rename Nifty_50_TRI to Nifty_50 for consistency
        nifty_df.rename(columns={'Nifty_50_TRI': 'Nifty_50'}, inplace=True)

        # Load USDINR
        with open('historical_macro_data/usdinr_historical.json', 'r') as f:
            usdinr_data = json.load(f)

        usdinr_df = pd.DataFrame(usdinr_data)
        usdinr_df['Date'] = pd.to_datetime(usdinr_df['Date'])

        logger.info(f"Loaded {len(regimes_df)} regime observations")
        logger.info(f"Loaded {len(nifty_df)} Nifty 50 TRI observations")
        logger.info(f"Loaded {len(usdinr_df)} USDINR observations")

        return regimes_df, nifty_df, usdinr_df

    def merge_data(self, regimes_df: pd.DataFrame, nifty_df: pd.DataFrame,
                   usdinr_df: pd.DataFrame) -> pd.DataFrame:
        """Merge regime, Nifty, and USDINR data"""
        logger.info("Merging regime, Nifty TRI, and USDINR data...")

        # Convert daily data to monthly (month-end values)
        nifty_df['YearMonth'] = nifty_df['Date'].dt.to_period('M')
        nifty_monthly = nifty_df.groupby('YearMonth').apply(
            lambda x: x.loc[x['Date'].idxmin()]
        ).reset_index(drop=True)

        usdinr_df['YearMonth'] = usdinr_df['Date'].dt.to_period('M')
        usdinr_monthly = usdinr_df.groupby('YearMonth').apply(
            lambda x: x.loc[x['Date'].idxmin()]
        ).reset_index(drop=True)

        # Prepare regime data
        regimes_df['YearMonth'] = pd.to_datetime(regimes_df['Date']).dt.to_period('M')

        # Merge all
        df = pd.merge(
            regimes_df,
            nifty_monthly[['YearMonth', 'Nifty_50']],
            on='YearMonth',
            how='inner'
        )

        df = pd.merge(
            df,
            usdinr_monthly[['YearMonth', 'USDINR']],
            on='YearMonth',
            how='inner'
        )

        df['Date'] = df['YearMonth'].dt.to_timestamp()
        df = df.sort_values('Date')

        # Calculate INR returns
        df['Nifty_INR_Return'] = df['Nifty_50'].pct_change()

        # Calculate USD returns
        # Convert Nifty to USD: Nifty_USD = Nifty_INR / USDINR
        df['Nifty_USD'] = df['Nifty_50'] / df['USDINR']

        # Calculate USD returns
        df['Nifty_USD_Return'] = df['Nifty_USD'].pct_change()

        # Calculate FX return (INR depreciation = positive for USD investor)
        df['FX_Return'] = df['USDINR'].pct_change()

        # Calculate volatility (6-month)
        df['Nifty_INR_Volatility_6M'] = df['Nifty_INR_Return'].rolling(6).std()
        df['Nifty_USD_Volatility_6M'] = df['Nifty_USD_Return'].rolling(6).std()

        # Calculate drawdowns
        df['Nifty_INR_Peak'] = df['Nifty_50'].expanding().max()
        df['Nifty_INR_Drawdown'] = (df['Nifty_50'] - df['Nifty_INR_Peak']) / df['Nifty_INR_Peak']

        df['Nifty_USD_Peak'] = df['Nifty_USD'].expanding().max()
        df['Nifty_USD_Drawdown'] = (df['Nifty_USD'] - df['Nifty_USD_Peak']) / df['Nifty_USD_Peak']

        logger.info(f"Merged dataset: {len(df)} observations from {df['Date'].min()} to {df['Date'].max()}")

        return df

    def calculate_equity_metrics_by_regime(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Calculate equity performance metrics by regime in both INR and USD

        Returns:
            Tuple of (inr_metrics, usd_metrics)
        """
        logger.info("Calculating equity metrics by regime in INR and USD...")

        # Calculate INR metrics
        inr_metrics_list = []
        for regime in self.regime_order:
            regime_data = df[df['Regime'] == regime]['Nifty_INR_Return'].dropna()
            vol_data = df[df['Regime'] == regime]['Nifty_INR_Volatility_6M'].dropna()
            dd_data = df[df['Regime'] == regime]['Nifty_INR_Drawdown'].dropna()

            if len(regime_data) == 0:
                continue

            mean_ret = regime_data.mean()
            std_ret = regime_data.std()
            count = len(regime_data)
            min_ret = regime_data.min()
            max_ret = regime_data.max()
            mean_vol = vol_data.mean() if len(vol_data) > 0 else 0
            min_dd = dd_data.min() if len(dd_data) > 0 else 0
            ann_ret = (1 + mean_ret) ** 12 - 1

            inr_metrics_list.append({
                'Regime': regime,
                'Mean_Return': mean_ret,
                'Std_Return': std_ret,
                'Volatility_6M': mean_vol,
                'Min_Return': min_ret,
                'Max_Return': max_ret,
                'Min_Drawdown': min_dd,
                'Count': count,
                'Ann_Return': ann_ret
            })

        inr_metrics = pd.DataFrame(inr_metrics_list)
        inr_metrics.set_index('Regime', inplace=True)

        # Calculate USD metrics
        usd_metrics_list = []
        for regime in self.regime_order:
            regime_data = df[df['Regime'] == regime]['Nifty_USD_Return'].dropna()
            vol_data = df[df['Regime'] == regime]['Nifty_USD_Volatility_6M'].dropna()
            dd_data = df[df['Regime'] == regime]['Nifty_USD_Drawdown'].dropna()

            if len(regime_data) == 0:
                continue

            mean_ret = regime_data.mean()
            std_ret = regime_data.std()
            count = len(regime_data)
            min_ret = regime_data.min()
            max_ret = regime_data.max()
            mean_vol = vol_data.mean() if len(vol_data) > 0 else 0
            min_dd = dd_data.min() if len(dd_data) > 0 else 0
            ann_ret = (1 + mean_ret) ** 12 - 1

            usd_metrics_list.append({
                'Regime': regime,
                'Mean_Return': mean_ret,
                'Std_Return': std_ret,
                'Volatility_6M': mean_vol,
                'Min_Return': min_ret,
                'Max_Return': max_ret,
                'Min_Drawdown': min_dd,
                'Count': count,
                'Ann_Return': ann_ret
            })

        usd_metrics = pd.DataFrame(usd_metrics_list)
        usd_metrics.set_index('Regime', inplace=True)

        # Print comparison
        logger.info("\n" + "=" * 80)
        logger.info("NIFTY 50 PERFORMANCE: INR vs USD BY REGIME")
        logger.info("=" * 80)
        logger.info(f"{'Regime':25s} {'INR Ann':>10s} {'USD Ann':>10s} {'Diff':>10s} {'FX Impact':>12s}")
        logger.info("-" * 80)

        for regime in self.regime_order:
            if regime in inr_metrics.index and regime in usd_metrics.index:
                inr_ann = inr_metrics.loc[regime, 'Ann_Return']
                usd_ann = usd_metrics.loc[regime, 'Ann_Return']
                diff = usd_ann - inr_ann

                # Calculate average FX return for this regime
                fx_data = df[df['Regime'] == regime]['FX_Return'].dropna()
                fx_ann = (1 + fx_data.mean()) ** 12 - 1 if len(fx_data) > 0 else 0

                logger.info(f"{regime:25s} {inr_ann*100:9.2f}% {usd_ann*100:9.2f}% {diff*100:9.2f}% {fx_ann*100:11.2f}%")

        return inr_metrics, usd_metrics

    def create_multi_currency_dashboard(self, df: pd.DataFrame, inr_metrics: pd.DataFrame,
                                       usd_metrics: pd.DataFrame,
                                       save_path: str = 'nifty_multi_currency_dashboard.png'):
        """
        Create comprehensive dashboard comparing INR vs USD performance

        Shows:
        1. Cumulative returns comparison (INR vs USD)
        2. Return distribution by regime (side-by-side box plots)
        3. Regime-specific metrics table (INR | USD)
        4. Volatility comparison
        5. Drawdown comparison
        """
        logger.info("Creating multi-currency dashboard...")

        fig = plt.figure(figsize=(24, 14))
        gs = fig.add_gridspec(3, 2, hspace=0.35, wspace=0.3)

        # 1. Cumulative returns comparison (top, full width)
        ax1 = fig.add_subplot(gs[0, :])

        # Calculate cumulative returns
        df['Nifty_INR_Cum_Return'] = (1 + df['Nifty_INR_Return']).cumprod()
        df['Nifty_USD_Cum_Return'] = (1 + df['Nifty_USD_Return']).cumprod()

        # Plot regime background
        for i in range(len(df) - 1):
            regime = df.iloc[i]['Regime']
            color = self.regime_colors[regime]
            ax1.axvspan(df.iloc[i]['Date'], df.iloc[i+1]['Date'],
                       alpha=0.2, color=color, zorder=1)

        # Plot cumulative returns
        ax1.plot(df['Date'], df['Nifty_INR_Cum_Return'],
                color='#22c55e', linewidth=2.5, zorder=2, label='Nifty 50 (INR)', alpha=0.8)
        ax1.plot(df['Date'], df['Nifty_USD_Cum_Return'],
                color='#3b82f6', linewidth=2.5, zorder=2, label='Nifty 50 (USD)', alpha=0.8)

        # Format
        ax1.set_title('Nifty 50 Cumulative Returns: INR vs USD (2006-2026)\nImpact of INR Depreciation on Offshore Investors',
                    fontsize=16, fontweight='bold', pad=20, color='white')
        ax1.set_ylabel('Cumulative Return', fontsize=12, fontweight='bold', color='white')
        ax1.grid(True, alpha=0.3, zorder=0, color='gray')
        ax1.legend(loc='upper left', fontsize=11, framealpha=0.95, labelcolor='white')
        ax1.tick_params(axis='x', colors='white', rotation=45)
        ax1.tick_params(axis='y', colors='white')
        ax1.xaxis.set_major_locator(mdates.YearLocator())
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        for spine in ax1.spines.values():
            spine.set_color('gray')

        # 2. Monthly return distribution by regime (middle left - side by side)
        ax2 = fig.add_subplot(gs[1, 0])

        # Prepare data
        inr_returns = []
        usd_returns = []
        labels = []
        colors = []

        for regime in self.regime_order:
            regime_data_inr = df[df['Regime'] == regime]['Nifty_INR_Return'].dropna()
            regime_data_usd = df[df['Regime'] == regime]['Nifty_USD_Return'].dropna()
            if len(regime_data_inr) > 0 and len(regime_data_usd) > 0:
                inr_returns.append(regime_data_inr.values)
                usd_returns.append(regime_data_usd.values)
                labels.append(f'{regime}\n(INR|USD)')
                colors.append(self.regime_colors[regime])

        # Create side-by-side box plots
        positions = []
        for i in range(len(labels)):
            positions.extend([i*2 + 1, i*2 + 1.4])

        all_data = []
        for i in range(len(inr_returns)):
            all_data.extend([inr_returns[i], usd_returns[i]])

        bp = ax2.boxplot(all_data, positions=positions, widths=0.3, patch_artist=True)

        # Color the boxes
        for i, regime in enumerate(self.regime_order):
            if i < len(inr_returns):
                color = self.regime_colors[regime]
                bp['boxes'][i*2].set_facecolor(color)
                bp['boxes'][i*2].set_alpha(0.7)
                bp['boxes'][i*2+1].set_facecolor(color)
                bp['boxes'][i*2+1].set_alpha(0.4)  # Lighter for USD

        ax2.set_title('Monthly Returns Distribution by Regime\n(Green=INR, Lighter=USD)',
                    fontsize=14, fontweight='bold', pad=15, color='white')
        ax2.set_ylabel('Monthly Return', fontsize=11, fontweight='bold', color='white')
        ax2.axhline(y=0, color='white', linestyle='--', linewidth=1, alpha=0.5)
        ax2.grid(True, axis='y', alpha=0.3, color='gray')
        ax2.tick_params(axis='x', colors='white', rotation=45)
        ax2.tick_params(axis='y', colors='white')
        ax2.set_xticks([p + 0.2 for p in positions[::2]])
        ax2.set_xticklabels(labels, fontsize=9, color='white')
        for spine in ax2.spines.values():
            spine.set_color('gray')

        # 3. Comparison table (middle right)
        ax3 = fig.add_subplot(gs[1, 1])
        ax3.axis('off')

        # Create comparison table data
        table_data = []
        for regime in self.regime_order:
            if regime in inr_metrics.index and regime in usd_metrics.index:
                inr_row = inr_metrics.loc[regime]
                usd_row = usd_metrics.loc[regime]

                # Calculate FX impact
                fx_data = df[df['Regime'] == regime]['FX_Return'].dropna()
                fx_ann = (1 + fx_data.mean()) ** 12 - 1 if len(fx_data) > 0 else 0

                table_data.append([
                    regime,
                    f"{inr_row['Ann_Return']*100:.1f}%",
                    f"{usd_row['Ann_Return']*100:.1f}%",
                    f"{(usd_row['Ann_Return'] - inr_row['Ann_Return'])*100:.1f}%",
                    f"{fx_ann*100:.1f}%",
                    f"{int(inr_row['Count'])} mo"
                ])

        table = ax3.table(cellText=table_data,
                         colLabels=['Regime', 'INR Return', 'USD Return', 'FX Impact', 'FX Move', 'Months'],
                         bbox=[0, 0, 1, 1],
                         cellLoc='center', colWidths=[0.22, 0.13, 0.13, 0.13, 0.12, 0.09])

        table.auto_set_font_size(False)
        table.set_fontsize(10)

        # Color regime column
        for i, regime in enumerate(self.regime_order):
            if i < len(table_data):
                cell = table[(i+1, 0)]
                cell.set_facecolor(self.regime_colors[regime])
                cell.set_text_props(weight='bold', color='white')

        # Header row
        for i in range(6):
            cell = table[(0, i)]
            cell.set_facecolor('#404040')
            cell.set_text_props(weight='bold', color='white')

        ax3.set_title('Nifty Performance: INR vs USD by Regime', fontsize=14, fontweight='bold', pad=15, color='white')

        # 4. Volatility comparison (bottom left)
        ax4 = fig.add_subplot(gs[2, 0])

        x = np.arange(len(self.regime_order))
        width = 0.35

        inr_vol = [inr_metrics.loc[r, 'Volatility_6M'] if r in inr_metrics.index else 0
                   for r in self.regime_order]
        usd_vol = [usd_metrics.loc[r, 'Volatility_6M'] if r in usd_metrics.index else 0
                   for r in self.regime_order]

        bars1 = ax4.bar(x - width/2, inr_vol, width, label='INR', alpha=0.8, color='#22c55e')
        bars2 = ax4.bar(x + width/2, usd_vol, width, label='USD', alpha=0.6, color='#3b82f6')

        ax4.set_title('Average 6-Month Volatility by Regime\n(INR vs USD)',
                    fontsize=14, fontweight='bold', pad=15, color='white')
        ax4.set_ylabel('Volatility (Monthly)', fontsize=11, fontweight='bold', color='white')
        ax4.set_xlabel('Regime', fontsize=11, fontweight='bold', color='white')
        ax4.set_xticks(x)
        ax4.set_xticklabels(self.regime_order, rotation=45, ha='right', color='white')
        ax4.tick_params(axis='y', colors='white')
        ax4.legend(labelcolor='white')
        ax4.grid(True, axis='y', alpha=0.3, color='gray')
        for spine in ax4.spines.values():
            spine.set_color('gray')

        # 5. Drawdown comparison (bottom right)
        ax5 = fig.add_subplot(gs[2, 1])

        inr_dd = [inr_metrics.loc[r, 'Min_Drawdown'] * 100 if r in inr_metrics.index else 0
                  for r in self.regime_order]
        usd_dd = [usd_metrics.loc[r, 'Min_Drawdown'] * 100 if r in usd_metrics.index else 0
                  for r in self.regime_order]

        bars1 = ax5.bar(x - width/2, inr_dd, width, label='INR', alpha=0.8, color='#22c55e')
        bars2 = ax5.bar(x + width/2, usd_dd, width, label='USD', alpha=0.6, color='#3b82f6')

        ax5.set_title('Maximum Drawdown by Regime\n(INR vs USD)',
                    fontsize=14, fontweight='bold', pad=15, color='white')
        ax5.set_ylabel('Max Drawdown (%)', fontsize=11, fontweight='bold', color='white')
        ax5.set_xlabel('Regime', fontsize=11, fontweight='bold', color='white')
        ax5.set_xticks(x)
        ax5.set_xticklabels(self.regime_order, rotation=45, ha='right', color='white')
        ax5.tick_params(axis='y', colors='white')
        ax5.legend(labelcolor='white')
        ax5.grid(True, axis='y', alpha=0.3, color='gray')
        ax5.axhline(y=0, color='white', linestyle='--', linewidth=1, alpha=0.5)
        for spine in ax5.spines.values():
            spine.set_color('gray')

        # Add overall title
        fig.suptitle('INDIAN EQUITY MARKET BY MACRO REGIME: INR vs USD\nKey Insights for Domestic vs Offshore Investors',
                    fontsize=18, fontweight='bold', y=0.995, color='white')

        plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='black')
        logger.info(f"Saved multi-currency dashboard to {save_path}")
        plt.close()

        return save_path

    def print_summary(self, df: pd.DataFrame, inr_metrics: pd.DataFrame, usd_metrics: pd.DataFrame):
        """Print comprehensive summary"""
        print("\n" + "=" * 80)
        print("NIFTY 50 PERFORMANCE: INR VS USD BY MACRO REGIME")
        print("=" * 80)

        print("\n🏆 Performance Ranking (INR):")
        print("-" * 80)
        inr_ranked = inr_metrics['Ann_Return'].sort_values(ascending=False)
        for i, (regime, ann_ret) in enumerate(inr_ranked.items(), 1):
            rank_emoji = ['🥇', '🥈', '🥉', '4th'][i-1]
            count = inr_metrics.loc[regime, 'Count']
            print(f"{rank_emoji} {regime:25s}: {ann_ret*100:6.1f}% ({int(count)} months)")

        print("\n🌎 Performance Ranking (USD):")
        print("-" * 80)
        usd_ranked = usd_metrics['Ann_Return'].sort_values(ascending=False)
        for i, (regime, ann_ret) in enumerate(usd_ranked.items(), 1):
            rank_emoji = ['🥇', '🥈', '🥉', '4th'][i-1]
            count = usd_metrics.loc[regime, 'Count']
            inr_ret = inr_metrics.loc[regime, 'Ann_Return']
            diff = ann_ret - inr_ret
            print(f"{rank_emoji} {regime:25s}: {ann_ret*100:6.1f}% (vs {inr_ret*100:5.1f}% INR, {diff*100:+.1f}% fx)")

        print("\n📊 Key Insights:")
        print("-" * 80)

        # Calculate overall period FX move
        fx_start = df['USDINR'].iloc[0]
        fx_end = df['USDINR'].iloc[-1]
        fx_total_move = (fx_end - fx_start) / fx_start

        # Calculate average annual FX depreciation
        months = len(df)
        years = months / 12
        fx_annual_dep = ((fx_end / fx_start) ** (1/years) - 1)

        print(f"• Period: {df['Date'].min().strftime('%Y-%m')} to {df['Date'].max().strftime('%Y-%m')} ({years:.1f} years)")
        print(f"• INR depreciated from {fx_start:.2f} to {fx_end:.2f} vs USD ({fx_total_move*100:.1f}% total)")
        print(f"• Average annual INR depreciation: {fx_annual_dep*100:.2f}%")
        print()

        # Find biggest FX impact
        max_fx_impact = -999
        worst_regime = None
        for regime in self.regime_order:
            if regime in inr_metrics.index and regime in usd_metrics.index:
                inr_ret = inr_metrics.loc[regime, 'Ann_Return']
                usd_ret = usd_metrics.loc[regime, 'Ann_Return']
                fx_impact = usd_ret - inr_ret
                if fx_impact < max_fx_impact:
                    max_fx_impact = fx_impact
                    worst_regime = regime

        if worst_regime:
            print(f"• Worst FX impact: {worst_regime}")
            print(f"  - INR return: {inr_metrics.loc[worst_regime, 'Ann_Return']*100:.1f}%")
            print(f"  - USD return: {usd_metrics.loc[worst_regime, 'Ann_Return']*100:.1f}%")
            print(f"  - FX penalty: {max_fx_impact*100:.1f}% (INR depreciation hurt offshore investors)")

        print("\n💡 Investment Implications:")
        print("-" * 80)
        print("• Domestic Investors (INR):")
        print(f"  - Focus on regime timing - best in {inr_ranked.index[0]}")
        print(f"  - Expected long-term return: ~{inr_metrics['Ann_Return'].mean()*100:.1f}% annually")
        print()
        print("• Offshore Investors (USD):")
        print(f"  - Must account for FX risk - INR depreciation ~{fx_annual_dep*100:.1f}% annually")
        print(f"  - Best regime: {usd_ranked.index[0]}")
        print(f"  - Expected long-term return: ~{usd_metrics['Ann_Return'].mean()*100:.1f}% annually")
        print(f"  - FX hedging can add ~{fx_annual_dep*100:.1f}% to returns")

        print("\n" + "=" * 80)


def main():
    """Generate multi-currency Nifty-regime analysis"""

    print("=" * 80)
    print("MULTI-CURRENCY REGIME-EQUITY PERFORMANCE ANALYZER")
    print("=" * 80)

    # Initialize
    analyzer = MultiCurrencyRegimeAnalyzer()

    # Load and merge data
    regimes_df, nifty_df, usdinr_df = analyzer.load_data()
    df = analyzer.merge_data(regimes_df, nifty_df, usdinr_df)

    # Calculate metrics
    inr_metrics, usd_metrics = analyzer.calculate_equity_metrics_by_regime(df)

    # Print summary
    analyzer.print_summary(df, inr_metrics, usd_metrics)

    # Create dashboard
    analyzer.create_multi_currency_dashboard(df, inr_metrics, usd_metrics,
                                            'nifty_multi_currency_dashboard.png')

    print("\n" + "=" * 80)
    print("✅ MULTI-CURRENCY ANALYSIS COMPLETE")
    print("=" * 80)
    print("\nOutputs:")
    print("  • nifty_multi_currency_dashboard.png - INR vs USD comparison dashboard")
    print("\nKey Insights:")
    print("  • Regime-specific returns in both INR and USD")
    print("  • FX impact by regime")
    print("  • Volatility and drawdown comparison")
    print("  • Investment implications for domestic vs offshore investors")
    print("\n" + "=" * 80)

    return analyzer, df, inr_metrics, usd_metrics


if __name__ == "__main__":
    main()
