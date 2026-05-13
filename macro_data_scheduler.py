#!/usr/bin/env python3
"""
Macro Data Pipeline Scheduler

Automated scheduler to:
1. Fetch macro data from RBI, MOSPI, NSE, FRED, NSDL
2. Classify regime (enhanced: FCI + softmax + z-scores)
3. Update Google Sheets (13 tabs)
4. Detect regime transitions
5. Compute forward regime probabilities (Markov model)
6. Check event alerts (oil, FPI, VIX, yield curve)
7. Generate weekly regime report with charts

Author: TresoWealth Analytics Team
Date: March 26, 2026
Version: 1.0
"""

import subprocess
import logging
import schedule
import time
from datetime import datetime
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/macro_data_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class MacroDataScheduler:
    """
    Scheduler for automated macro data pipeline
    """

    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.log_dir = 'logs'
        self.ensure_log_dir()

    def ensure_log_dir(self):
        """Ensure log directory exists"""
        os.makedirs(self.log_dir, exist_ok=True)

    def fetch_macro_data(self):
        """Step 1: Fetch macro data from all sources (using enhanced scraper)"""
        logger.info("=" * 80)
        logger.info("STEP 1: Fetching Macro Data (Real Scraping)")
        logger.info("=" * 80)

        script_path = os.path.join(self.script_dir, 'macro_data_fetcher_v2.py')

        try:
            # Import and run the enhanced fetcher directly
            sys.path.insert(0, self.script_dir)
            from macro_data_fetcher_v2 import EnhancedMacroDataFetcher

            fetcher = EnhancedMacroDataFetcher(use_browserbase=False)
            macro_data = fetcher.fetch_all_macro_data()

            # Save the data for next steps
            self.latest_macro_data = macro_data

            logger.info("✅ Macro data fetched successfully")
            logger.info(f"   Repo Rate: {macro_data['rbi']['repo_rate']}%")
            logger.info(f"   CPI: {macro_data['mospi_inflation']['cpi']}%")
            logger.info(f"   GDP: {macro_data['mospi_growth']['gdp_growth']}%")
            logger.info(f"   Nifty: {macro_data['nse']['nifty_50']:.2f}")

            return True

        except Exception as e:
            logger.error(f"❌ Error fetching macro data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def classify_regime(self):
        """Step 2: Classify market regime"""
        logger.info("=" * 80)
        logger.info("STEP 2: Classifying Regime")
        logger.info("=" * 80)

        try:
            # Import and run the ENHANCED regime classifier (v2: FCI + softmax + z-scores)
            sys.path.insert(0, self.script_dir)
            from enhanced_regime_classifier import EnhancedRegimeClassifier

            if not hasattr(self, 'latest_macro_data'):
                logger.error("❌ No macro data available. Run fetch_macro_data first.")
                return False

            classifier = EnhancedRegimeClassifier(method='hybrid')
            regime_result = classifier.classify_current_enhanced(self.latest_macro_data)

            # Save the regime result for next steps
            self.latest_regime_result = regime_result

            logger.info("✅ Regime classified successfully")
            logger.info(f"   Regime: {regime_result['regime']}")
            logger.info(f"   Code: {regime_result['regime_code']}")
            logger.info(f"   Confidence: {regime_result['confidence']:.2f}")
            logger.info(f"   Color: {regime_result['color']}")

            return True

        except Exception as e:
            logger.error(f"❌ Error classifying regime: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def update_google_sheets(self):
        """Step 3: Update Google Sheets with macro data and regime"""
        logger.info("=" * 80)
        logger.info("STEP 3: Updating Google Sheets")
        logger.info("=" * 80)

        try:
            # Import the optimized sheets manager
            sys.path.insert(0, self.script_dir)
            from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

            if not hasattr(self, 'latest_macro_data'):
                logger.error("❌ No macro data available. Run fetch_macro_data first.")
                return False

            if not hasattr(self, 'latest_regime_result'):
                logger.error("❌ No regime classification available. Run classify_regime first.")
                return False

            # Initialize the sheets manager (uses service_account.json)
            manager = OptimizedMacroDataSheetsManager()

            # Get spreadsheet ID from environment or auto-detect
            spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')

            if not spreadsheet_id:
                # Auto-detect accessible spreadsheets
                logger.info("No spreadsheet ID in environment, auto-detecting...")
                try:
                    spreadsheets = manager.gc.list_spreadsheet_files()
                    if spreadsheets:
                        spreadsheet_id = spreadsheets[0]['id']
                        logger.info(f"Auto-detected spreadsheet: {spreadsheets[0]['name']}")
                    else:
                        logger.error("❌ No accessible spreadsheets found")
                        logger.error("Please share a spreadsheet with: analytics-bot@treso-analytics.iam.gserviceaccount.com")
                        return False
                except Exception as e:
                    logger.error(f"❌ Could not auto-detect spreadsheet: {e}")
                    return False

            # Update all worksheets (optimized batch version)
            manager.update_all_macro_data(
                spreadsheet_id,
                self.latest_macro_data,
                self.latest_regime_result
            )

            logger.info("✅ Google Sheets updated successfully")
            logger.info(f"   Spreadsheet ID: {spreadsheet_id}")

            return True

        except Exception as e:
            logger.error(f"❌ Error updating Google Sheets: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def check_regime_transition(self):
        """Step 4: Check for regime transitions and send alerts"""
        logger.info("=" * 80)
        logger.info("STEP 4: Checking Regime Transitions")
        logger.info("=" * 80)

        try:
            # Import the optimized sheets manager
            sys.path.insert(0, self.script_dir)
            from macro_data_sheets_v2 import OptimizedMacroDataSheetsManager

            if not hasattr(self, 'latest_regime_result'):
                logger.error("❌ No regime classification available.")
                return False

            # Initialize the sheets manager
            manager = OptimizedMacroDataSheetsManager()

            # Use the spreadsheet ID
            spreadsheet_id = os.getenv('MACRO_SPREADSHEET_ID', '10ZXOkfh7t9MH6XWmZbo16UxchW_EhAoWlYsXGgoHCrU')

            # Check for regime transition
            transition_result = manager.check_regime_transition(
                spreadsheet_id,
                self.latest_regime_result
            )

            if transition_result['transition_detected']:
                logger.warning("⚠️ REGIME TRANSITION DETECTED!")
                logger.warning(f"   Previous: {transition_result['previous_regime']}")
                logger.warning(f"   New: {transition_result['new_regime']}")

                # TODO: Implement alert system
                # - Slack notification
                # - Email notification
                # - SMS for critical transitions

            else:
                logger.info("✅ No regime transition detected")
                logger.info(f"   Current regime: {transition_result['new_regime']}")

            return True

        except Exception as e:
            logger.error(f"❌ Error checking regime transition: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def compute_forward_probabilities(self):
        """Step 5: Compute forward regime distribution using Markov transition model"""
        logger.info("=" * 80)
        logger.info("STEP 5: Computing Forward Regime Probabilities")
        logger.info("=" * 80)

        try:
            sys.path.insert(0, self.script_dir)
            from regime_transition_model import RegimeTransitionModel

            if not hasattr(self, 'latest_regime_result'):
                logger.error("❌ No regime classification available.")
                return False

            model = RegimeTransitionModel()
            hist_file = os.path.join(self.script_dir,
                                     'historical_macro_data', 'regimes_2000_2026_100pct_real.json')
            df = model.load_historical_data(hist_file)
            model.build_transition_matrix(df)
            model.calculate_regime_durations(df)
            model.calculate_stationary_distribution()

            current = self.latest_regime_result['regime']
            forward = {}
            for horizon, months in [('3m', 3), ('6m', 6), ('12m', 12)]:
                dist = model.predict_regime_distribution(current, months)
                forward[horizon] = {k: round(v, 4) for k, v in dist.items()}

            self.forward_probabilities = forward
            logger.info(f"✅ Forward probabilities computed for {current}:")
            for horizon in ['3m', '6m', '12m']:
                top = max(forward[horizon], key=forward[horizon].get)
                logger.info(f"   {horizon}: {top} ({forward[horizon][top]:.2%})")
            return True

        except Exception as e:
            logger.error(f"❌ Error computing forward probabilities: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def check_event_alerts(self):
        """Step 6: Check for oil shocks, FPI reversals, extreme VIX"""
        logger.info("=" * 80)
        logger.info("STEP 6: Checking Event Alerts")
        logger.info("=" * 80)

        alerts = []
        try:
            if not hasattr(self, 'latest_macro_data'):
                logger.info("   No macro data — skipping alerts")
                return True

            md = self.latest_macro_data

            # Oil shock
            brent_change = md.get('oil', {}).get('brent_3m_change_pct', 0)
            brent = md.get('oil', {}).get('brent_usd', 0)
            if abs(brent_change) > 15:
                direction = "SURGE" if brent_change > 0 else "DROP"
                alerts.append(f"OIL_{direction}: Brent ${brent}/bbl, 3M change {brent_change:+.1f}%")

            # FPI equity outflow
            fpi = md.get('fpi_flows', {})
            equity_flow = fpi.get('fpi_equity_flow', 0)
            if equity_flow < -5000:
                alerts.append(f"FPI_EQUITY_OUTFLOW: Daily equity flow Rs {equity_flow:,.0f} Cr")

            # Extreme VIX
            vix = md.get('nse', {}).get('vix', 0)
            if vix > 28:
                alerts.append(f"HIGH_VIX: India VIX = {vix:.1f}")

            # Yield curve slope (compute from RBI data)
            rbi = md.get('rbi', {})
            gsec = rbi.get('gsec_10y', 0)
            tbill = rbi.get('tbill_91d', 0)
            if gsec and tbill:
                slope = gsec - tbill
                if slope < 0.2:
                    alerts.append(f"FLAT_CURVE: 10Y-3M spread = {slope:+.2f}%")

            if alerts:
                for a in alerts:
                    logger.warning(f"   ⚠️ {a}")
            else:
                logger.info("   ✅ No alerts triggered")

            self.event_alerts = alerts
            return True

        except Exception as e:
            logger.error(f"❌ Error in event alerts: {e}")
            return True  # Non-fatal

    def generate_weekly_report(self):
        """Step 5: Generate weekly regime report with charts"""
        logger.info("=" * 80)
        logger.info("STEP 5: Generating Weekly Regime Report")
        logger.info("=" * 80)

        try:
            sys.path.insert(0, self.script_dir)
            from macro_report_generator import MacroReportGenerator

            if not hasattr(self, 'latest_macro_data'):
                logger.error("❌ No macro data. Run fetch_macro_data first.")
                return False
            if not hasattr(self, 'latest_regime_result'):
                logger.error("❌ No regime classification. Run classify_regime first.")
                return False

            generator = MacroReportGenerator()
            report_date = datetime.now().strftime('%Y-%m-%d')
            report_path = generator.generate_report(
                self.latest_macro_data,
                self.latest_regime_result,
                report_date
            )

            logger.info(f"✅ Weekly report generated: {report_path}")
            logger.info(f"   Charts: 6 (regime probs, signals, FCI, timeline, returns INR, returns USD)")
            return True

        except Exception as e:
            logger.error(f"❌ Error generating weekly report: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def run_full_pipeline(self):
        """Run the complete macro data pipeline"""
        logger.info("=" * 80)
        logger.info("MACRO DATA PIPELINE - STARTING")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 80)

        success = True

        # Step 1: Fetch macro data
        if not self.fetch_macro_data():
            logger.error("❌ Pipeline failed at Step 1: Fetch macro data")
            success = False

        # Step 2: Classify regime
        if success and not self.classify_regime():
            logger.error("❌ Pipeline failed at Step 2: Classify regime")
            success = False

        # Step 3: Update Google Sheets
        if success and not self.update_google_sheets():
            logger.error("❌ Pipeline failed at Step 3: Update Google Sheets")
            success = False

        # Step 4: Check regime transitions
        if success and not self.check_regime_transition():
            logger.error("❌ Pipeline failed at Step 4: Check regime transitions")
            success = False

        # Step 5: Compute forward regime probabilities (Markov transition model)
        if success and not self.compute_forward_probabilities():
            logger.error("❌ Pipeline failed at Step 5: Forward probabilities")
            success = False

        # Step 6: Check event alerts (oil, FPI, VIX, yield curve)
        if success and not self.check_event_alerts():
            logger.error("❌ Pipeline failed at Step 6: Event alerts")
            success = False

        # Step 7: Generate weekly regime report with charts
        if success and not self.generate_weekly_report():
            logger.error("❌ Pipeline failed at Step 7: Generate report")
            success = False

        # Final status
        logger.info("=" * 80)
        if success:
            logger.info("✅ MACRO DATA PIPELINE - ALL STEPS COMPLETE")
        else:
            logger.error("❌ MACRO DATA PIPELINE - FAILED")
        logger.info("=" * 80)

        return success

    def schedule_weekly(self):
        """Schedule pipeline to run weekly (every Monday at 9 AM IST)"""
        logger.info("Scheduling macro data pipeline for weekly execution (Mondays 9 AM IST)")

        # Schedule for every Monday at 9 AM
        schedule.every().monday.at("09:00").do(self.run_full_pipeline)

        logger.info("Scheduler started. Waiting for next scheduled run...")
        logger.info("Next run: Next Monday at 9:00 AM IST")

        # Keep the scheduler running
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("\nScheduler stopped by user")

    def schedule_daily(self):
        """Schedule pipeline to run daily (for market data only)"""
        logger.info("Scheduling market data fetch for daily execution (6 PM IST)")

        schedule.every().day.at("18:00").do(self.fetch_macro_data)

        logger.info("Daily scheduler started. Waiting for next scheduled run...")

        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("\nScheduler stopped by user")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Test the scheduler"""

    # Create scheduler
    scheduler = MacroDataScheduler()

    # Ask user what they want to do
    print("\n" + "=" * 80)
    print("MACRO DATA PIPELINE SCHEDULER")
    print("=" * 80)
    print("\nOptions:")
    print("1. Run pipeline once (test)")
    print("2. Start weekly scheduler (Mondays 9 AM IST)")
    print("3. Start daily scheduler (6 PM IST - market data only)")
    print("4. Exit")

    while True:
        choice = input("\nEnter choice (1-4): ").strip()

        if choice == '1':
            print("\nRunning pipeline once...")
            scheduler.run_full_pipeline()
            break

        elif choice == '2':
            print("\nStarting weekly scheduler (Mondays 9 AM IST)...")
            print("Press Ctrl+C to stop")
            scheduler.schedule_weekly()

        elif choice == '3':
            print("\nStarting daily scheduler (6 PM IST)...")
            print("Press Ctrl+C to stop")
            scheduler.schedule_daily()

        elif choice == '4':
            print("\nExiting...")
            break

        else:
            print("\nInvalid choice. Please enter 1-4.")


if __name__ == "__main__":
    main()
