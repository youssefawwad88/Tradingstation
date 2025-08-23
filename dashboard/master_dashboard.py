#!/usr/bin/env python3
"""Master Dashboard for trade plans, R-multiples, and signal aggregation.
Provides comprehensive view of all trading opportunities and risk management.
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard.scoring import SignalScorer
from dashboard.trade_plans import TradePlanGenerator
from utils.config import config
from utils.logging_setup import get_logger
from utils.spaces_io import SpacesIO


class MasterDashboard:
    """Master dashboard for aggregating signals and generating trade plans."""

    def __init__(self):
        self.config = config
        self.spaces = SpacesIO()
        self.logger = get_logger("master_dashboard")
        self.trade_planner = TradePlanGenerator()
        self.scorer = SignalScorer()

    def load_all_signals(self) -> pd.DataFrame:
        """Load and combine signals from all screeners."""
        all_signals = []

        # List of all screener signal files
        signal_files = [
            "signals/gapgo.csv",
            "signals/orb.csv",
            "signals/avwap_reclaim.csv",
            "signals/breakout.csv",
            "signals/ema_pullback.csv",
            "signals/exhaustion_reversal.csv"
        ]

        for signal_file in signal_files:
            try:
                df = self.spaces.download_csv(signal_file)
                if df is not None and not df.empty:
                    # Add source screener
                    df['screener'] = signal_file.split('/')[1].replace('.csv', '')
                    all_signals.append(df)
                    self.logger.info(f"Loaded {len(df)} signals from {signal_file}")
                else:
                    self.logger.warning(f"No signals found in {signal_file}")

            except Exception as e:
                self.logger.warning(f"Could not load {signal_file}: {e}")

        if all_signals:
            combined_df = pd.concat(all_signals, ignore_index=True)
            self.logger.info(f"Combined total: {len(combined_df)} signals")
            return combined_df
        else:
            self.logger.warning("No signals loaded from any screener")
            return pd.DataFrame()

    def filter_active_signals(self, signals_df: pd.DataFrame, hours_lookback: int = 24) -> pd.DataFrame:
        """Filter signals to only include recent active ones."""
        if signals_df.empty:
            return signals_df

        # Convert timestamp to datetime if it's not already
        if 'timestamp_utc' in signals_df.columns:
            signals_df['timestamp_utc'] = pd.to_datetime(signals_df['timestamp_utc'])

            # Filter to recent signals
            cutoff_time = datetime.now(timezone.utc) - pd.Timedelta(hours=hours_lookback)
            active_signals = signals_df[signals_df['timestamp_utc'] >= cutoff_time].copy()

            self.logger.info(f"Filtered to {len(active_signals)} active signals (last {hours_lookback}h)")
            return active_signals
        else:
            self.logger.warning("No timestamp_utc column found, returning all signals")
            return signals_df

    def generate_dashboard_report(self, signals_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive dashboard report."""
        if signals_df.empty:
            return {
                "summary": {"total_signals": 0, "screeners_active": 0},
                "top_opportunities": [],
                "risk_summary": {},
                "generated_at": datetime.now(timezone.utc).isoformat()
            }

        # Generate trade plans
        trade_plans = self.trade_planner.generate_plans(signals_df)

        # Score and rank opportunities
        scored_signals = self.scorer.score_signals(signals_df)

        # Generate summary statistics
        summary = {
            "total_signals": len(signals_df),
            "screeners_active": signals_df['screener'].nunique(),
            "long_signals": len(signals_df[signals_df['direction'] == 'long']),
            "short_signals": len(signals_df[signals_df['direction'] == 'short']),
            "avg_score": scored_signals['composite_score'].mean() if not scored_signals.empty else 0,
            "unique_symbols": signals_df['symbol'].nunique()
        }

        # Top opportunities (highest scored)
        top_opportunities = []
        if not scored_signals.empty:
            top_signals = scored_signals.nlargest(10, 'composite_score')
            for _, signal in top_signals.iterrows():
                top_opportunities.append({
                    "symbol": signal['symbol'],
                    "screener": signal['screener'],
                    "setup_name": signal.get('setup_name', 'Unknown'),
                    "direction": signal['direction'],
                    "score": signal.get('score', 0),
                    "composite_score": signal['composite_score'],
                    "entry": signal.get('entry', 0),
                    "stop": signal.get('stop', 0),
                    "r_multiple": signal.get('r_multiple', 0),
                    "timestamp": signal['timestamp_utc'].isoformat() if 'timestamp_utc' in signal else None
                })

        # Risk summary
        risk_summary = self.trade_planner.calculate_portfolio_risk(trade_plans)

        return {
            "summary": summary,
            "top_opportunities": top_opportunities,
            "risk_summary": risk_summary,
            "trade_plans": trade_plans[:20],  # Top 20 trade plans
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

    def save_dashboard(self, dashboard_data: Dict[str, Any]) -> bool:
        """Save dashboard data to Spaces."""
        try:
            # Convert to DataFrame for CSV storage
            df = pd.DataFrame([dashboard_data])

            # Save as JSON-like CSV for structured data
            success = self.spaces.upload_dataframe(df, "dashboard/master_dashboard.csv")

            if success:
                self.logger.info("Dashboard saved successfully")
                return True
            else:
                self.logger.error("Failed to save dashboard")
                return False

        except Exception as e:
            self.logger.error(f"Error saving dashboard: {e}")
            return False

    def run(self, hours_lookback: int = 24) -> Dict[str, Any]:
        """Run the master dashboard generation."""
        self.logger.info("Starting master dashboard generation")

        try:
            # Load all signals
            signals_df = self.load_all_signals()

            # Filter to active signals
            active_signals = self.filter_active_signals(signals_df, hours_lookback)

            # Generate dashboard report
            dashboard_data = self.generate_dashboard_report(active_signals)

            # Save dashboard
            self.save_dashboard(dashboard_data)

            # Print summary to console
            self.print_dashboard_summary(dashboard_data)

            return dashboard_data

        except Exception as e:
            self.logger.error(f"Dashboard generation failed: {e}")
            raise

    def print_dashboard_summary(self, dashboard_data: Dict[str, Any]):
        """Print a formatted summary to console."""
        summary = dashboard_data['summary']

        print("\n" + "="*60)
        print("TRADING SYSTEM MASTER DASHBOARD")
        print("="*60)
        print(f"Generated: {dashboard_data['generated_at']}")
        print(f"Total Signals: {summary['total_signals']}")
        print(f"Active Screeners: {summary['screeners_active']}")
        print(f"Long/Short: {summary['long_signals']}/{summary['short_signals']}")
        print(f"Unique Symbols: {summary['unique_symbols']}")
        print(f"Average Score: {summary['avg_score']:.2f}")

        print("\nTOP OPPORTUNITIES:")
        print("-" * 60)
        for i, opp in enumerate(dashboard_data['top_opportunities'][:5], 1):
            print(f"{i}. {opp['symbol']} ({opp['screener']}) - Score: {opp['composite_score']:.2f}")
            print(f"   {opp['setup_name']} | {opp['direction'].upper()} | R: {opp['r_multiple']:.1f}")

        risk = dashboard_data['risk_summary']
        if risk:
            print("\nRISK SUMMARY:")
            print("-" * 60)
            print(f"Total Portfolio Risk: {risk.get('total_risk_percent', 0):.1f}%")
            print(f"Active Positions: {risk.get('position_count', 0)}")

        print("="*60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Master Trading Dashboard")
    parser.add_argument("--hours-lookback", type=int, default=24,
                       help="Hours to look back for active signals (default: 24)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    try:
        dashboard = MasterDashboard()
        dashboard.run(hours_lookback=args.hours_lookback)

    except Exception as e:
        print(f"Dashboard generation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
