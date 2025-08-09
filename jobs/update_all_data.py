"""
Comprehensive data update job for Trading Station.
Runs ticker selection, daily data update, and AVWAP anchor finding.
"""

import sys
from datetime import datetime
from typing import List, Optional

from utils.logging_setup import get_logger, setup_logging, log_job_start, log_job_complete
from utils.ticker_management import update_master_tickerlist
from ticker_selectors.opportunity_ticker_finder import OpportunityTickerFinder
from jobs.update_daily import DailyDataUpdater
from jobs.update_intraday import IntradayDataUpdater
from jobs.update_intraday_30min import Intraday30minUpdater
from jobs.find_avwap_anchors import AvwapAnchorFinder

logger = get_logger(__name__)

class ComprehensiveDataUpdater:
    """Orchestrates complete data update workflow."""
    
    def __init__(self):
        self.opportunity_finder = OpportunityTickerFinder()
        self.daily_updater = DailyDataUpdater()
        self.intraday_updater = IntradayDataUpdater()
        self.intraday_30min_updater = Intraday30minUpdater()
        self.anchor_finder = AvwapAnchorFinder()
    
    def run_ticker_selection(self) -> List[str]:
        """Run opportunity ticker selection."""
        logger.info("=== PHASE 1: Ticker Selection ===")
        
        try:
            # Run opportunity finder
            selector_tickers = self.opportunity_finder.run_screening()
            
            # Update master ticker list (includes manual tickers)
            master_tickers = update_master_tickerlist(selector_tickers)
            
            logger.info(f"Ticker selection complete: {len(master_tickers)} tickers in master list")
            return master_tickers
            
        except Exception as e:
            logger.error(f"Ticker selection failed: {e}")
            return []
    
    def run_daily_updates(self, ticker_list: List[str]) -> bool:
        """Run daily data updates."""
        logger.info("=== PHASE 2: Daily Data Update ===")
        
        try:
            results = self.daily_updater.run_update(ticker_list)
            success_count = sum(1 for success in results.values() if success)
            
            logger.info(f"Daily update complete: {success_count}/{len(ticker_list)} successful")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Daily data update failed: {e}")
            return False
    
    def run_intraday_updates(self, ticker_list: List[str]) -> bool:
        """Run intraday data updates."""
        logger.info("=== PHASE 3: Intraday Data Update ===")
        
        try:
            # Update 1-minute data
            logger.info("Updating 1-minute intraday data...")
            results_1m = self.intraday_updater.run_update(ticker_list)
            success_1m = sum(1 for success in results_1m.values() if success)
            
            # Update 30-minute data (can resample from 1-min)
            logger.info("Updating 30-minute intraday data...")
            results_30m = self.intraday_30min_updater.run_update(ticker_list)
            success_30m = sum(1 for success in results_30m.values() if success)
            
            logger.info(
                f"Intraday update complete: "
                f"1-min: {success_1m}/{len(ticker_list)}, "
                f"30-min: {success_30m}/{len(ticker_list)}"
            )
            
            return success_1m > 0 or success_30m > 0
            
        except Exception as e:
            logger.error(f"Intraday data update failed: {e}")
            return False
    
    def run_anchor_finding(self, ticker_list: List[str]) -> bool:
        """Run AVWAP anchor finding."""
        logger.info("=== PHASE 4: AVWAP Anchor Finding ===")
        
        try:
            anchors = self.anchor_finder.run_anchor_finding(ticker_list)
            
            logger.info(f"Anchor finding complete: {len(anchors)} anchors found")
            return True
            
        except Exception as e:
            logger.error(f"AVWAP anchor finding failed: {e}")
            return False
    
    def run_comprehensive_update(self, skip_ticker_selection: bool = False) -> bool:
        """Run the complete data update workflow."""
        start_time = datetime.now()
        log_job_start(logger, "comprehensive_data_update")
        
        try:
            # Phase 1: Ticker Selection
            if not skip_ticker_selection:
                ticker_list = self.run_ticker_selection()
                if not ticker_list:
                    logger.error("No tickers available for processing")
                    return False
            else:
                # Load existing master ticker list
                from utils.ticker_management import load_master_tickerlist
                ticker_list = load_master_tickerlist()
                logger.info(f"Using existing ticker list: {len(ticker_list)} tickers")
                if not ticker_list:
                    logger.error("No existing ticker list found")
                    return False
            
            # Phase 2: Daily Data
            daily_success = self.run_daily_updates(ticker_list)
            if not daily_success:
                logger.warning("Daily data update had issues, but continuing...")
            
            # Phase 3: Intraday Data
            intraday_success = self.run_intraday_updates(ticker_list)
            if not intraday_success:
                logger.warning("Intraday data update had issues, but continuing...")
            
            # Phase 4: AVWAP Anchors
            anchor_success = self.run_anchor_finding(ticker_list)
            if not anchor_success:
                logger.warning("AVWAP anchor finding had issues")
            
            # Summary
            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            log_job_complete(logger, "comprehensive_data_update", elapsed_ms, len(ticker_list))
            
            logger.info("=== COMPREHENSIVE UPDATE COMPLETE ===")
            logger.info(f"Processed {len(ticker_list)} tickers")
            logger.info(f"Daily data: {'✓' if daily_success else '✗'}")
            logger.info(f"Intraday data: {'✓' if intraday_success else '✗'}")
            logger.info(f"AVWAP anchors: {'✓' if anchor_success else '✗'}")
            
            return daily_success or intraday_success
            
        except Exception as e:
            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            logger.error(f"Comprehensive update failed: {e}")
            log_job_complete(logger, "comprehensive_data_update", elapsed_ms, 0)
            return False

def main():
    """Main entry point for comprehensive data update."""
    # Parse command line arguments
    skip_ticker_selection = "--skip-ticker-selection" in sys.argv
    
    # Set up logging
    setup_logging()
    
    try:
        updater = ComprehensiveDataUpdater()
        success = updater.run_comprehensive_update(skip_ticker_selection)
        
        if success:
            print("✓ Comprehensive data update completed successfully")
            return 0
        else:
            print("✗ Comprehensive data update failed")
            return 1
            
    except Exception as e:
        logger.error(f"Comprehensive data update failed: {e}")
        print(f"✗ Comprehensive data update failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())