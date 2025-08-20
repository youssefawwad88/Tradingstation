#!/usr/bin/env python3
"""
Main orchestrator for the trading system.
Minimal shim for automated scheduling and execution across market sessions.
"""

import os
import sys
import time
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import config
from utils.logging_setup import get_logger
from orchestrator.schedules import get_schedule_for_mode
from orchestrator.modes import MarketMode, determine_market_mode


def main():
    """Main orchestrator entry point."""
    parser = argparse.ArgumentParser(description="Trading System Orchestrator")
    parser.add_argument("--mode", choices=["premarket", "market", "postmarket", "daily"], 
                       help="Force specific mode (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run without executing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    logger = get_logger("orchestrator")
    
    try:
        # Determine market mode
        if args.mode:
            mode = MarketMode(args.mode)
        else:
            mode = determine_market_mode()
        
        logger.info(f"Running orchestrator in {mode.value} mode")
        
        # Get schedule for this mode
        schedule = get_schedule_for_mode(mode)
        
        if args.dry_run:
            logger.info("DRY RUN - Would execute:")
            for job in schedule:
                logger.info(f"  - {job['name']}: {job['command']}")
            return
        
        # Execute scheduled jobs
        for job in schedule:
            logger.info(f"Executing: {job['name']}")
            
            start_time = time.time()
            try:
                # Execute job command
                result = os.system(job['command'])
                execution_time = time.time() - start_time
                
                if result == 0:
                    logger.info(f"✓ {job['name']} completed in {execution_time:.2f}s")
                else:
                    logger.error(f"✗ {job['name']} failed with exit code {result}")
                    
                    # Check if job is critical
                    if job.get('critical', False):
                        logger.error("Critical job failed, stopping orchestrator")
                        sys.exit(1)
                        
            except Exception as e:
                logger.error(f"✗ {job['name']} failed with exception: {e}")
                if job.get('critical', False):
                    logger.error("Critical job failed, stopping orchestrator")
                    sys.exit(1)
            
            # Optional delay between jobs
            if 'delay' in job:
                logger.info(f"Waiting {job['delay']}s before next job...")
                time.sleep(job['delay'])
        
        logger.info("Orchestrator completed successfully")
        
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()