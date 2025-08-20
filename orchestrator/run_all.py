#!/usr/bin/env python3
"""Main orchestrator for the trading system.
Minimal shim for automated scheduling and execution across market sessions.
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

# Compute project root programmatically
project_root = Path(__file__).resolve().parents[1]

# Add parent directory to path for imports
sys.path.append(str(project_root))

from orchestrator.modes import MarketMode, determine_market_mode
from orchestrator.schedules import get_schedule_for_mode
from utils.config import config
from utils.logging_setup import get_logger


def main():
    """Main orchestrator entry point."""
    parser = argparse.ArgumentParser(description="Trading System Orchestrator")
    parser.add_argument(
        "--mode",
        choices=["premarket", "market", "postmarket", "daily"], 
        help="Force specific mode (default: auto-detect)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without executing"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    # Setup logging
    logger = get_logger("orchestrator")

    # Log environment at startup
    logger.info(f"env={dict(os.environ)}")

    # Log project root for debugging
    logger.info(f"Running orchestrator in {config.APP_ENV} mode")
    logger.info(f"Project root: {project_root}")
    if config.DEPLOYMENT_TAG:
        logger.info(f"Deployment: {config.DEPLOYMENT_TAG}")

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
                logger.info(f"  - {job['name']}: {job['module']} {' '.join(job.get('args', []))}")
            return

        # Execute scheduled jobs
        for job in schedule:
            logger.info(f"Executing: {job['name']}")

            start_time = time.time()
            try:
                # Build command with sys.executable and -m pattern
                cmd = [sys.executable, "-m", job["module"]] + job.get("args", [])

                # Log the exact command being executed
                logger.info(f"Command: {' '.join(cmd)}")

                result = subprocess.run(
                    cmd,
                    cwd=project_root,
                    shell=False,
                    check=False,
                    capture_output=True,
                    text=True,
                    env=os.environ,
                    timeout=1800,  # 30-minute timeout for any single job
                )

                execution_time = time.time() - start_time

                # Check for non-zero return code and log details
                if result.returncode != 0:
                    # Trim stdout and stderr to ~2KB each
                    stdout_trimmed = result.stdout[-2048:] if result.stdout else "None"
                    stderr_trimmed = result.stderr[-2048:] if result.stderr else "None"
                    
                    logger.error(f"✗ {job['name']} failed after {execution_time:.2f}s")
                    logger.error(f"Command: {cmd}")
                    logger.error(f"Return code: {result.returncode}")
                    logger.error(f"STDOUT (last 2KB): {stdout_trimmed}")
                    logger.error(f"STDERR (last 2KB): {stderr_trimmed}")
                    
                    # Check if job is critical
                    if job.get('critical', False):
                        logger.error("Critical job failed, stopping orchestrator")
                        sys.exit(1)
                else:
                    logger.info(f"✓ {job['name']} completed in {execution_time:.2f}s")

                    # Log stdout if present
                    if result.stdout:
                        logger.debug(f"STDOUT: {result.stdout.strip()}")

            except subprocess.TimeoutExpired:
                logger.error(f"✗ {job['name']} failed - timeout after 30 minutes")
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
