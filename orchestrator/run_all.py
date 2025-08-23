#!/usr/bin/env python3
"""Continuous minute scheduler for the trading system.
Runs in a tick loop, executing jobs based on time-based scheduling.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Set

# Compute project root programmatically
project_root = Path(__file__).resolve().parents[1]

# Add parent directory to path for imports
sys.path.append(str(project_root))

from orchestrator.schedules import due_every_minute, due_every_quarter_hour, due_once_at
from utils.config import config
from utils.env_validation import validate_spaces_endpoint, validate_paths, validate_do_ids
from utils.logging_setup import get_logger
from utils.time_utils import get_market_time

# Global flag for graceful shutdown
shutdown_requested = False

# Memory for "run once per day" jobs
daily_job_memory: Set[str] = set()


def signal_handler(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_requested
    shutdown_requested = True
    logger = get_logger("orchestrator")
    logger.info(f"📨 Received signal {signum}, initiating graceful shutdown...")


def execute_job(job: Dict, logger) -> bool:
    """Execute a single job with proper error handling and logging.
    
    Returns:
        bool: True if job succeeded, False if it failed
    """
    job_name = job['name']
    module = job['module']
    args = job.get('args', [])
    critical = job.get('critical', False)

    logger.info(f"Executing: {job_name}")

    start_time = time.time()
    try:
        # Build command with sys.executable and -m pattern
        cmd = [sys.executable, "-m", module] + args

        # Log the exact command being executed
        logger.info(f"Command: python -m {module} {' '.join(args)}")

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

            logger.error(f"✗ {job_name} failed after {execution_time:.2f}s")
            logger.error(f"Command: {cmd}")
            logger.error(f"Return code: {result.returncode}")
            logger.error(f"STDOUT (last 2KB): {stdout_trimmed}")
            logger.error(f"STDERR (last 2KB): {stderr_trimmed}")

            return False
        else:
            logger.info(f"✓ {job_name} completed in {execution_time:.2f}s")

            # Log stdout if present (debug level)
            if result.stdout:
                logger.debug(f"STDOUT: {result.stdout.strip()}")

            return True

    except subprocess.TimeoutExpired:
        execution_time = time.time() - start_time
        logger.error(f"✗ {job_name} failed - timeout after {execution_time:.1f}s")
        return False

    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"✗ {job_name} failed with exception after {execution_time:.2f}s: {e}")
        return False


def get_due_jobs(now_et: datetime, logger) -> list:
    """Get all jobs due at the current time."""
    global daily_job_memory

    jobs = []

    # Jobs due every minute
    jobs.extend(due_every_minute(now_et))

    # Jobs due every quarter hour
    jobs.extend(due_every_quarter_hour(now_et))

    # Jobs that run once at specific times
    once_jobs = due_once_at(now_et)
    for job in once_jobs:
        if job.get('run_once_per_day', False):
            # Check if we've already run this job today
            today_key = f"{now_et.date()}_{job['name']}"
            if today_key not in daily_job_memory:
                jobs.append(job)
                daily_job_memory.add(today_key)
                logger.debug(f"🗓️ Scheduled once-per-day job: {job['name']}")
            else:
                logger.debug(f"⏭️ Skipping already-run job: {job['name']}")
        else:
            jobs.append(job)

    # Clean up old memory entries (keep only today's entries)
    today_str = str(now_et.date())
    daily_job_memory = {key for key in daily_job_memory if key.startswith(today_str)}

    return jobs


def format_next_jobs(now_et: datetime) -> str:
    """Format a preview of upcoming jobs for heartbeat logging."""
    next_minute = now_et.replace(second=0, microsecond=0)
    next_minute = next_minute.replace(minute=next_minute.minute + 1)

    # Get jobs for next minute
    next_jobs = get_due_jobs(next_minute, get_logger("orchestrator"))

    if not next_jobs:
        return "No jobs scheduled"

    # Return just the count and first few job names
    job_names = [job['name'] for job in next_jobs[:3]]
    if len(next_jobs) > 3:
        job_names.append(f"... +{len(next_jobs) - 3} more")

    return f"{len(next_jobs)} jobs: {', '.join(job_names)}"


def main():
    """Main continuous scheduler entry point."""
    global shutdown_requested

    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description="Trading System Continuous Scheduler")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without executing"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    # Setup logging
    logger = get_logger("orchestrator")

    # Log startup information
    logger.info(f"🚀 Running orchestrator in {config.APP_ENV} mode")
    logger.info(f"📁 Project root: {project_root}")
    if config.DEPLOYMENT_TAG:
        logger.info(f"🏷️ Deployment: {config.DEPLOYMENT_TAG}")

    # Validate environment variables early - fail fast if misconfigured
    logger.info("🔍 Validating environment configuration...")
    try:
        validate_spaces_endpoint(
            os.getenv("SPACES_ENDPOINT", ""), 
            os.getenv("SPACES_REGION", "")
        )
        validate_paths(
            os.getenv("DATA_ROOT", ""), 
            os.getenv("UNIVERSE_KEY", "")
        )
        validate_do_ids(os.getenv("DO_APP_ID", ""))
        logger.info("✅ Environment validation passed")
    except RuntimeError as e:
        logger.critical(f"❌ Environment validation failed: {e}")
        logger.critical("🚨 Fix environment variables before running the orchestrator")
        return 1

    mode_str = "DRY RUN" if args.dry_run else "LIVE"
    logger.info(f"🎯 Starting continuous minute scheduler in {mode_str} mode")
    logger.info("⏰ Will run 1-min data fetch every minute 04:00-20:00 ET")
    logger.info("📊 Will run 30-min data fetch at :00/:15/:30/:45")
    logger.info("🌅 Will run daily refresh at 06:30 ET once per day")
    logger.info("🔄 Entering main tick loop...")

    try:
        while not shutdown_requested:
            # Get current time in ET
            now_et = get_market_time()

            # Get all jobs due at this minute
            due_jobs = get_due_jobs(now_et, logger)

            # Log heartbeat every minute
            next_jobs_preview = format_next_jobs(now_et)
            deployment_info = f"tag={config.DEPLOYMENT_TAG or 'unknown'}"
            logger.info(f"💓 Heartbeat: {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')} | Next: {next_jobs_preview} | {deployment_info}")

            # Execute due jobs
            if due_jobs:
                logger.info(f"📋 Found {len(due_jobs)} job(s) due for execution")

                for job in due_jobs:
                    if shutdown_requested:
                        logger.info("🛑 Shutdown requested, stopping job execution")
                        break

                    if args.dry_run:
                        logger.info(f"[DRY RUN] Would execute: {job['name']} - python -m {job['module']} {' '.join(job.get('args', []))}")
                    else:
                        success = execute_job(job, logger)

                        # Handle critical job failures
                        if not success and job.get('critical', False):
                            logger.error(f"🚨 Critical job '{job['name']}' failed, stopping orchestrator")
                            sys.exit(1)

                        # Small delay between jobs to avoid overwhelming the system
                        if len(due_jobs) > 1:
                            time.sleep(1)
            else:
                logger.debug(f"⏸️ No jobs due at {now_et.strftime('%H:%M')}")

            # Calculate sleep time to next minute boundary
            now_seconds = now_et.second + now_et.microsecond / 1000000.0
            sleep_time = 60 - now_seconds

            logger.debug(f"😴 Sleeping {sleep_time:.1f}s until next minute boundary")

            # Sleep in small chunks to allow for responsive shutdown
            sleep_start = time.time()
            while time.time() - sleep_start < sleep_time:
                if shutdown_requested:
                    break
                time.sleep(min(1.0, sleep_time - (time.time() - sleep_start)))

        logger.info("🏁 Orchestrator shutdown complete")
        return 0

    except KeyboardInterrupt:
        logger.info("⏹️ Orchestrator stopped by user (Ctrl+C)")
        return 0
    except Exception as e:
        logger.error(f"💥 Unexpected error in main loop: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
