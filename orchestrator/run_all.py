"""
Main orchestrator for Trading Station.
Starts scheduler, manages services, and provides CLI interface.
"""

import sys
import argparse
import time
from datetime import datetime
from typing import List, Optional

from utils.config import validate_config, DEBUG_MODE
from utils.logging_setup import setup_logging, get_logger
from utils.ticker_management import echo_ticker_summary
from orchestrator.scheduler import get_scheduler
from orchestrator.job_registry import get_job_registry, list_jobs
from orchestrator.healthchecks import get_health_checker

logger = get_logger(__name__)

class TradingStationOrchestrator:
    """Main orchestrator for the Trading Station system."""
    
    def __init__(self):
        self.scheduler = get_scheduler()
        self.job_registry = get_job_registry()
        self.health_checker = get_health_checker()
    
    def validate_system(self) -> bool:
        """Validate system configuration and dependencies."""
        logger.info("Validating system configuration...")
        
        try:
            # Validate configuration
            if not validate_config():
                logger.warning("Configuration validation had warnings")
            
            # Echo ticker summary
            echo_ticker_summary()
            
            # Test job registry
            jobs = list_jobs()
            logger.info(f"Job registry loaded: {len(jobs)} jobs available")
            
            # Test health checker
            system_health = self.health_checker.get_system_health()
            logger.info(f"Health checker initialized: {system_health.overall_status.value}")
            
            logger.info("System validation complete")
            return True
            
        except Exception as e:
            logger.error(f"System validation failed: {e}")
            return False
    
    def start_scheduler(self) -> bool:
        """Start the job scheduler."""
        try:
            self.scheduler.start()
            return True
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            return False
    
    def run_interactive(self):
        """Run in interactive mode with CLI commands."""
        logger.info("Starting interactive mode. Type 'help' for commands.")
        
        while True:
            try:
                command = input("tradingstation> ").strip().lower()
                
                if command in ['exit', 'quit', 'q']:
                    break
                elif command == 'help':
                    self._print_help()
                elif command == 'status':
                    self._print_status()
                elif command == 'jobs':
                    self._print_jobs()
                elif command == 'health':
                    self._print_health()
                elif command.startswith('run '):
                    job_name = command[4:].strip()
                    self._run_job(job_name)
                elif command == 'pause':
                    self.scheduler.pause()
                    print("Scheduler paused")
                elif command == 'resume':
                    self.scheduler.resume()
                    print("Scheduler resumed")
                elif command == '':
                    continue
                else:
                    print(f"Unknown command: {command}. Type 'help' for available commands.")
                    
            except KeyboardInterrupt:
                print("\nUse 'exit' to quit")
            except EOFError:
                break
            except Exception as e:
                logger.error(f"Error in interactive mode: {e}")
                print(f"Error: {e}")
    
    def _print_help(self):
        """Print help message."""
        help_text = """
Available commands:
  help          - Show this help message
  status        - Show scheduler status
  jobs          - List all available jobs
  health        - Show system health
  run <job>     - Run a specific job immediately
  pause         - Pause the scheduler
  resume        - Resume the scheduler
  exit/quit/q   - Exit the program
        """
        print(help_text)
    
    def _print_status(self):
        """Print scheduler status."""
        status = self.scheduler.get_status()
        
        print(f"\nScheduler Status:")
        print(f"  Running: {status['running']}")
        print(f"  State: {status['scheduler_state']}")
        print(f"  Total Jobs: {status['total_jobs']}")
        print(f"  Current Time: {status['current_time']}")
        
        print(f"\nNext Job Runs:")
        for job in sorted(status['jobs'], key=lambda x: x['next_run'] or ''):
            next_run = job['next_run']
            if next_run:
                next_run = datetime.fromisoformat(next_run).strftime('%H:%M:%S')
            else:
                next_run = "Not scheduled"
            print(f"  {job['name']}: {next_run}")
    
    def _print_jobs(self):
        """Print all available jobs."""
        jobs = list_jobs()
        
        print(f"\nAvailable Jobs ({len(jobs)}):")
        
        # Group by tags
        by_tag = {}
        for job in jobs:
            for tag in job.tags:
                if tag not in by_tag:
                    by_tag[tag] = []
                by_tag[tag].append(job)
        
        for tag, tag_jobs in by_tag.items():
            print(f"\n  {tag.upper()}:")
            for job in tag_jobs:
                can_run, reason = self.job_registry.can_run_job(job.name)
                status = "✓" if can_run else "✗"
                print(f"    {status} {job.name}: {job.description}")
                if not can_run:
                    print(f"        ({reason})")
    
    def _print_health(self):
        """Print system health."""
        summary = self.health_checker.get_health_summary()
        print(f"\n{summary}")
    
    def _run_job(self, job_name: str):
        """Run a specific job."""
        if not job_name:
            print("Please specify a job name")
            return
        
        job_def = self.job_registry.get_job(job_name)
        if not job_def:
            print(f"Job '{job_name}' not found")
            return
        
        print(f"Running job: {job_name}")
        success, message = self.scheduler.run_job_now(job_name)
        
        if success:
            print(f"✓ Job completed successfully: {message}")
        else:
            print(f"✗ Job failed: {message}")
    
    def run_daemon(self):
        """Run as a daemon (non-interactive)."""
        logger.info("Running in daemon mode...")
        
        try:
            self.scheduler.keep_alive()
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Error in daemon mode: {e}")
        finally:
            self.scheduler.shutdown()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Trading Station Orchestrator")
    
    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help='Run in interactive mode'
    )
    
    parser.add_argument(
        '--daemon', '-d',
        action='store_true', 
        help='Run as daemon (default)'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate configuration and exit'
    )
    
    parser.add_argument(
        '--run-job',
        type=str,
        help='Run a specific job and exit'
    )
    
    parser.add_argument(
        '--list-jobs',
        action='store_true',
        help='List all available jobs and exit'
    )
    
    parser.add_argument(
        '--health',
        action='store_true',
        help='Show system health and exit'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = 'DEBUG' if args.debug or DEBUG_MODE else None
    setup_logging(level=log_level)
    
    logger.info("Starting Trading Station Orchestrator")
    logger.info(f"Arguments: {vars(args)}")
    
    # Create orchestrator
    orchestrator = TradingStationOrchestrator()
    
    # Validate system
    if not orchestrator.validate_system():
        logger.error("System validation failed")
        return 1
    
    # Handle specific commands
    if args.validate_only:
        print("✓ System validation passed")
        return 0
    
    if args.list_jobs:
        jobs = list_jobs()
        print(f"Available jobs: {len(jobs)}")
        for job in jobs:
            print(f"  {job.name}: {job.description}")
        return 0
    
    if args.health:
        health_checker = get_health_checker()
        summary = health_checker.get_health_summary()
        print(summary)
        return 0
    
    if args.run_job:
        job_registry = get_job_registry()
        success, message = job_registry.execute_job_with_retry(args.run_job)
        print(f"Job {args.run_job}: {'SUCCESS' if success else 'FAILED'}")
        print(f"Message: {message}")
        return 0 if success else 1
    
    # Start scheduler
    if not orchestrator.start_scheduler():
        logger.error("Failed to start scheduler")
        return 1
    
    # Run mode
    try:
        if args.interactive:
            orchestrator.run_interactive()
        else:
            orchestrator.run_daemon()
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        return 1
    finally:
        orchestrator.scheduler.shutdown()
    
    logger.info("Trading Station Orchestrator shutdown complete")
    return 0

if __name__ == "__main__":
    sys.exit(main())