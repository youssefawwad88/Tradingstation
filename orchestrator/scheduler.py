"""
APScheduler-based job scheduler for Trading Station.
Handles market-hours aware scheduling with timezone support.
"""

import yaml
from datetime import datetime, time
from typing import Dict, Any, List, Optional
from pathlib import Path
import signal
import sys

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
import pytz

from utils.config import TIMEZONE, get_strategy_config
from utils.logging_setup import get_logger, setup_logging
from utils.time_utils import is_market_regular_session, now_et
from orchestrator.job_registry import get_job_registry, JobRegistry
from orchestrator.healthchecks import get_health_checker, write_heartbeat

logger = get_logger(__name__)

class TradingStationScheduler:
    """Market-hours aware job scheduler."""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler(timezone=TIMEZONE)
        self.job_registry = get_job_registry()
        self.health_checker = get_health_checker()
        self.running = False
        
        # Load schedule configuration
        self.schedule_config = self._load_schedule_config()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Set up scheduler event listeners
        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
        self.scheduler.add_listener(self._job_missed, EVENT_JOB_MISSED)
    
    def _load_schedule_config(self) -> Dict[str, Any]:
        """Load schedule configuration from YAML."""
        try:
            config = get_strategy_config()
            return config.get('schedules', {})
        except Exception as e:
            logger.warning(f"Failed to load schedule config: {e}")
            return self._get_default_schedule()
    
    def _get_default_schedule(self) -> Dict[str, Any]:
        """Get default schedule configuration."""
        return {
            "premarket": {
                "jobs": [
                    {
                        "name": "opportunity_ticker_finder",
                        "time": "06:30",
                        "enabled": True
                    },
                    {
                        "name": "find_avwap_anchors", 
                        "time": "06:45",
                        "enabled": True
                    },
                    {
                        "name": "update_daily",
                        "time": "07:00", 
                        "enabled": True
                    }
                ]
            },
            "regular_session": {
                "jobs": [
                    {
                        "name": "update_intraday",
                        "interval": "1min",
                        "enabled": True,
                        "start_time": "09:30",
                        "end_time": "16:00"
                    },
                    {
                        "name": "update_intraday_30min",
                        "interval": "10min",
                        "enabled": True,
                        "start_time": "09:30", 
                        "end_time": "16:00"
                    }
                ]
            }
        }
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down scheduler...")
        self.shutdown()
        sys.exit(0)
    
    def _job_executed(self, event):
        """Handle job execution events."""
        logger.debug(f"Job executed: {event.job_id}")
    
    def _job_error(self, event):
        """Handle job error events."""
        logger.error(f"Job error: {event.job_id} - {event.exception}")
    
    def _job_missed(self, event):
        """Handle missed job events."""
        logger.warning(f"Job missed: {event.job_id}")
    
    def _create_job_wrapper(self, job_name: str):
        """Create a wrapper function for scheduled job execution."""
        def wrapper():
            try:
                # Check market conditions for market-dependent jobs
                job_def = self.job_registry.get_job(job_name)
                if job_def and job_def.requires_market_open:
                    if not is_market_regular_session():
                        logger.info(f"Skipping {job_name} - market not in regular session")
                        return
                
                # Execute the job
                success, message = self.job_registry.execute_job_with_retry(job_name)
                
                if not success:
                    logger.error(f"Scheduled job {job_name} failed: {message}")
                
            except Exception as e:
                logger.error(f"Error in job wrapper for {job_name}: {e}")
        
        return wrapper
    
    def _parse_time(self, time_str: str) -> time:
        """Parse time string in HH:MM format."""
        try:
            hour, minute = map(int, time_str.split(':'))
            return time(hour, minute)
        except ValueError:
            logger.error(f"Invalid time format: {time_str}")
            return time(9, 30)  # Default to 9:30 AM
    
    def _add_premarket_jobs(self):
        """Add premarket jobs to scheduler."""
        premarket_jobs = self.schedule_config.get("premarket", {}).get("jobs", [])
        
        for job_config in premarket_jobs:
            if not job_config.get("enabled", True):
                continue
            
            job_name = job_config["name"]
            job_time = self._parse_time(job_config["time"])
            
            # Create cron trigger for daily execution
            trigger = CronTrigger(
                hour=job_time.hour,
                minute=job_time.minute,
                timezone=TIMEZONE
            )
            
            self.scheduler.add_job(
                self._create_job_wrapper(job_name),
                trigger=trigger,
                id=f"premarket_{job_name}",
                name=f"Premarket {job_name}",
                max_instances=1,
                replace_existing=True
            )
            
            logger.info(f"Scheduled premarket job: {job_name} at {job_time}")
    
    def _add_regular_session_jobs(self):
        """Add regular session jobs to scheduler."""
        session_jobs = self.schedule_config.get("regular_session", {}).get("jobs", [])
        
        for job_config in session_jobs:
            if not job_config.get("enabled", True):
                continue
            
            job_name = job_config["name"]
            interval = job_config.get("interval", "15min")
            start_time = self._parse_time(job_config.get("start_time", "09:30"))
            end_time = self._parse_time(job_config.get("end_time", "16:00"))
            
            # Parse interval
            if interval.endswith("min"):
                minutes = int(interval[:-3])
                trigger = IntervalTrigger(minutes=minutes, timezone=TIMEZONE)
            elif interval.endswith("s"):
                seconds = int(interval[:-1])
                trigger = IntervalTrigger(seconds=seconds, timezone=TIMEZONE)
            else:
                logger.warning(f"Unknown interval format: {interval}")
                continue
            
            # Add job with time restrictions
            self.scheduler.add_job(
                self._create_job_wrapper(job_name),
                trigger=trigger,
                id=f"session_{job_name}",
                name=f"Session {job_name}",
                max_instances=1,
                replace_existing=True,
                # Note: APScheduler doesn't have built-in time window support
                # We handle this in the job wrapper by checking market hours
            )
            
            logger.info(f"Scheduled session job: {job_name} every {interval} ({start_time}-{end_time})")
    
    def _add_screener_jobs(self):
        """Add screener jobs to scheduler."""
        screeners_config = self.schedule_config.get("screeners", {})
        
        # High-frequency screeners (first hour)
        high_freq = screeners_config.get("high_freq", [])
        for job_config in high_freq:
            if not job_config.get("enabled", True):
                continue
            
            job_name = job_config["name"]
            interval = job_config.get("interval", "1min")
            
            if interval.endswith("min"):
                minutes = int(interval[:-3])
                trigger = IntervalTrigger(minutes=minutes, timezone=TIMEZONE)
            else:
                continue
            
            self.scheduler.add_job(
                self._create_job_wrapper(job_name),
                trigger=trigger,
                id=f"screener_hf_{job_name}",
                name=f"HF Screener {job_name}",
                max_instances=1,
                replace_existing=True
            )
            
            logger.info(f"Scheduled HF screener: {job_name} every {interval}")
        
        # Swing screeners (every 15 min)
        swing = screeners_config.get("swing", [])
        for job_config in swing:
            if not job_config.get("enabled", True):
                continue
            
            job_name = job_config["name"]
            interval = job_config.get("interval", "15min")
            
            if interval.endswith("min"):
                minutes = int(interval[:-3])
                trigger = IntervalTrigger(minutes=minutes, timezone=TIMEZONE)
            else:
                continue
            
            self.scheduler.add_job(
                self._create_job_wrapper(job_name),
                trigger=trigger,
                id=f"screener_swing_{job_name}",
                name=f"Swing Screener {job_name}",
                max_instances=1,
                replace_existing=True
            )
            
            logger.info(f"Scheduled swing screener: {job_name} every {interval}")
    
    def _add_dashboard_jobs(self):
        """Add dashboard consolidation jobs."""
        dashboard_jobs = self.schedule_config.get("dashboard", {}).get("jobs", [])
        
        for job_config in dashboard_jobs:
            if not job_config.get("enabled", True):
                continue
            
            job_name = job_config["name"]
            interval = job_config.get("interval", "5min")
            
            if interval.endswith("min"):
                minutes = int(interval[:-3])
                trigger = IntervalTrigger(minutes=minutes, timezone=TIMEZONE)
            
                self.scheduler.add_job(
                    self._create_job_wrapper(job_name),
                    trigger=trigger,
                    id=f"dashboard_{job_name}",
                    name=f"Dashboard {job_name}",
                    max_instances=1,
                    replace_existing=True
                )
                
                logger.info(f"Scheduled dashboard job: {job_name} every {interval}")
    
    def _add_heartbeat_job(self):
        """Add heartbeat job for health monitoring."""
        self.scheduler.add_job(
            write_heartbeat,
            trigger=IntervalTrigger(seconds=30, timezone=TIMEZONE),
            id="system_heartbeat",
            name="System Heartbeat",
            max_instances=1,
            replace_existing=True
        )
        
        logger.info("Scheduled system heartbeat every 30 seconds")
    
    def setup_schedule(self):
        """Set up the complete schedule."""
        logger.info("Setting up job schedule...")
        
        # Add all job categories
        self._add_premarket_jobs()
        self._add_regular_session_jobs()
        self._add_screener_jobs()
        self._add_dashboard_jobs()
        self._add_heartbeat_job()
        
        # Log scheduled jobs
        jobs = self.scheduler.get_jobs()
        logger.info(f"Total jobs scheduled: {len(jobs)}")
        
        for job in jobs:
            next_run = job.next_run_time
            next_run_str = next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else "Not scheduled"
            logger.info(f"  {job.name}: {next_run_str}")
    
    def start(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        logger.info("Starting Trading Station scheduler...")
        
        try:
            self.setup_schedule()
            self.scheduler.start()
            self.running = True
            
            # Write initial heartbeat
            write_heartbeat()
            
            logger.info("Scheduler started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    def shutdown(self):
        """Shutdown the scheduler gracefully."""
        if not self.running:
            return
        
        logger.info("Shutting down scheduler...")
        
        try:
            self.scheduler.shutdown(wait=True)
            self.running = False
            logger.info("Scheduler shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during scheduler shutdown: {e}")
    
    def pause(self):
        """Pause the scheduler."""
        self.scheduler.pause()
        logger.info("Scheduler paused")
    
    def resume(self):
        """Resume the scheduler."""
        self.scheduler.resume()
        logger.info("Scheduler resumed")
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        jobs = self.scheduler.get_jobs()
        
        status = {
            "running": self.running,
            "scheduler_state": self.scheduler.state,
            "total_jobs": len(jobs),
            "current_time": now_et().isoformat(),
            "jobs": []
        }
        
        for job in jobs:
            job_info = {
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            }
            status["jobs"].append(job_info)
        
        return status
    
    def run_job_now(self, job_name: str) -> tuple[bool, str]:
        """Run a specific job immediately."""
        logger.info(f"Running job {job_name} immediately...")
        return self.job_registry.execute_job_with_retry(job_name)
    
    def keep_alive(self):
        """Keep the scheduler running (blocking)."""
        if not self.running:
            raise RuntimeError("Scheduler is not running")
        
        logger.info("Scheduler is running. Press Ctrl+C to stop.")
        
        try:
            while self.running:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.shutdown()

# Global scheduler instance
_scheduler: Optional[TradingStationScheduler] = None

def get_scheduler() -> TradingStationScheduler:
    """Get the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = TradingStationScheduler()
    return _scheduler

# Export classes and functions
__all__ = [
    'TradingStationScheduler',
    'get_scheduler'
]