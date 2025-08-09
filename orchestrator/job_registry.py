"""
Job registry for Trading Station.
Centralized registry of all jobs with metadata and execution wrappers.
"""

from typing import Dict, Any, Callable, Optional, List
from datetime import datetime
import importlib
import traceback
from dataclasses import dataclass

from utils.logging_setup import get_logger, get_run_id, set_run_id
from utils.time_utils import is_market_regular_session, is_market_open, now_et
from orchestrator.healthchecks import record_job_start, record_job_complete

logger = get_logger(__name__)

@dataclass
class JobDefinition:
    """Definition of a job with metadata and execution details."""
    name: str
    module_path: str
    function_name: str = "main"
    description: str = ""
    requires_market_open: bool = False
    allow_prepost: bool = False
    max_runtime_minutes: int = 60
    retry_count: int = 0
    tags: List[str] = None

class JobRegistry:
    """Registry of all jobs in the system."""
    
    def __init__(self):
        self.jobs: Dict[str, JobDefinition] = {}
        self._register_core_jobs()
    
    def _register_core_jobs(self):
        """Register core system jobs."""
        
        # Data fetching jobs
        self.register_job(JobDefinition(
            name="update_intraday",
            module_path="jobs.update_intraday",
            description="Update 1-minute intraday data for all tickers",
            requires_market_open=False,  # Allow manual runs
            allow_prepost=True,
            max_runtime_minutes=30,
            retry_count=1,
            tags=["data", "intraday", "realtime"]
        ))
        
        self.register_job(JobDefinition(
            name="update_intraday_30min",
            module_path="jobs.update_intraday_30min",
            description="Update 30-minute intraday data (resampled or API)",
            requires_market_open=False,
            allow_prepost=True,
            max_runtime_minutes=20,
            retry_count=1,
            tags=["data", "intraday"]
        ))
        
        self.register_job(JobDefinition(
            name="update_daily",
            module_path="jobs.update_daily",
            description="Update daily OHLCV data for all tickers",
            requires_market_open=False,
            allow_prepost=True,
            max_runtime_minutes=45,
            retry_count=2,
            tags=["data", "daily"]
        ))
        
        # Analysis jobs
        self.register_job(JobDefinition(
            name="find_avwap_anchors",
            module_path="jobs.find_avwap_anchors",
            description="Find AVWAP anchor points from daily data",
            requires_market_open=False,
            max_runtime_minutes=15,
            retry_count=1,
            tags=["analysis", "avwap"]
        ))
        
        # Ticker selection
        self.register_job(JobDefinition(
            name="opportunity_ticker_finder",
            module_path="ticker_selectors.opportunity_ticker_finder",
            description="Find opportunity tickers using Ashraf breakout logic",
            requires_market_open=False,
            max_runtime_minutes=30,
            retry_count=2,
            tags=["selection", "screening"]
        ))
        
        # Comprehensive update
        self.register_job(JobDefinition(
            name="update_all_data",
            module_path="jobs.update_all_data",
            description="Comprehensive data update workflow",
            requires_market_open=False,
            max_runtime_minutes=120,
            retry_count=1,
            tags=["data", "comprehensive"]
        ))
        
        # Screeners (placeholder for now)
        for screener in ["gapgo", "orb", "avwap", "breakout", "ema_pullback", "exhaustion"]:
            self.register_job(JobDefinition(
                name=screener,
                module_path=f"screeners.{screener}",
                description=f"{screener.upper()} screener strategy",
                requires_market_open=True,
                max_runtime_minutes=5,
                retry_count=1,
                tags=["screener", "strategy"]
            ))
    
    def register_job(self, job_def: JobDefinition):
        """Register a job in the registry."""
        if job_def.tags is None:
            job_def.tags = []
        
        self.jobs[job_def.name] = job_def
        logger.debug(f"Registered job: {job_def.name}")
    
    def get_job(self, job_name: str) -> Optional[JobDefinition]:
        """Get a job definition by name."""
        return self.jobs.get(job_name)
    
    def list_jobs(self, tag_filter: str = None) -> List[JobDefinition]:
        """List all jobs, optionally filtered by tag."""
        jobs = list(self.jobs.values())
        
        if tag_filter:
            jobs = [job for job in jobs if tag_filter in job.tags]
        
        return jobs
    
    def can_run_job(self, job_name: str) -> tuple[bool, str]:
        """Check if a job can run given current market conditions."""
        job_def = self.get_job(job_name)
        if not job_def:
            return False, f"Job {job_name} not found"
        
        current_time = now_et()
        
        # Check market hours requirement
        if job_def.requires_market_open:
            if not is_market_open(current_time):
                return False, "Market is not open"
        
        # Check pre/post market allowance
        if not job_def.allow_prepost:
            if not is_market_regular_session(current_time):
                if is_market_open(current_time):
                    return False, "Job not allowed during extended hours"
        
        return True, "OK"
    
    def execute_job(
        self, 
        job_name: str, 
        args: List[Any] = None, 
        kwargs: Dict[str, Any] = None,
        run_id: str = None
    ) -> tuple[bool, str]:
        """Execute a job with proper wrapping and error handling."""
        
        job_def = self.get_job(job_name)
        if not job_def:
            return False, f"Job {job_name} not found in registry"
        
        # Check if job can run
        can_run, reason = self.can_run_job(job_name)
        if not can_run:
            logger.warning(f"Job {job_name} cannot run: {reason}")
            return False, reason
        
        # Set up run ID
        if run_id:
            set_run_id(run_id)
        else:
            run_id = get_run_id()
        
        # Record job start
        start_time = datetime.now()
        record_job_start(job_name, run_id)
        
        logger.info(f"Starting job {job_name} (run_id: {run_id})")
        
        try:
            # Import the module
            module = importlib.import_module(job_def.module_path)
            
            # Get the function
            if not hasattr(module, job_def.function_name):
                raise AttributeError(f"Function {job_def.function_name} not found in {job_def.module_path}")
            
            job_function = getattr(module, job_def.function_name)
            
            # Prepare arguments
            args = args or []
            kwargs = kwargs or {}
            
            # Execute the job
            result = job_function(*args, **kwargs)
            
            # Calculate duration
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            # Determine success based on result
            success = True
            message = f"Completed successfully in {duration_ms:.1f}ms"
            
            # Some jobs return boolean or dict to indicate success
            if isinstance(result, bool):
                success = result
                if not success:
                    message = f"Job returned False after {duration_ms:.1f}ms"
            elif isinstance(result, dict):
                # Check for common success indicators
                if 'success' in result:
                    success = result['success']
                elif 'error' in result:
                    success = False
                    message = f"Job reported error: {result['error']}"
            
            # Record completion
            record_job_complete(job_name, success, duration_ms, message)
            
            if success:
                logger.info(f"Job {job_name} completed successfully in {duration_ms:.1f}ms")
            else:
                logger.warning(f"Job {job_name} failed: {message}")
            
            return success, message
            
        except Exception as e:
            # Calculate duration
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            # Get full traceback
            error_details = traceback.format_exc()
            message = f"Job failed with exception: {str(e)}"
            
            logger.error(f"Job {job_name} failed after {duration_ms:.1f}ms: {e}")
            logger.debug(f"Full traceback for {job_name}:\n{error_details}")
            
            # Record failure
            record_job_complete(job_name, False, duration_ms, message)
            
            return False, message
    
    def execute_job_with_retry(
        self, 
        job_name: str, 
        args: List[Any] = None, 
        kwargs: Dict[str, Any] = None,
        run_id: str = None
    ) -> tuple[bool, str]:
        """Execute a job with retry logic."""
        
        job_def = self.get_job(job_name)
        if not job_def:
            return False, f"Job {job_name} not found in registry"
        
        max_attempts = job_def.retry_count + 1  # Original attempt + retries
        
        for attempt in range(max_attempts):
            if attempt > 0:
                logger.info(f"Retrying job {job_name} (attempt {attempt + 1}/{max_attempts})")
            
            success, message = self.execute_job(job_name, args, kwargs, run_id)
            
            if success:
                return True, message
            
            # If this was the last attempt, return the failure
            if attempt == max_attempts - 1:
                final_message = f"Job failed after {max_attempts} attempts. Last error: {message}"
                logger.error(final_message)
                return False, final_message
            
            # Wait before retry (simple exponential backoff)
            import time
            wait_time = 2 ** attempt  # 1s, 2s, 4s, etc.
            logger.info(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
        
        return False, "Unexpected end of retry loop"
    
    def get_job_summary(self) -> Dict[str, Any]:
        """Get a summary of all registered jobs."""
        summary = {
            "total_jobs": len(self.jobs),
            "jobs_by_tag": {},
            "jobs": []
        }
        
        # Count jobs by tag
        all_tags = set()
        for job_def in self.jobs.values():
            all_tags.update(job_def.tags)
        
        for tag in all_tags:
            summary["jobs_by_tag"][tag] = len([
                job for job in self.jobs.values() 
                if tag in job.tags
            ])
        
        # Job details
        for job_def in self.jobs.values():
            can_run, reason = self.can_run_job(job_def.name)
            summary["jobs"].append({
                "name": job_def.name,
                "description": job_def.description,
                "tags": job_def.tags,
                "can_run": can_run,
                "reason": reason if not can_run else "OK"
            })
        
        return summary

# Global job registry instance
_job_registry: Optional[JobRegistry] = None

def get_job_registry() -> JobRegistry:
    """Get the global job registry instance."""
    global _job_registry
    if _job_registry is None:
        _job_registry = JobRegistry()
    return _job_registry

# Convenience functions
def execute_job(job_name: str, *args, **kwargs) -> tuple[bool, str]:
    """Execute a job by name."""
    return get_job_registry().execute_job(job_name, list(args), kwargs)

def execute_job_with_retry(job_name: str, *args, **kwargs) -> tuple[bool, str]:
    """Execute a job with retry logic."""
    return get_job_registry().execute_job_with_retry(job_name, list(args), kwargs)

def list_jobs(tag_filter: str = None) -> List[JobDefinition]:
    """List all jobs."""
    return get_job_registry().list_jobs(tag_filter)

# Export classes and functions
__all__ = [
    'JobDefinition', 'JobRegistry',
    'get_job_registry', 'execute_job', 'execute_job_with_retry', 'list_jobs'
]