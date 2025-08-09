"""
Health monitoring and checks for Trading Station.
Tracks job execution, API usage, and system status.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum

from utils.config import SYSTEM_DIR, HEALTHCHECK_DEGRADED_AFTER_MINS, HEALTHCHECK_FAIL_AFTER_MINS
from utils.logging_setup import get_logger
from utils.storage import get_storage
from utils.time_utils import now_utc, now_et

logger = get_logger(__name__)

class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UNKNOWN = "unknown"

@dataclass
class JobHealth:
    """Health information for a job."""
    job_name: str
    last_run: Optional[datetime]
    last_success: Optional[datetime]
    last_failure: Optional[datetime]
    total_runs: int
    success_count: int
    failure_count: int
    avg_duration_ms: float
    status: HealthStatus
    message: str

@dataclass
class SystemHealth:
    """Overall system health."""
    timestamp: datetime
    overall_status: HealthStatus
    jobs: Dict[str, JobHealth]
    api_usage: Dict[str, Any]
    storage_status: Dict[str, Any]
    uptime_minutes: float

class HealthChecker:
    """Monitors and tracks system health."""
    
    def __init__(self):
        self.storage = get_storage()
        self.health_file_path = f"{SYSTEM_DIR}/health.json"
        self.heartbeat_file_path = f"{SYSTEM_DIR}/heartbeat.json"
        
        # Create directory if needed
        Path(self.health_file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing health data
        self.health_data = self._load_health_data()
        self.start_time = now_utc()
    
    def _load_health_data(self) -> Dict[str, Any]:
        """Load existing health data from storage."""
        try:
            if self.storage.exists(self.health_file_path):
                with open(self.health_file_path, 'r') as f:
                    data = json.load(f)
                    # Convert timestamp strings back to datetime objects
                    self._convert_timestamps_from_str(data)
                    return data
            else:
                return {"jobs": {}, "system_start": now_utc().isoformat()}
        except Exception as e:
            logger.warning(f"Failed to load health data: {e}")
            return {"jobs": {}, "system_start": now_utc().isoformat()}
    
    def _convert_timestamps_from_str(self, data: Dict[str, Any]):
        """Convert timestamp strings back to datetime objects."""
        if "jobs" in data:
            for job_data in data["jobs"].values():
                for field in ["last_run", "last_success", "last_failure"]:
                    if job_data.get(field):
                        try:
                            job_data[field] = datetime.fromisoformat(job_data[field])
                        except:
                            job_data[field] = None
    
    def _save_health_data(self):
        """Save health data to storage."""
        try:
            # Convert datetime objects to strings for JSON serialization
            data_to_save = self._prepare_data_for_json(self.health_data)
            
            with open(self.health_file_path, 'w') as f:
                json.dump(data_to_save, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to save health data: {e}")
    
    def _prepare_data_for_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for JSON serialization."""
        json_data = {}
        for key, value in data.items():
            if key == "jobs":
                json_data[key] = {}
                for job_name, job_data in value.items():
                    json_data[key][job_name] = {}
                    for field, field_value in job_data.items():
                        if isinstance(field_value, datetime):
                            json_data[key][job_name][field] = field_value.isoformat()
                        else:
                            json_data[key][job_name][field] = field_value
            else:
                json_data[key] = value
        return json_data
    
    def record_job_start(self, job_name: str, run_id: str = None):
        """Record a job start."""
        now = now_utc()
        
        if job_name not in self.health_data["jobs"]:
            self.health_data["jobs"][job_name] = {
                "job_name": job_name,
                "last_run": None,
                "last_success": None,
                "last_failure": None,
                "total_runs": 0,
                "success_count": 0,
                "failure_count": 0,
                "total_duration_ms": 0,
                "avg_duration_ms": 0,
                "status": HealthStatus.UNKNOWN.value,
                "message": "Starting...",
                "current_run_start": None,
                "run_id": None
            }
        
        job_data = self.health_data["jobs"][job_name]
        job_data["last_run"] = now
        job_data["total_runs"] += 1
        job_data["current_run_start"] = now
        job_data["run_id"] = run_id
        job_data["status"] = "running"
        job_data["message"] = f"Started at {now.strftime('%H:%M:%S')}"
        
        self._save_health_data()
        logger.debug(f"Recorded job start: {job_name}")
    
    def record_job_complete(self, job_name: str, success: bool, duration_ms: float, message: str = ""):
        """Record a job completion."""
        now = now_utc()
        
        if job_name not in self.health_data["jobs"]:
            logger.warning(f"Job {job_name} not found in health data")
            return
        
        job_data = self.health_data["jobs"][job_name]
        
        if success:
            job_data["last_success"] = now
            job_data["success_count"] += 1
            job_data["status"] = HealthStatus.HEALTHY.value
        else:
            job_data["last_failure"] = now
            job_data["failure_count"] += 1
            job_data["status"] = HealthStatus.FAILED.value
        
        # Update duration statistics
        job_data["total_duration_ms"] = job_data.get("total_duration_ms", 0) + duration_ms
        job_data["avg_duration_ms"] = job_data["total_duration_ms"] / job_data["total_runs"]
        
        job_data["message"] = message or (
            f"Completed successfully in {duration_ms:.1f}ms" if success else 
            f"Failed after {duration_ms:.1f}ms"
        )
        
        # Clear current run tracking
        job_data.pop("current_run_start", None)
        job_data.pop("run_id", None)
        
        self._save_health_data()
        logger.debug(f"Recorded job completion: {job_name} - {'SUCCESS' if success else 'FAILURE'}")
    
    def check_job_health(self, job_name: str) -> HealthStatus:
        """Check the health status of a specific job."""
        if job_name not in self.health_data["jobs"]:
            return HealthStatus.UNKNOWN
        
        job_data = self.health_data["jobs"][job_name]
        now = now_utc()
        
        # If job is currently running, check if it's stuck
        if job_data.get("current_run_start"):
            run_duration = (now - job_data["current_run_start"]).total_seconds() / 60
            if run_duration > HEALTHCHECK_FAIL_AFTER_MINS:
                return HealthStatus.FAILED
            elif run_duration > HEALTHCHECK_DEGRADED_AFTER_MINS:
                return HealthStatus.DEGRADED
            else:
                return HealthStatus.HEALTHY  # Running normally
        
        # Check when last run was
        last_run = job_data.get("last_run")
        if not last_run:
            return HealthStatus.UNKNOWN
        
        minutes_since_last_run = (now - last_run).total_seconds() / 60
        
        # Determine expected frequency based on job name
        expected_interval_minutes = self._get_expected_interval(job_name)
        
        if minutes_since_last_run > expected_interval_minutes * 3:  # 3x expected interval
            return HealthStatus.FAILED
        elif minutes_since_last_run > expected_interval_minutes * 2:  # 2x expected interval
            return HealthStatus.DEGRADED
        else:
            # Check recent success rate
            recent_success_rate = self._get_recent_success_rate(job_name)
            if recent_success_rate < 0.5:  # Less than 50% success
                return HealthStatus.FAILED
            elif recent_success_rate < 0.8:  # Less than 80% success
                return HealthStatus.DEGRADED
            else:
                return HealthStatus.HEALTHY
    
    def _get_expected_interval(self, job_name: str) -> int:
        """Get expected interval in minutes for a job."""
        intervals = {
            "update_intraday": 1,           # Every minute
            "update_intraday_30min": 10,    # Every 10 minutes
            "gapgo": 1,                     # Every minute during first hour
            "orb": 1440,                    # Once per day
            "avwap": 15,                    # Every 15 minutes
            "breakout": 15,                 # Every 15 minutes
            "ema_pullback": 15,             # Every 15 minutes
            "exhaustion": 15,               # Every 15 minutes
            "update_daily": 1440,           # Once per day
            "find_avwap_anchors": 1440,     # Once per day
            "opportunity_ticker_finder": 1440, # Once per day
            "master_dashboard": 5,          # Every 5 minutes
        }
        return intervals.get(job_name, 60)  # Default to 1 hour
    
    def _get_recent_success_rate(self, job_name: str) -> float:
        """Get recent success rate for a job (simplified calculation)."""
        job_data = self.health_data["jobs"].get(job_name, {})
        
        total_runs = job_data.get("total_runs", 0)
        success_count = job_data.get("success_count", 0)
        
        if total_runs == 0:
            return 1.0  # No data, assume healthy
        
        return success_count / total_runs
    
    def get_system_health(self) -> SystemHealth:
        """Get overall system health."""
        now = now_utc()
        
        # Check all job healths
        job_healths = {}
        worst_status = HealthStatus.HEALTHY
        
        for job_name in self.health_data["jobs"]:
            job_status = self.check_job_health(job_name)
            job_data = self.health_data["jobs"][job_name]
            
            job_health = JobHealth(
                job_name=job_name,
                last_run=job_data.get("last_run"),
                last_success=job_data.get("last_success"),
                last_failure=job_data.get("last_failure"),
                total_runs=job_data.get("total_runs", 0),
                success_count=job_data.get("success_count", 0),
                failure_count=job_data.get("failure_count", 0),
                avg_duration_ms=job_data.get("avg_duration_ms", 0),
                status=job_status,
                message=job_data.get("message", "")
            )
            
            job_healths[job_name] = job_health
            
            # Track worst status
            if job_status == HealthStatus.FAILED:
                worst_status = HealthStatus.FAILED
            elif job_status == HealthStatus.DEGRADED and worst_status != HealthStatus.FAILED:
                worst_status = HealthStatus.DEGRADED
        
        # Calculate uptime
        system_start = datetime.fromisoformat(self.health_data.get("system_start", now.isoformat()))
        uptime_minutes = (now - system_start).total_seconds() / 60
        
        # Mock API and storage status for now
        api_usage = {"rate_limit_remaining": 75, "calls_this_minute": 2}
        storage_status = {"local_available": True, "cloud_available": True}
        
        return SystemHealth(
            timestamp=now,
            overall_status=worst_status,
            jobs=job_healths,
            api_usage=api_usage,
            storage_status=storage_status,
            uptime_minutes=uptime_minutes
        )
    
    def write_heartbeat(self):
        """Write a heartbeat file to indicate system is alive."""
        try:
            heartbeat_data = {
                "timestamp": now_utc().isoformat(),
                "status": "alive",
                "pid": os.getpid()
            }
            
            with open(self.heartbeat_file_path, 'w') as f:
                json.dump(heartbeat_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to write heartbeat: {e}")
    
    def is_system_healthy(self) -> bool:
        """Check if system is overall healthy."""
        system_health = self.get_system_health()
        return system_health.overall_status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]
    
    def get_health_summary(self) -> str:
        """Get a human-readable health summary."""
        system_health = self.get_system_health()
        
        summary_lines = [
            f"System Health: {system_health.overall_status.value.upper()}",
            f"Uptime: {system_health.uptime_minutes:.1f} minutes",
            f"Jobs monitored: {len(system_health.jobs)}",
            "",
            "Job Status:"
        ]
        
        for job_name, job_health in system_health.jobs.items():
            status_icon = {
                HealthStatus.HEALTHY: "✓",
                HealthStatus.DEGRADED: "⚠",
                HealthStatus.FAILED: "✗",
                HealthStatus.UNKNOWN: "?"
            }.get(job_health.status, "?")
            
            last_run_str = "Never" if not job_health.last_run else job_health.last_run.strftime("%H:%M:%S")
            
            summary_lines.append(
                f"  {status_icon} {job_name}: {job_health.status.value} "
                f"(last: {last_run_str}, success: {job_health.success_count}/{job_health.total_runs})"
            )
        
        return "\n".join(summary_lines)

# Global health checker instance
_health_checker: Optional[HealthChecker] = None

def get_health_checker() -> HealthChecker:
    """Get the global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker

# Convenience functions
def record_job_start(job_name: str, run_id: str = None):
    """Record a job start."""
    get_health_checker().record_job_start(job_name, run_id)

def record_job_complete(job_name: str, success: bool, duration_ms: float, message: str = ""):
    """Record a job completion."""
    get_health_checker().record_job_complete(job_name, success, duration_ms, message)

def write_heartbeat():
    """Write system heartbeat."""
    get_health_checker().write_heartbeat()

def get_system_health() -> SystemHealth:
    """Get system health."""
    return get_health_checker().get_system_health()

# Export classes and functions
__all__ = [
    'HealthStatus', 'JobHealth', 'SystemHealth', 'HealthChecker',
    'get_health_checker', 'record_job_start', 'record_job_complete',
    'write_heartbeat', 'get_system_health'
]