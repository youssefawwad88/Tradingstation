#!/usr/bin/env python3
"""
Health check tool for monitoring system status and performance.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import config
from utils.spaces_io import SpacesIO
from utils.providers.router import health_check as provider_health_check
from utils.logging_setup import get_logger


class HealthChecker:
    """System health monitoring and diagnostics."""
    
    def __init__(self):
        self.logger = get_logger("health_checker")
        self.config = config
        self.spaces = SpacesIO()
        
    def check_api_limits(self) -> Dict[str, Any]:
        """Check MarketData API connectivity and health."""
        try:
            # Use the provider health check
            is_healthy, status_msg = provider_health_check()
            
            result = {
                "status": "healthy" if is_healthy else "degraded",
                "api_accessible": is_healthy,
                "last_test": datetime.now(timezone.utc).isoformat(),
                "provider": "marketdata",
                "message": status_msg,
            }
            
            if not is_healthy:
                result["issue"] = f"MarketData API issue: {status_msg}"
            
            return result
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "api_accessible": False,
                "error": str(e),
                "provider": "marketdata",
                "last_test": datetime.now(timezone.utc).isoformat()
            }
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check if data is fresh and up-to-date."""
        try:
            # Check fetch status manifest
            manifest = self.spaces.download_json("data/manifest/fetch_status.json")
            
            if not manifest:
                return {
                    "status": "unhealthy",
                    "issue": "No fetch status manifest found"
                }
            
            current_time = datetime.now(timezone.utc)
            freshness_results = {}
            overall_fresh = True
            
            # Check each data type
            data_types = ["universe", "daily", "intraday_1min", "intraday_30min"]
            
            for data_type in data_types:
                if data_type in manifest:
                    last_fetch_str = manifest[data_type].get("last_fetch")
                    if last_fetch_str:
                        last_fetch = datetime.fromisoformat(last_fetch_str.replace('Z', '+00:00'))
                        age_hours = (current_time - last_fetch).total_seconds() / 3600
                        
                        # Define freshness thresholds
                        thresholds = {
                            "universe": 24,      # Daily update
                            "daily": 24,         # Daily update
                            "intraday_1min": 1,  # Hourly during market
                            "intraday_30min": 4  # Every few hours
                        }
                        
                        is_fresh = age_hours <= thresholds[data_type]
                        if not is_fresh:
                            overall_fresh = False
                        
                        freshness_results[data_type] = {
                            "last_fetch": last_fetch_str,
                            "age_hours": round(age_hours, 2),
                            "threshold_hours": thresholds[data_type],
                            "fresh": is_fresh,
                            "status": manifest[data_type].get("status", "unknown")
                        }
                    else:
                        freshness_results[data_type] = {
                            "fresh": False,
                            "issue": "No last_fetch timestamp"
                        }
                        overall_fresh = False
            
            return {
                "status": "healthy" if overall_fresh else "degraded",
                "overall_fresh": overall_fresh,
                "data_types": freshness_results,
                "checked_at": current_time.isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def check_signals(self) -> Dict[str, Any]:
        """Check signal generation and quality."""
        try:
            signal_files = [
                "data/signals/gapgo.csv",
                "data/signals/orb.csv",
                "data/signals/avwap_reclaim.csv",
                "data/signals/breakout.csv",
                "data/signals/ema_pullback.csv",
                "data/signals/exhaustion_reversal.csv"
            ]
            
            signal_results = {}
            total_signals = 0
            active_screeners = 0
            
            for signal_file in signal_files:
                screener_name = os.path.basename(signal_file).replace('.csv', '')
                
                try:
                    df = self.spaces.download_csv(signal_file)
                    
                    if df is not None and not df.empty:
                        # Filter to recent signals (last 24 hours)
                        if 'timestamp_utc' in df.columns:
                            df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
                            cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
                            recent_signals = df[df['timestamp_utc'] >= cutoff]
                            
                            signal_count = len(recent_signals)
                            total_signals += signal_count
                            
                            if signal_count > 0:
                                active_screeners += 1
                                avg_score = recent_signals['score'].mean() if 'score' in recent_signals.columns else 0
                            else:
                                avg_score = 0
                        else:
                            signal_count = len(df)
                            total_signals += signal_count
                            avg_score = df['score'].mean() if 'score' in df.columns else 0
                    else:
                        signal_count = 0
                        avg_score = 0
                    
                    signal_results[screener_name] = {
                        "recent_signals": signal_count,
                        "avg_score": round(avg_score, 2) if avg_score > 0 else 0,
                        "status": "active" if signal_count > 0 else "inactive"
                    }
                    
                except Exception as e:
                    signal_results[screener_name] = {
                        "status": "error",
                        "error": str(e)
                    }
            
            return {
                "status": "healthy" if active_screeners > 0 else "degraded",
                "total_recent_signals": total_signals,
                "active_screeners": active_screeners,
                "screeners": signal_results,
                "checked_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def check_storage(self) -> Dict[str, Any]:
        """Check storage usage and health."""
        try:
            # Get file counts for different data types
            data_paths = {
                "universe": "data/universe/",
                "daily": "data/daily/",
                "intraday_1min": "data/intraday/1min/",
                "intraday_30min": "data/intraday/30min/",
                "signals": "data/signals/",
                "avwap_anchors": "data/avwap_anchors/",
                "dashboard": "data/dashboard/"
            }
            
            storage_results = {}
            total_files = 0
            
            for data_type, path in data_paths.items():
                try:
                    files = self.spaces.list_files(path)
                    file_count = len(files) if files else 0
                    total_files += file_count
                    
                    storage_results[data_type] = {
                        "file_count": file_count,
                        "status": "healthy" if file_count > 0 else "empty"
                    }
                    
                except Exception as e:
                    storage_results[data_type] = {
                        "status": "error",
                        "error": str(e)
                    }
            
            return {
                "status": "healthy" if total_files > 0 else "degraded",
                "total_files": total_files,
                "storage_breakdown": storage_results,
                "checked_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy", 
                "error": str(e)
            }
    
    def check_system_components(self) -> Dict[str, Any]:
        """Check if all system components are present and executable."""
        components = {
            "data_fetch_manager": "jobs/data_fetch_manager.py",
            "orchestrator": "orchestrator/run_all.py",
            "master_dashboard": "dashboard/master_dashboard.py",
            "screeners": [
                "screeners/gapgo.py",
                "screeners/orb.py", 
                "screeners/avwap_reclaim.py",
                "screeners/breakout.py",
                "screeners/ema_pullback.py",
                "screeners/exhaustion_reversal.py"
            ]
        }
        
        try:
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            component_results = {}
            all_healthy = True
            
            for component, paths in components.items():
                if isinstance(paths, str):
                    paths = [paths]
                
                component_status = []
                for path in paths:
                    full_path = os.path.join(base_path, path)
                    exists = os.path.exists(full_path)
                    executable = os.access(full_path, os.X_OK) if exists else False
                    
                    component_status.append({
                        "path": path,
                        "exists": exists,
                        "executable": executable,
                        "status": "healthy" if exists and executable else "unhealthy"
                    })
                    
                    if not (exists and executable):
                        all_healthy = False
                
                component_results[component] = component_status
            
            return {
                "status": "healthy" if all_healthy else "degraded",
                "components": component_results,
                "checked_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def run_comprehensive_health_check(self) -> Dict[str, Any]:
        """Run all health checks and return comprehensive status."""
        self.logger.info("Starting comprehensive health check")
        
        health_results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_status": "unknown",
            "checks": {}
        }
        
        # Define all checks
        checks = [
            ("api_limits", self.check_api_limits),
            ("data_freshness", self.check_data_freshness),
            ("signals", self.check_signals),
            ("storage", self.check_storage),
            ("system_components", self.check_system_components)
        ]
        
        healthy_count = 0
        total_checks = len(checks)
        
        for check_name, check_func in checks:
            self.logger.info(f"Running health check: {check_name}")
            try:
                result = check_func()
                health_results["checks"][check_name] = result
                
                if result.get("status") == "healthy":
                    healthy_count += 1
                    
            except Exception as e:
                self.logger.error(f"Health check failed: {check_name} - {e}")
                health_results["checks"][check_name] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
        
        # Determine overall status
        if healthy_count == total_checks:
            health_results["overall_status"] = "healthy"
        elif healthy_count >= total_checks * 0.7:  # 70% healthy
            health_results["overall_status"] = "degraded"
        else:
            health_results["overall_status"] = "unhealthy"
        
        health_results["healthy_checks"] = healthy_count
        health_results["total_checks"] = total_checks
        
        self.logger.info(f"Health check complete: {health_results['overall_status']}")
        return health_results
    
    def print_health_report(self, health_results: Dict[str, Any]):
        """Print formatted health report."""
        print("\n" + "="*60)
        print("TRADINGSTATION HEALTH CHECK REPORT")
        print("="*60)
        print(f"Timestamp: {health_results['timestamp']}")
        print(f"Overall Status: {health_results['overall_status'].upper()}")
        print(f"Healthy Checks: {health_results['healthy_checks']}/{health_results['total_checks']}")
        print("-"*60)
        
        for check_name, result in health_results["checks"].items():
            status = result.get("status", "unknown")
            status_icon = "✅" if status == "healthy" else "⚠️" if status == "degraded" else "❌"
            
            print(f"\n{status_icon} {check_name.replace('_', ' ').title()}: {status.upper()}")
            
            # Show specific details
            if "error" in result:
                print(f"   Error: {result['error']}")
            
            if check_name == "api_limits" and "api_accessible" in result:
                print(f"   API Accessible: {result['api_accessible']}")
            
            elif check_name == "data_freshness" and "data_types" in result:
                for data_type, details in result["data_types"].items():
                    fresh_icon = "✅" if details.get("fresh", False) else "❌"
                    age = details.get("age_hours", 0)
                    print(f"   {fresh_icon} {data_type}: {age:.1f}h old")
            
            elif check_name == "signals" and "screeners" in result:
                print(f"   Active Screeners: {result.get('active_screeners', 0)}")
                print(f"   Total Recent Signals: {result.get('total_recent_signals', 0)}")
            
            elif check_name == "storage" and "storage_breakdown" in result:
                print(f"   Total Files: {result.get('total_files', 0)}")
        
        print("\n" + "="*60)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="TradingStation Health Check")
    parser.add_argument("--check", choices=["api", "data", "signals", "storage", "components"], 
                       help="Run specific check only")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    checker = HealthChecker()
    
    if args.check:
        # Run specific check
        check_methods = {
            "api": checker.check_api_limits,
            "data": checker.check_data_freshness,
            "signals": checker.check_signals,
            "storage": checker.check_storage,
            "components": checker.check_system_components
        }
        
        if args.check in check_methods:
            result = check_methods[args.check]()
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                print(f"\n{args.check.title()} Check Result:")
                print(json.dumps(result, indent=2))
    else:
        # Run comprehensive check
        results = checker.run_comprehensive_health_check()
        
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            checker.print_health_report(results)
        
        # Exit with error code if unhealthy
        sys.exit(0 if results["overall_status"] in ["healthy", "degraded"] else 1)


if __name__ == "__main__":
    main()