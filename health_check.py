#!/usr/bin/env python3
"""
Health Check Endpoint for Deployment Validation
===============================================

This script provides a simple health check endpoint that can be used
to validate deployment success and verify the correct code version
is running in production.

Usage:
    python3 health_check.py
    python3 health_check.py --json
    
For production monitoring, this can be called periodically to ensure
the deployment is running the expected code version.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))


def get_health_status():
    """Get comprehensive health status for deployment validation."""
    health_data = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "checks": {},
        "deployment_info": {},
        "environment": {}
    }
    
    try:
        # Get deployment info
        try:
            commit_hash = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=os.path.dirname(__file__)
            ).decode().strip()
            
            branch = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=os.path.dirname(__file__)
            ).decode().strip()
            
            health_data["deployment_info"] = {
                "commit_hash": commit_hash,
                "short_hash": commit_hash[:8],
                "branch": branch
            }
            health_data["checks"]["git_info"] = True
            
        except Exception as e:
            health_data["checks"]["git_info"] = False
            health_data["deployment_info"]["error"] = str(e)
            
        # Check critical file existence
        critical_files = [
            "jobs/data_fetch_manager.py",
            "orchestrator/run_all.py",
            "utils/config.py"
        ]
        
        missing_files = []
        for file_path in critical_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
                
        health_data["checks"]["critical_files"] = len(missing_files) == 0
        if missing_files:
            health_data["missing_files"] = missing_files
            
        # Check critical log messages in data_fetch_manager.py
        data_fetch_file = "jobs/data_fetch_manager.py"
        if os.path.exists(data_fetch_file):
            with open(data_fetch_file, 'r') as f:
                content = f.read()
                
            critical_messages = [
                "--- Triggering 1-Minute Intraday Update Only ---",
                "📥 Downloading master_tickerlist.csv",
                "DEPLOYMENT"  # Our new deployment tracking
            ]
            
            messages_found = sum(1 for msg in critical_messages if msg in content)
            health_data["checks"]["hotfix_messages"] = messages_found >= 2
            health_data["hotfix_messages_found"] = f"{messages_found}/{len(critical_messages)}"
        else:
            health_data["checks"]["hotfix_messages"] = False
            
        # Check Python imports
        try:
            from jobs.data_fetch_manager import DataFetchManager
            health_data["checks"]["imports"] = True
        except ImportError as e:
            health_data["checks"]["imports"] = False
            health_data["import_error"] = str(e)
            
        # Environment info
        health_data["environment"] = {
            "python_version": sys.version.split()[0],
            "has_alpha_vantage_key": bool(os.getenv("ALPHA_VANTAGE_API_KEY")),
            "has_spaces_credentials": all([
                os.getenv("SPACES_ACCESS_KEY_ID"),
                os.getenv("SPACES_SECRET_ACCESS_KEY"),
                os.getenv("SPACES_BUCKET_NAME")
            ])
        }
        
        # Overall health status
        all_checks_passed = all(health_data["checks"].values())
        health_data["status"] = "healthy" if all_checks_passed else "degraded"
        
        # Add deployment validation signature
        deployment_hash = health_data["deployment_info"].get("short_hash", "unknown")
        health_data["deployment_signature"] = f"tradingstation-{deployment_hash}-{datetime.utcnow().strftime('%Y%m%d')}"
        
    except Exception as e:
        health_data["status"] = "error"
        health_data["error"] = str(e)
        
    return health_data


def main():
    parser = argparse.ArgumentParser(description="Health check for deployment validation")
    parser.add_argument("--json", action="store_true", help="Output JSON format")
    args = parser.parse_args()
    
    health_status = get_health_status()
    
    if args.json:
        print(json.dumps(health_status, indent=2))
    else:
        # Human readable format
        status_emoji = "✅" if health_status["status"] == "healthy" else "⚠️" if health_status["status"] == "degraded" else "❌"
        
        print(f"🏥 TRADINGSTATION HEALTH CHECK {status_emoji}")
        print(f"Status: {health_status['status'].upper()}")
        print(f"Timestamp: {health_status['timestamp']}")
        print()
        
        # Deployment info
        deployment_info = health_status.get("deployment_info", {})
        if "short_hash" in deployment_info:
            print(f"🚀 Deployment: {deployment_info['short_hash']} ({deployment_info['branch']})")
            print(f"🆔 Signature: {health_status.get('deployment_signature', 'unknown')}")
        else:
            print("🚀 Deployment: Unknown")
        print()
        
        # Checks
        print("🔍 Health Checks:")
        for check_name, passed in health_status["checks"].items():
            status = "✅" if passed else "❌"
            print(f"  {check_name.replace('_', ' ').title()}: {status}")
            
        # Environment
        env = health_status.get("environment", {})
        print("\n🌍 Environment:")
        print(f"  Python: {env.get('python_version', 'unknown')}")
        print(f"  Alpha Vantage API: {'✅' if env.get('has_alpha_vantage_key') else '❌'}")
        print(f"  Spaces Credentials: {'✅' if env.get('has_spaces_credentials') else '❌'}")
        
        # Hotfix validation
        if "hotfix_messages_found" in health_status:
            print(f"\n🔧 Hotfix Validation: {health_status['hotfix_messages_found']} critical messages found")
            
        # Errors
        if "error" in health_status:
            print(f"\n❌ Error: {health_status['error']}")
        if "missing_files" in health_status:
            print(f"\n❌ Missing Files: {', '.join(health_status['missing_files'])}")
        if "import_error" in health_status:
            print(f"\n❌ Import Error: {health_status['import_error']}")
            
    # Exit code based on health
    if health_status["status"] == "healthy":
        sys.exit(0)
    elif health_status["status"] == "degraded":
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()