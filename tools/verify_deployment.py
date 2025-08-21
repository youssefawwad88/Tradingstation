#!/usr/bin/env python3
"""
Deployment verification tool to ensure system is properly configured.
"""

import os
import sys
import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from utils.spaces_io import SpacesIO
from utils.providers.router import health_check as provider_health_check
from utils.logging_setup import get_logger


class DeploymentVerifier:
    """Comprehensive deployment verification."""
    
    def __init__(self):
        self.logger = get_logger("deployment_verifier")
        self.results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_status": "UNKNOWN",
            "checks": {}
        }
    
    def check_environment_variables(self) -> Tuple[bool, Dict[str, Any]]:
        """Verify all required environment variables are set."""
        required_vars = [
            "MARKETDATA_TOKEN",
            "SPACES_ACCESS_KEY_ID", 
            "SPACES_SECRET_ACCESS_KEY",
            "SPACES_REGION",
            "SPACES_BUCKET_NAME",
            "SPACES_ENDPOINT",
            "APP_ENV"
        ]
        
        results = {
            "status": "PASS",
            "missing_vars": [],
            "present_vars": [],
            "details": {}
        }
        
        for var in required_vars:
            value = os.getenv(var)
            if value:
                results["present_vars"].append(var)
                # Mask sensitive values
                if "SECRET" in var or "KEY" in var:
                    results["details"][var] = f"{value[:8]}..." if len(value) > 8 else "***"
                else:
                    results["details"][var] = value
            else:
                results["missing_vars"].append(var)
        
        if results["missing_vars"]:
            results["status"] = "FAIL"
            
        return results["status"] == "PASS", results
    
    def check_marketdata_connectivity(self) -> Tuple[bool, Dict[str, Any]]:
        """Test MarketData API connectivity and health."""
        results = {
            "status": "UNKNOWN",
            "api_key_valid": False,
            "test_call_success": False,
            "provider": "marketdata",
            "error": None
        }
        
        try:
            is_healthy, status_msg = provider_health_check()
            
            if is_healthy:
                results["api_key_valid"] = True
                results["test_call_success"] = True
                results["status"] = "PASS"
                results["message"] = status_msg
            else:
                results["status"] = "FAIL"
                results["error"] = f"Health check failed: {status_msg}"
                
        except Exception as e:
            results["status"] = "FAIL"
            results["error"] = str(e)
        
        return results["status"] == "PASS", results
    
    def check_spaces_connectivity(self) -> Tuple[bool, Dict[str, Any]]:
        """Test DigitalOcean Spaces connectivity and permissions."""
        results = {
            "status": "UNKNOWN",
            "connection_success": False,
            "read_access": False,
            "write_access": False,
            "bucket_exists": False,
            "error": None
        }
        
        try:
            spaces = SpacesIO()
            
            # Test connection and bucket access
            test_key = "test/deployment_verification.txt"
            test_content = f"Deployment verification test - {datetime.now(timezone.utc).isoformat()}"
            
            # Test write
            if spaces.upload_text(test_content, test_key):
                results["write_access"] = True
                results["connection_success"] = True
                
                # Test read
                downloaded = spaces.download_text(test_key)
                if downloaded == test_content:
                    results["read_access"] = True
                    
                # Cleanup test file
                spaces.delete_file(test_key)
            
            if results["connection_success"] and results["read_access"] and results["write_access"]:
                results["status"] = "PASS"
                results["bucket_exists"] = True
            else:
                results["status"] = "FAIL"
                
        except Exception as e:
            results["status"] = "FAIL"
            results["error"] = str(e)
        
        return results["status"] == "PASS", results
    
    def check_data_structure(self) -> Tuple[bool, Dict[str, Any]]:
        """Verify data structure exists in Spaces."""
        results = {
            "status": "UNKNOWN",
            "required_paths": [],
            "missing_paths": [],
            "existing_files": {},
            "error": None
        }
        
        required_paths = [
            "data/universe/",
            "data/daily/",
            "data/intraday/1min/",
            "data/intraday/30min/",
            "data/signals/",
            "data/avwap_anchors/",
            "data/dashboard/",
            "data/manifest/"
        ]
        
        try:
            spaces = SpacesIO()
            
            for path in required_paths:
                results["required_paths"].append(path)
                
                # Check if path exists by trying to list files
                try:
                    files = spaces.list_files(path)
                    results["existing_files"][path] = len(files) if files else 0
                except:
                    results["missing_paths"].append(path)
            
            if not results["missing_paths"]:
                results["status"] = "PASS"
            else:
                results["status"] = "PARTIAL"  # Some paths missing but not critical
                
        except Exception as e:
            results["status"] = "FAIL"
            results["error"] = str(e)
        
        return results["status"] in ["PASS", "PARTIAL"], results
    
    def check_system_components(self) -> Tuple[bool, Dict[str, Any]]:
        """Verify all system components are present."""
        results = {
            "status": "UNKNOWN",
            "components": {},
            "missing_components": [],
            "error": None
        }
        
        component_paths = {
            "data_fetch_manager": "jobs/data_fetch_manager.py",
            "find_avwap_anchors": "jobs/find_avwap_anchors.py",
            "backfill_rebuilder": "jobs/backfill_rebuilder.py",
            "gapgo_screener": "screeners/gapgo.py",
            "orb_screener": "screeners/orb.py",
            "avwap_reclaim_screener": "screeners/avwap_reclaim.py",
            "breakout_screener": "screeners/breakout.py",
            "ema_pullback_screener": "screeners/ema_pullback.py",
            "exhaustion_reversal_screener": "screeners/exhaustion_reversal.py",
            "master_dashboard": "dashboard/master_dashboard.py",
            "orchestrator": "orchestrator/run_all.py",
            "health_check": "tools/health_check.py"
        }
        
        try:
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            for component, rel_path in component_paths.items():
                full_path = os.path.join(base_path, rel_path)
                exists = os.path.exists(full_path)
                results["components"][component] = {
                    "path": rel_path,
                    "exists": exists,
                    "executable": os.access(full_path, os.X_OK) if exists else False
                }
                
                if not exists:
                    results["missing_components"].append(component)
            
            if not results["missing_components"]:
                results["status"] = "PASS"
            else:
                results["status"] = "FAIL"
                
        except Exception as e:
            results["status"] = "FAIL"
            results["error"] = str(e)
        
        return results["status"] == "PASS", results
    
    def run_verification(self) -> Dict[str, Any]:
        """Run all verification checks."""
        self.logger.info("Starting deployment verification")
        
        checks = [
            ("environment_variables", self.check_environment_variables),
            ("marketdata_connectivity", self.check_marketdata_connectivity),
            ("spaces_connectivity", self.check_spaces_connectivity), 
            ("data_structure", self.check_data_structure),
            ("system_components", self.check_system_components)
        ]
        
        all_passed = True
        
        for check_name, check_func in checks:
            self.logger.info(f"Running check: {check_name}")
            try:
                passed, details = check_func()
                self.results["checks"][check_name] = details
                
                if not passed:
                    all_passed = False
                    self.logger.error(f"Check failed: {check_name}")
                else:
                    self.logger.info(f"Check passed: {check_name}")
                    
            except Exception as e:
                self.logger.error(f"Check error: {check_name} - {e}")
                self.results["checks"][check_name] = {
                    "status": "ERROR",
                    "error": str(e)
                }
                all_passed = False
        
        self.results["overall_status"] = "PASS" if all_passed else "FAIL"
        self.logger.info(f"Verification complete: {self.results['overall_status']}")
        
        return self.results
    
    def print_verification_report(self):
        """Print a formatted verification report."""
        results = self.results
        
        print("\n" + "="*70)
        print("TRADINGSTATION DEPLOYMENT VERIFICATION REPORT")
        print("="*70)
        print(f"Timestamp: {results['timestamp']}")
        print(f"Overall Status: {results['overall_status']}")
        print("-"*70)
        
        for check_name, check_results in results["checks"].items():
            status = check_results.get("status", "UNKNOWN")
            status_icon = "✅" if status == "PASS" else "⚠️" if status == "PARTIAL" else "❌"
            
            print(f"\n{status_icon} {check_name.replace('_', ' ').title()}: {status}")
            
            if status == "FAIL" and "error" in check_results:
                print(f"   Error: {check_results['error']}")
            
            # Show specific details for some checks
            if check_name == "environment_variables":
                if check_results.get("missing_vars"):
                    print(f"   Missing: {', '.join(check_results['missing_vars'])}")
                print(f"   Present: {len(check_results.get('present_vars', []))}/{len(check_results.get('present_vars', [])) + len(check_results.get('missing_vars', []))}")
            
            elif check_name == "system_components":
                missing = check_results.get("missing_components", [])
                if missing:
                    print(f"   Missing components: {', '.join(missing)}")
                total_components = len(check_results.get("components", {}))
                working_components = total_components - len(missing)
                print(f"   Components: {working_components}/{total_components} present")
        
        print("\n" + "="*70)
        if results["overall_status"] == "PASS":
            print("🎉 DEPLOYMENT VERIFICATION SUCCESSFUL")
            print("System is ready for operation.")
        else:
            print("❌ DEPLOYMENT VERIFICATION FAILED")
            print("Please fix the issues above before proceeding.")
        print("="*70)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Deployment Verification Tool")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    verifier = DeploymentVerifier()
    results = verifier.run_verification()
    
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        verifier.print_verification_report()
    
    # Exit with error code if verification failed
    sys.exit(0 if results["overall_status"] == "PASS" else 1)


if __name__ == "__main__":
    main()