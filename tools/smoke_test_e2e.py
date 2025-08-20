#!/usr/bin/env python3
"""
End-to-end smoke test for the TradingStation system.
"""

import os
import sys
import time
import subprocess
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

# Add parent directory to path for imports  
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import config
from utils.spaces_io import SpacesIO
from utils.logging_setup import get_logger


class SmokeTestRunner:
    """End-to-end smoke test for system functionality."""
    
    def __init__(self):
        self.logger = get_logger("smoke_test")
        self.base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.test_results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_status": "UNKNOWN",
            "tests": {}
        }
    
    def run_command(self, command: str, timeout: int = 120) -> Tuple[bool, str, str]:
        """Run a shell command and return success, stdout, stderr."""
        try:
            self.logger.info(f"Running command: {command}")
            
            result = subprocess.run(
                command,
                shell=True,
                cwd=self.base_path,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            success = result.returncode == 0
            return success, result.stdout, result.stderr
            
        except subprocess.TimeoutExpired:
            return False, "", f"Command timed out after {timeout} seconds"
        except Exception as e:
            return False, "", str(e)
    
    def test_data_fetch_manager(self) -> Dict[str, Any]:
        """Test data fetch manager with limited scope."""
        test_result = {
            "status": "UNKNOWN",
            "tests": {},
            "errors": []
        }
        
        # Test universe update
        success, stdout, stderr = self.run_command(
            "python3 jobs/data_fetch_manager.py --job universe --test-mode"
        )
        
        test_result["tests"]["universe_fetch"] = {
            "success": success,
            "stdout_lines": len(stdout.split('\n')) if stdout else 0,
            "error": stderr if not success else None
        }
        
        if not success:
            test_result["errors"].append(f"Universe fetch failed: {stderr}")
        
        # Test daily data fetch for a single symbol
        success, stdout, stderr = self.run_command(
            "python3 jobs/data_fetch_manager.py --job daily --tickers AAPL --test-mode"
        )
        
        test_result["tests"]["daily_fetch"] = {
            "success": success,
            "stdout_lines": len(stdout.split('\n')) if stdout else 0,
            "error": stderr if not success else None
        }
        
        if not success:
            test_result["errors"].append(f"Daily fetch failed: {stderr}")
        
        # Determine overall test status
        all_passed = all(t["success"] for t in test_result["tests"].values())
        test_result["status"] = "PASS" if all_passed else "FAIL"
        
        return test_result
    
    def test_screeners(self) -> Dict[str, Any]:
        """Test all screeners with limited scope."""
        test_result = {
            "status": "UNKNOWN",
            "tests": {},
            "errors": []
        }
        
        screeners = [
            ("gapgo", "python3 screeners/gapgo.py --test-mode --tickers AAPL"),
            ("orb", "python3 screeners/orb.py --test-mode --tickers AAPL"),
            ("avwap_reclaim", "python3 screeners/avwap_reclaim.py --test-mode --tickers AAPL"),
            ("breakout", "python3 screeners/breakout.py --test-mode --tickers AAPL"),
            ("ema_pullback", "python3 screeners/ema_pullback.py --test-mode --tickers AAPL"),
            ("exhaustion_reversal", "python3 screeners/exhaustion_reversal.py --test-mode --tickers AAPL")
        ]
        
        for screener_name, command in screeners:
            success, stdout, stderr = self.run_command(command, timeout=60)
            
            test_result["tests"][screener_name] = {
                "success": success,
                "stdout_lines": len(stdout.split('\n')) if stdout else 0,
                "error": stderr if not success else None
            }
            
            if not success:
                test_result["errors"].append(f"{screener_name} screener failed: {stderr}")
        
        # Determine overall test status
        all_passed = all(t["success"] for t in test_result["tests"].values())
        test_result["status"] = "PASS" if all_passed else "FAIL"
        
        return test_result
    
    def test_dashboard(self) -> Dict[str, Any]:
        """Test dashboard generation."""
        test_result = {
            "status": "UNKNOWN",
            "tests": {},
            "errors": []
        }
        
        # Test master dashboard
        success, stdout, stderr = self.run_command(
            "python3 dashboard/master_dashboard.py --test-mode --hours-lookback 24"
        )
        
        test_result["tests"]["master_dashboard"] = {
            "success": success,
            "stdout_lines": len(stdout.split('\n')) if stdout else 0,
            "error": stderr if not success else None
        }
        
        if not success:
            test_result["errors"].append(f"Master dashboard failed: {stderr}")
        
        test_result["status"] = "PASS" if success else "FAIL"
        return test_result
    
    def test_orchestrator(self) -> Dict[str, Any]:
        """Test orchestrator dry run."""
        test_result = {
            "status": "UNKNOWN", 
            "tests": {},
            "errors": []
        }
        
        # Test orchestrator dry run for each mode
        modes = ["premarket", "market", "postmarket", "daily"]
        
        for mode in modes:
            success, stdout, stderr = self.run_command(
                f"python3 orchestrator/run_all.py --mode {mode} --dry-run"
            )
            
            test_result["tests"][f"orchestrator_{mode}"] = {
                "success": success,
                "stdout_lines": len(stdout.split('\n')) if stdout else 0,
                "shows_jobs": "Would execute:" in stdout if stdout else False,
                "error": stderr if not success else None
            }
            
            if not success:
                test_result["errors"].append(f"Orchestrator {mode} mode failed: {stderr}")
        
        # Determine overall test status
        all_passed = all(t["success"] for t in test_result["tests"].values())
        test_result["status"] = "PASS" if all_passed else "FAIL"
        
        return test_result
    
    def test_utilities(self) -> Dict[str, Any]:
        """Test utility tools."""
        test_result = {
            "status": "UNKNOWN",
            "tests": {},
            "errors": []
        }
        
        # Test health check
        success, stdout, stderr = self.run_command(
            "python3 tools/health_check.py --json"
        )
        
        test_result["tests"]["health_check"] = {
            "success": success,
            "stdout_lines": len(stdout.split('\n')) if stdout else 0,
            "json_output": "timestamp" in stdout if stdout else False,
            "error": stderr if not success else None
        }
        
        if not success:
            test_result["errors"].append(f"Health check failed: {stderr}")
        
        # Test deployment verification
        success, stdout, stderr = self.run_command(
            "python3 tools/verify_deployment.py --json"
        )
        
        test_result["tests"]["deployment_verification"] = {
            "success": success,
            "stdout_lines": len(stdout.split('\n')) if stdout else 0,
            "json_output": "overall_status" in stdout if stdout else False,
            "error": stderr if not success else None
        }
        
        if not success:
            test_result["errors"].append(f"Deployment verification failed: {stderr}")
        
        # Determine overall test status
        all_passed = all(t["success"] for t in test_result["tests"].values())
        test_result["status"] = "PASS" if all_passed else "FAIL"
        
        return test_result
    
    def test_data_integrity(self) -> Dict[str, Any]:
        """Test basic data integrity checks."""
        test_result = {
            "status": "UNKNOWN",
            "tests": {},
            "errors": []
        }
        
        try:
            spaces = SpacesIO()
            
            # Test connection to Spaces
            test_result["tests"]["spaces_connection"] = {
                "success": True,
                "error": None
            }
            
            # Test basic file operations
            test_content = f"Smoke test - {datetime.now(timezone.utc).isoformat()}"
            test_key = "test/smoke_test.txt"
            
            # Upload test
            upload_success = spaces.upload_text(test_content, test_key)
            test_result["tests"]["spaces_upload"] = {
                "success": upload_success,
                "error": None if upload_success else "Upload failed"
            }
            
            if upload_success:
                # Download test
                downloaded = spaces.download_text(test_key)
                download_success = downloaded == test_content
                test_result["tests"]["spaces_download"] = {
                    "success": download_success,
                    "error": None if download_success else "Content mismatch"
                }
                
                # Cleanup
                spaces.delete_file(test_key)
            else:
                test_result["errors"].append("Could not test download due to upload failure")
            
        except Exception as e:
            test_result["tests"]["spaces_connection"] = {
                "success": False,
                "error": str(e)
            }
            test_result["errors"].append(f"Spaces connectivity test failed: {e}")
        
        # Determine overall test status
        all_passed = all(t["success"] for t in test_result["tests"].values())
        test_result["status"] = "PASS" if all_passed else "FAIL"
        
        return test_result
    
    def run_full_smoke_test(self) -> Dict[str, Any]:
        """Run complete smoke test suite."""
        self.logger.info("Starting end-to-end smoke test")
        
        test_suites = [
            ("data_integrity", self.test_data_integrity),
            ("data_fetch_manager", self.test_data_fetch_manager),
            ("screeners", self.test_screeners),
            ("dashboard", self.test_dashboard),
            ("orchestrator", self.test_orchestrator),
            ("utilities", self.test_utilities)
        ]
        
        passed_suites = 0
        total_suites = len(test_suites)
        
        for suite_name, test_func in test_suites:
            self.logger.info(f"Running test suite: {suite_name}")
            
            try:
                start_time = time.time()
                result = test_func()
                execution_time = time.time() - start_time
                
                result["execution_time_seconds"] = round(execution_time, 2)
                self.test_results["tests"][suite_name] = result
                
                if result["status"] == "PASS":
                    passed_suites += 1
                    self.logger.info(f"✅ {suite_name}: PASSED ({execution_time:.1f}s)")
                else:
                    self.logger.error(f"❌ {suite_name}: FAILED ({execution_time:.1f}s)")
                    
            except Exception as e:
                self.logger.error(f"❌ {suite_name}: ERROR - {e}")
                self.test_results["tests"][suite_name] = {
                    "status": "ERROR",
                    "error": str(e),
                    "execution_time_seconds": 0
                }
        
        # Determine overall status
        if passed_suites == total_suites:
            self.test_results["overall_status"] = "PASS"
        elif passed_suites >= total_suites * 0.7:  # 70% pass rate
            self.test_results["overall_status"] = "PARTIAL"
        else:
            self.test_results["overall_status"] = "FAIL"
        
        self.test_results["passed_suites"] = passed_suites
        self.test_results["total_suites"] = total_suites
        
        self.logger.info(f"Smoke test complete: {self.test_results['overall_status']}")
        return self.test_results
    
    def print_smoke_test_report(self):
        """Print formatted smoke test report."""
        results = self.test_results
        
        print("\n" + "="*70)
        print("TRADINGSTATION END-TO-END SMOKE TEST REPORT")
        print("="*70)
        print(f"Timestamp: {results['timestamp']}")
        print(f"Overall Status: {results['overall_status']}")
        print(f"Passed Suites: {results['passed_suites']}/{results['total_suites']}")
        print("-"*70)
        
        for suite_name, result in results["tests"].items():
            status = result.get("status", "UNKNOWN")
            execution_time = result.get("execution_time_seconds", 0)
            status_icon = "✅" if status == "PASS" else "⚠️" if status == "PARTIAL" else "❌"
            
            print(f"\n{status_icon} {suite_name.replace('_', ' ').title()}: {status} ({execution_time:.1f}s)")
            
            if "tests" in result:
                for test_name, test_details in result["tests"].items():
                    test_success = test_details.get("success", False)
                    test_icon = "  ✅" if test_success else "  ❌"
                    print(f"{test_icon} {test_name}")
            
            if "errors" in result and result["errors"]:
                print(f"   Errors: {len(result['errors'])}")
                for error in result["errors"][:3]:  # Show first 3 errors
                    print(f"     - {error}")
        
        print("\n" + "="*70)
        if results["overall_status"] == "PASS":
            print("🎉 ALL SMOKE TESTS PASSED")
            print("System is functioning correctly end-to-end.")
        elif results["overall_status"] == "PARTIAL":
            print("⚠️ PARTIAL SUCCESS")
            print("Some components are working, but issues detected.")
        else:
            print("❌ SMOKE TESTS FAILED") 
            print("Critical system issues detected.")
        print("="*70)


def main():
    """Main entry point."""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="TradingStation Smoke Test")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    runner = SmokeTestRunner()
    results = runner.run_full_smoke_test()
    
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        runner.print_smoke_test_report()
    
    # Exit with appropriate code
    exit_codes = {"PASS": 0, "PARTIAL": 1, "FAIL": 2}
    sys.exit(exit_codes.get(results["overall_status"], 2))


if __name__ == "__main__":
    main()