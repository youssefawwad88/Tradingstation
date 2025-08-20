#!/usr/bin/env python3
"""
Deployment Verification Script
============================

This script helps verify that a clean deployment was successful by checking
that all components are running the expected versions.

Usage:
    python verify_deployment.py

This should be run after clearing the build cache and redeploying to ensure
all components are running the latest VERSION 2.0 code.
"""

import os
import subprocess
import sys
from datetime import datetime

def get_git_info():
    """Get current git commit information."""
    try:
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], 
            cwd=os.path.dirname(__file__)
        ).decode().strip()[:8]
        
        commit_message = subprocess.check_output(
            ["git", "log", "-1", "--pretty=format:%s"], 
            cwd=os.path.dirname(__file__)
        ).decode().strip()
        
        return commit_hash, commit_message
    except Exception as e:
        return "unknown", f"Error: {e}"

def test_orchestrator_version():
    """Test that orchestrator shows VERSION 2.0 message."""
    print("🔍 Testing Orchestrator Version...")
    try:
        result = subprocess.run(
            [sys.executable, "orchestrator/run_all.py", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Combine stdout and stderr for checking
        combined_output = result.stdout + result.stderr
        
        # Check if VERSION 2.0 message appears in output
        if "--- ORCHESTRATOR VERSION 2.0 RUNNING" in combined_output:
            print("✅ Orchestrator VERSION 2.0 confirmed")
            # Extract deployment info if present
            for line in combined_output.split('\n'):
                if "--- ORCHESTRATOR VERSION 2.0 RUNNING" in line:
                    print(f"   {line.strip()}")
                    break
            return True
        else:
            print("❌ Orchestrator VERSION 2.0 NOT detected")
            print("COMBINED OUTPUT:", combined_output[:500])
            return False
            
    except Exception as e:
        print(f"❌ Error testing orchestrator: {e}")
        return False

def test_data_fetch_manager_version():
    """Test that data fetch manager shows VERSION 2.0 message."""
    print("\n🔍 Testing Data Fetch Manager Version...")
    try:
        result = subprocess.run(
            [sys.executable, "jobs/data_fetch_manager.py", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Combine stdout and stderr for checking
        combined_output = result.stdout + result.stderr
        
        # Check if VERSION 2.0 message appears in output
        if "--- DATA FETCH MANAGER VERSION 2.0 RUNNING" in combined_output:
            print("✅ Data Fetch Manager VERSION 2.0 confirmed")
            # Extract deployment info if present
            for line in combined_output.split('\n'):
                if "--- DATA FETCH MANAGER VERSION 2.0 RUNNING" in line:
                    print(f"   {line.strip()}")
                    break
            return True
        else:
            print("❌ Data Fetch Manager VERSION 2.0 NOT detected")
            print("COMBINED OUTPUT:", combined_output[:500])
            return False
            
    except Exception as e:
        print(f"❌ Error testing data fetch manager: {e}")
        return False

def test_file_timestamps():
    """Check file modification timestamps to verify fresh deployment."""
    print("\n🔍 Checking File Timestamps...")
    
    key_files = [
        "orchestrator/run_all.py",
        "jobs/data_fetch_manager.py"
    ]
    
    for file_path in key_files:
        if os.path.exists(file_path):
            mtime = os.path.getmtime(file_path)
            mtime_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
            print(f"📁 {file_path}: {mtime_str}")
        else:
            print(f"❌ {file_path}: NOT FOUND")

def main():
    """Run all deployment verification tests."""
    print("=" * 60)
    print("DEPLOYMENT VERIFICATION SCRIPT")
    print("=" * 60)
    
    # Get git info
    commit_hash, commit_message = get_git_info()
    print(f"📝 Current Git Commit: {commit_hash}")
    print(f"📝 Commit Message: {commit_message}")
    print(f"🕐 Verification Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Run tests
    orchestrator_ok = test_orchestrator_version()
    data_manager_ok = test_data_fetch_manager_version()
    
    test_file_timestamps()
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    if orchestrator_ok and data_manager_ok:
        print("✅ SUCCESS: All components show VERSION 2.0")
        print("✅ Deployment verification PASSED")
        print("\n📋 Next Steps:")
        print("   1. Deploy to DigitalOcean (clear build cache first)")
        print("   2. Monitor runtime logs for all three diagnostic messages:")
        print("      - --- ORCHESTRATOR VERSION 2.0 RUNNING [DEPLOYMENT v...] ---")
        print("      - ORCHESTRATOR: Preparing to execute command: '...data_fetch_manager.py --interval 1min'")
        print("      - --- DATA FETCH MANAGER VERSION 2.0 RUNNING [DEPLOYMENT v...] ---")
        return True
    else:
        print("❌ FAILURE: Some components not showing VERSION 2.0")
        print("❌ Deployment verification FAILED")
        print("\n🔧 Troubleshooting:")
        print("   1. Check that code changes were committed properly")
        print("   2. Ensure all files have been updated")
        print("   3. Try running scripts directly to see error messages")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)