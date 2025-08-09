#!/usr/bin/env python3
"""
Simple health check script to validate deployment configuration.
This script verifies that the basic setup works before full deployment.
"""

import sys
import os
from pathlib import Path

def check_python_version():
    """Check Python version compatibility."""
    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major == 3 and version.minor >= 8:
        print("✅ Python version is compatible")
        return True
    else:
        print("❌ Python version should be 3.8 or higher")
        return False

def check_files():
    """Check that required files exist."""
    required_files = [
        '.python-version',
        'Procfile',
        'app.py',
        'requirements.txt'
    ]
    
    missing_files = []
    for file in required_files:
        if not Path(file).exists():
            missing_files.append(file)
        else:
            print(f"✅ {file} exists")
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    
    return True

def check_app_structure():
    """Check app structure."""
    required_dirs = ['utils', 'screeners', 'jobs', 'orchestrator']
    
    for dir_name in required_dirs:
        if Path(dir_name).exists():
            print(f"✅ {dir_name}/ directory exists")
        else:
            print(f"⚠️  {dir_name}/ directory missing (may be optional)")
    
    return True

def check_procfile():
    """Check Procfile format."""
    try:
        with open('Procfile', 'r') as f:
            content = f.read().strip()
        
        if content.startswith('web:') and 'streamlit' in content and '--server.address=0.0.0.0' in content:
            print("✅ Procfile format is correct")
            return True
        else:
            print(f"❌ Procfile format issue: {content}")
            return False
    except Exception as e:
        print(f"❌ Error reading Procfile: {e}")
        return False

def check_python_version_file():
    """Check .python-version file."""
    try:
        with open('.python-version', 'r') as f:
            version = f.read().strip()
        
        if version in ['3.8', '3.9', '3.10', '3.11']:
            print(f"✅ Python version file specifies: {version}")
            return True
        else:
            print(f"⚠️  Python version {version} may have compatibility issues")
            return True
    except Exception as e:
        print(f"❌ Error reading .python-version: {e}")
        return False

def main():
    """Run all health checks."""
    print("🔍 Running deployment health checks...\n")
    
    checks = [
        ("Python version", check_python_version),
        ("Required files", check_files),
        ("App structure", check_app_structure),
        ("Procfile format", check_procfile),
        ("Python version file", check_python_version_file)
    ]
    
    passed = 0
    total = len(checks)
    
    for name, check_func in checks:
        print(f"\n📋 {name}:")
        if check_func():
            passed += 1
        else:
            print(f"❌ {name} check failed")
    
    print(f"\n🎯 Health check summary: {passed}/{total} checks passed")
    
    if passed == total:
        print("✅ All deployment checks passed! Ready for deployment.")
        return 0
    else:
        print("❌ Some checks failed. Review the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())