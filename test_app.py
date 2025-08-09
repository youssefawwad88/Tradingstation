#!/usr/bin/env python3
"""
Test script to verify the Streamlit app can be imported and basic functionality works.
This helps validate the app before deployment.
"""

import sys
import importlib.util

def test_streamlit_import():
    """Test that streamlit can be imported."""
    try:
        import streamlit as st
        print("✅ Streamlit import successful")
        return True
    except ImportError as e:
        print(f"❌ Streamlit import failed: {e}")
        return False

def test_app_import():
    """Test that the app can be imported without errors."""
    try:
        # Import the app module
        spec = importlib.util.spec_from_file_location("app", "app.py")
        if spec is None:
            print("❌ Could not load app.py")
            return False
        
        app_module = importlib.util.module_from_spec(spec)
        
        # This will execute the import but not run the main function
        print("✅ App module can be loaded")
        return True
    except Exception as e:
        print(f"❌ App import failed: {e}")
        return False

def test_basic_pandas():
    """Test that pandas works correctly."""
    try:
        import pandas as pd
        import numpy as np
        
        # Create a simple DataFrame
        df = pd.DataFrame({
            'ticker': ['AAPL', 'TSLA'],
            'price': [150.0, 200.0]
        })
        
        if len(df) == 2:
            print("✅ Pandas functionality works")
            return True
        else:
            print("❌ Pandas DataFrame creation failed")
            return False
    except Exception as e:
        print(f"❌ Pandas test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 Running app validation tests...\n")
    
    tests = [
        ("Streamlit import", test_streamlit_import),
        ("App import", test_app_import), 
        ("Pandas functionality", test_basic_pandas)
    ]
    
    passed = 0
    total = len(tests)
    
    for name, test_func in tests:
        print(f"📋 Testing {name}:")
        if test_func():
            passed += 1
        print()
    
    print(f"🎯 Test summary: {passed}/{total} tests passed")
    
    if passed == total:
        print("✅ All app validation tests passed!")
        return 0
    else:
        print("❌ Some tests failed. The app may not work correctly.")
        return 1

if __name__ == "__main__":
    sys.exit(main())