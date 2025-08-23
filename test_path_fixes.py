#!/usr/bin/env python3
"""Smoke test for the path handling fixes.

This test validates that:
1. Path building works correctly
2. Writer guards function properly
3. Tools are accessible and validate input
"""

import logging
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_path_building():
    """Test path building functionality."""
    print("Testing path building...")

    # Set test environment
    os.environ["SPACES_BASE_PREFIX"] = "trading-system"
    os.environ["DATA_ROOT"] = "data"
    os.environ["UNIVERSE_KEY"] = "data/Universe/master_tickerlist.csv"

    from utils.paths import daily_key, intraday_key, universe_key

    # Test path construction
    universe = universe_key()
    intraday = intraday_key("AAPL", "1min")
    daily = daily_key("AAPL")

    assert universe == "trading-system/data/Universe/master_tickerlist.csv"
    assert intraday == "trading-system/data/intraday/1min/AAPL.csv"
    assert daily == "trading-system/data/daily/AAPL.csv"

    print("✅ Path building works correctly")


def test_writer_guards():
    """Test writer guard functionality."""
    print("Testing writer guards...")

    os.environ["SPACES_BASE_PREFIX"] = "trading-system"

    # Suppress logging for cleaner test output
    logging.getLogger("utils.spaces_io").setLevel(logging.ERROR)

    from utils.spaces_io import _normalize_key

    # Test double-prefix fix
    result = _normalize_key("trading-system/trading-system/data/test.csv")
    assert result == "trading-system/data/test.csv"

    # Test missing base fix
    result = _normalize_key("data/test.csv")
    assert result == "trading-system/data/test.csv"

    # Test no change needed
    result = _normalize_key("trading-system/data/test.csv")
    assert result == "trading-system/data/test.csv"

    print("✅ Writer guards work correctly")


def test_tools_accessibility():
    """Test that tools are accessible and validate correctly."""
    print("Testing tools accessibility...")

    # Test repair tool
    import subprocess
    result = subprocess.run([
        sys.executable, "tools/repair_paths.py", "--help"
    ], capture_output=True, text=True, env=dict(os.environ, SPACES_BASE_PREFIX="trading-system"))

    assert result.returncode == 0
    assert "Repair S3 object paths" in result.stdout

    # Test inspect tool
    result = subprocess.run([
        sys.executable, "tools/inspect_spaces.py", "--help"
    ], capture_output=True, text=True)

    assert result.returncode == 0
    assert "--folders-only" in result.stdout

    print("✅ Tools are accessible and working")


def test_universe_regex():
    """Test universe regex fix."""
    print("Testing universe regex...")


    import pandas as pd

    from utils.universe import _extract_valid_tickers

    # Create test data with various symbols including hyphens and dots
    test_data = pd.DataFrame({
        'ticker': ['AAPL', 'BRK.B', 'SPY', 'VT-I', 'INVALID@', 'TOOLONG123456789']
    })

    valid_tickers = _extract_valid_tickers(test_data)

    # Should accept AAPL, BRK.B, SPY, VT-I but reject INVALID@ and TOOLONG123456789
    expected = ['AAPL', 'BRK.B', 'SPY', 'VT-I']
    assert valid_tickers == expected

    print("✅ Universe regex works correctly")


def main():
    """Run all smoke tests."""
    print("=" * 60)
    print("SMOKE TEST: Path Handling Fixes")
    print("=" * 60)

    try:
        test_path_building()
        test_writer_guards()
        test_tools_accessibility()
        test_universe_regex()

        print()
        print("=" * 60)
        print("🎉 ALL TESTS PASSED")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
