#!/usr/bin/env python3
"""
Examples of using the Comprehensive Data Fetcher
==============================================

This script demonstrates how easy it is to fetch different types of data
by simply changing the configuration at the top of comprehensive_data_fetcher.py

Run these examples by editing the configuration in comprehensive_data_fetcher.py
and then running: python3 comprehensive_data_fetcher.py
"""


def show_examples():
    print("🚀 Comprehensive Data Fetcher Usage Examples")
    print("=" * 60)

    print("\n📝 Example 1: Fetch 1-minute intraday data for AAPL")
    print("   Edit comprehensive_data_fetcher.py configuration:")
    print("   TICKER_SYMBOL = 'AAPL'")
    print("   DATA_INTERVAL = '1min'")
    print("   DATA_TYPE = 'INTRADAY'")
    print("   FILE_SIZE_THRESHOLD_KB = 10")
    print("   Then run: python3 comprehensive_data_fetcher.py")

    print("\n📝 Example 2: Fetch 30-minute intraday data for TSLA")
    print("   Edit comprehensive_data_fetcher.py configuration:")
    print("   TICKER_SYMBOL = 'TSLA'")
    print("   DATA_INTERVAL = '30min'")
    print("   DATA_TYPE = 'INTRADAY'")
    print("   FILE_SIZE_THRESHOLD_KB = 10")
    print("   Then run: python3 comprehensive_data_fetcher.py")

    print("\n📝 Example 3: Fetch daily data for MSFT")
    print("   Edit comprehensive_data_fetcher.py configuration:")
    print("   TICKER_SYMBOL = 'MSFT'")
    print("   DATA_INTERVAL = '1min'  # (ignored for daily)")
    print("   DATA_TYPE = 'DAILY'")
    print("   FILE_SIZE_THRESHOLD_KB = 10")
    print("   Then run: python3 comprehensive_data_fetcher.py")

    print("\n📝 Example 4: Different file size threshold")
    print("   Edit comprehensive_data_fetcher.py configuration:")
    print("   TICKER_SYMBOL = 'GOOGL'")
    print("   DATA_INTERVAL = '1min'")
    print("   DATA_TYPE = 'INTRADAY'")
    print("   FILE_SIZE_THRESHOLD_KB = 5  # Lower threshold")
    print("   Then run: python3 comprehensive_data_fetcher.py")

    print("\n🎯 Key Benefits:")
    print("   ✅ Single script handles all data types")
    print("   ✅ All configuration at the top - easy to change")
    print("   ✅ Intelligent full vs compact fetching")
    print("   ✅ Proper error handling and logging")
    print("   ✅ Works in test mode without API keys")

    print("\n🔄 Before vs After:")
    print(
        "   BEFORE: 3 separate scripts (fetch_daily.py, fetch_30min.py, fetch_intraday_compact.py)"
    )
    print("   AFTER:  1 powerful script (comprehensive_data_fetcher.py)")

    print("\n💡 Smart Features:")
    print("   📊 Files < 10KB → Full historical fetch")
    print("   ⚡ Files ≥ 10KB → Compact real-time updates")
    print("   🔄 Automatic data merging and deduplication")
    print("   📈 Daily data gets full historical + real-time updates")
    print("   ⏰ Intraday data uses intelligent fetch strategy")


if __name__ == "__main__":
    show_examples()
