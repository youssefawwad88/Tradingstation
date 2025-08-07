#!/usr/bin/env python3
"""
Generate Master Ticker List

This script implements the unified ticker strategy by merging two sources:
1. Manual tickers from tickerlist.txt (always included, no filters)
2. S&P 500 tickers filtered using Ashraf's breakout logic

The resulting master_tickerlist.csv powers all fetchers (both full and compact).
"""

import sys
import os
import pandas as pd
import logging
from datetime import datetime, timedelta
import time

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, update_scheduler_status
from utils.alpha_vantage_api import get_daily_data, get_intraday_data, get_company_overview

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_manual_tickers():
    """
    Load manual tickers from tickerlist.txt.
    These are always included with no filters applied.
    
    Returns:
        list: List of manual ticker symbols
    """
    logger.info("Loading manual tickers from tickerlist.txt")
    
    try:
        manual_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tickerlist.txt")
        if os.path.exists(manual_file):
            with open(manual_file, 'r') as f:
                # Handle numbered format like "1.NVDA"
                tickers = []
                for line in f.readlines():
                    line = line.strip()
                    if line:
                        # Remove number prefix if present (e.g., "1.NVDA" -> "NVDA")
                        if '.' in line and line.split('.')[0].isdigit():
                            ticker = line.split('.', 1)[1]
                        else:
                            ticker = line
                        tickers.append(ticker)
                
            logger.info(f"✅ Loaded {len(tickers)} manual tickers: {tickers}")
            return tickers
        else:
            logger.warning(f"Manual ticker file not found: {manual_file}")
            return []
    except Exception as e:
        logger.error(f"Error loading manual tickers: {e}")
        return []

def load_sp500_tickers():
    """
    Load S&P 500 tickers from data/universe/sp500.csv.
    
    Returns:
        list: List of S&P 500 ticker symbols
    """
    logger.info("Loading S&P 500 tickers from data/universe/sp500.csv")
    
    try:
        sp500_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "universe", "sp500.csv")
        if os.path.exists(sp500_file):
            df = pd.read_csv(sp500_file)
            tickers = df['Symbol'].tolist()
            logger.info(f"✅ Loaded {len(tickers)} S&P 500 tickers")
            return tickers
        else:
            logger.warning(f"S&P 500 file not found: {sp500_file}")
            return []
    except Exception as e:
        logger.error(f"Error loading S&P 500 tickers: {e}")
        return []

def calculate_gap_percentage(ticker):
    """
    Calculate gap percentage from prior close.
    
    Args:
        ticker (str): Stock symbol
        
    Returns:
        float: Gap percentage (positive for gap up, negative for gap down)
    """
    try:
        # Get latest daily data (2 days to compare)
        daily_df = get_daily_data(ticker, outputsize='compact')
        if daily_df.empty or len(daily_df) < 2:
            return 0.0
        
        # Alpha Vantage daily data is sorted newest first
        today_open = float(daily_df.iloc[0]['open'])
        yesterday_close = float(daily_df.iloc[1]['close'])
        
        gap_percent = ((today_open - yesterday_close) / yesterday_close) * 100
        return gap_percent
        
    except Exception as e:
        logger.debug(f"Error calculating gap for {ticker}: {e}")
        return 0.0

def calculate_early_volume_spike(ticker):
    """
    Calculate early volume spike (9:30–9:44 volume vs 5-day average).
    
    Args:
        ticker (str): Stock symbol
        
    Returns:
        float: Volume spike ratio (e.g., 1.15 = 115% of average)
    """
    try:
        # Get 1-minute intraday data for volume analysis
        intraday_df = get_intraday_data(ticker, interval='1min', outputsize='full')
        if intraday_df.empty:
            return 0.0
        
        # Convert timestamp column
        intraday_df['timestamp'] = pd.to_datetime(intraday_df['timestamp'])
        
        # Filter for 9:30-9:44 AM ET time window (first 15 minutes)
        today = datetime.now().date()
        early_morning_start = pd.Timestamp.combine(today, pd.Timestamp('09:30:00').time())
        early_morning_end = pd.Timestamp.combine(today, pd.Timestamp('09:44:59').time())
        
        # Get today's early volume (9:30-9:44)
        today_early = intraday_df[
            (intraday_df['timestamp'] >= early_morning_start) & 
            (intraday_df['timestamp'] <= early_morning_end)
        ]
        
        if today_early.empty:
            return 0.0
        
        today_early_volume = today_early['volume'].sum()
        
        # Get 5-day average early volume (exclude today)
        five_days_ago = datetime.now() - timedelta(days=5)
        historical_data = intraday_df[intraday_df['timestamp'] < early_morning_start]
        historical_data = historical_data[historical_data['timestamp'] >= five_days_ago]
        
        # Group by date and calculate daily early volume for past 5 days
        historical_data['date'] = historical_data['timestamp'].dt.date
        daily_early_volumes = []
        
        for date in historical_data['date'].unique():
            daily_data = historical_data[historical_data['date'] == date]
            # Filter for 9:30-9:44 window for each day
            date_start = pd.Timestamp.combine(date, pd.Timestamp('09:30:00').time())
            date_end = pd.Timestamp.combine(date, pd.Timestamp('09:44:59').time())
            
            early_data = daily_data[
                (daily_data['timestamp'] >= date_start) & 
                (daily_data['timestamp'] <= date_end)
            ]
            
            if not early_data.empty:
                daily_early_volumes.append(early_data['volume'].sum())
        
        if not daily_early_volumes:
            return 0.0
        
        avg_early_volume = sum(daily_early_volumes) / len(daily_early_volumes)
        
        if avg_early_volume == 0:
            return 0.0
        
        volume_spike_ratio = today_early_volume / avg_early_volume
        return volume_spike_ratio
        
    except Exception as e:
        logger.debug(f"Error calculating early volume spike for {ticker}: {e}")
        return 0.0

def check_vwap_reclaim_or_breakout(ticker):
    """
    Check if VWAP is reclaimed OR breakout above pre-market high.
    
    Args:
        ticker (str): Stock symbol
        
    Returns:
        bool: True if VWAP reclaimed or pre-market high broken
    """
    try:
        # Get intraday data for VWAP and pre-market analysis
        intraday_df = get_intraday_data(ticker, interval='1min', outputsize='compact')
        if intraday_df.empty:
            return False
        
        # Convert timestamp and add VWAP calculation
        intraday_df['timestamp'] = pd.to_datetime(intraday_df['timestamp'])
        intraday_df['vwap'] = (intraday_df['close'] * intraday_df['volume']).cumsum() / intraday_df['volume'].cumsum()
        
        # Get latest price and VWAP
        latest_price = float(intraday_df.iloc[-1]['close'])
        latest_vwap = float(intraday_df.iloc[-1]['vwap'])
        
        # Check VWAP reclaim (simplified: current price > VWAP)
        vwap_reclaimed = latest_price > latest_vwap
        
        # Check pre-market high breakout (simplified: use early session high)
        today = datetime.now().date()
        premarket_start = pd.Timestamp.combine(today, pd.Timestamp('04:00:00').time())
        market_open = pd.Timestamp.combine(today, pd.Timestamp('09:30:00').time())
        
        premarket_data = intraday_df[
            (intraday_df['timestamp'] >= premarket_start) & 
            (intraday_df['timestamp'] < market_open)
        ]
        
        if not premarket_data.empty:
            premarket_high = premarket_data['high'].max()
            breakout_above_premarket = latest_price > premarket_high
        else:
            breakout_above_premarket = False
        
        return vwap_reclaimed or breakout_above_premarket
        
    except Exception as e:
        logger.debug(f"Error checking VWAP/breakout for {ticker}: {e}")
        return False

def check_fundamental_filters(ticker):
    """
    Check fundamental filters: Market Cap, Float, Stock Price.
    
    Args:
        ticker (str): Stock symbol
        
    Returns:
        dict: Dictionary with filter results
    """
    try:
        # Get company overview data
        overview = get_company_overview(ticker)
        if not overview:
            return {
                'market_cap_ok': False,
                'float_ok': False,
                'price_ok': False,
                'reason': 'No fundamental data available'
            }
        
        # Extract key metrics
        market_cap = float(overview.get('MarketCapitalization', 0)) if overview.get('MarketCapitalization') != 'None' else 0
        shares_outstanding = float(overview.get('SharesOutstanding', 0)) if overview.get('SharesOutstanding') != 'None' else 0
        
        # Get current price from daily data
        daily_df = get_daily_data(ticker, outputsize='compact')
        current_price = float(daily_df.iloc[0]['close']) if not daily_df.empty else 0
        
        # Apply filters
        market_cap_ok = market_cap > 2_000_000_000  # > $2B
        float_ok = shares_outstanding > 100_000_000 and shares_outstanding < 999_999_999  # >100M, avoid low float <20M implicitly
        price_ok = current_price > 5.0  # > $5
        
        return {
            'market_cap_ok': market_cap_ok,
            'float_ok': float_ok,
            'price_ok': price_ok,
            'market_cap': market_cap,
            'shares_outstanding': shares_outstanding,
            'current_price': current_price,
            'reason': f"MarketCap: ${market_cap/1e9:.1f}B, Float: {shares_outstanding/1e6:.0f}M, Price: ${current_price:.2f}"
        }
        
    except Exception as e:
        logger.debug(f"Error checking fundamentals for {ticker}: {e}")
        return {
            'market_cap_ok': False,
            'float_ok': False,
            'price_ok': False,
            'reason': f'Error: {str(e)}'
        }

def apply_ashraf_filters(ticker):
    """
    Apply Ashraf filtering logic to a single ticker.
    
    Args:
        ticker (str): Stock symbol
        
    Returns:
        dict: Filter results with pass/fail status and reasons
    """
    logger.debug(f"Applying Ashraf filters to {ticker}")
    
    try:
        # 1. Gap % Filter
        gap_percent = calculate_gap_percentage(ticker)
        gap_filter_pass = abs(gap_percent) > 1.5
        
        # 2. Early Volume Spike Filter
        volume_spike = calculate_early_volume_spike(ticker)
        volume_filter_pass = volume_spike > 1.15
        
        # 3. VWAP Reclaim or Breakout Filter
        vwap_breakout_pass = check_vwap_reclaim_or_breakout(ticker)
        
        # 4. Fundamental Filters
        fundamentals = check_fundamental_filters(ticker)
        fundamental_pass = fundamentals['market_cap_ok'] and fundamentals['float_ok'] and fundamentals['price_ok']
        
        # Overall pass (all filters must pass)
        overall_pass = gap_filter_pass and volume_filter_pass and vwap_breakout_pass and fundamental_pass
        
        result = {
            'ticker': ticker,
            'pass': overall_pass,
            'gap_percent': gap_percent,
            'gap_filter_pass': gap_filter_pass,
            'volume_spike': volume_spike,
            'volume_filter_pass': volume_filter_pass,
            'vwap_breakout_pass': vwap_breakout_pass,
            'fundamental_pass': fundamental_pass,
            'fundamental_details': fundamentals,
            'reason': f"Gap: {gap_percent:.1f}%, Vol: {volume_spike:.2f}x, VWAP/BO: {vwap_breakout_pass}, Fund: {fundamental_pass}"
        }
        
        if overall_pass:
            logger.info(f"✅ {ticker} PASSED all filters: {result['reason']}")
        else:
            logger.debug(f"❌ {ticker} FAILED filters: {result['reason']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error applying filters to {ticker}: {e}")
        return {
            'ticker': ticker,
            'pass': False,
            'reason': f'Error: {str(e)}'
        }

def filter_sp500_tickers(sp500_tickers):
    """
    Apply Ashraf filtering logic to S&P 500 tickers.
    
    Args:
        sp500_tickers (list): List of S&P 500 ticker symbols
        
    Returns:
        list: Filtered ticker symbols that pass all criteria
    """
    logger.info(f"🔍 Applying Ashraf filters to {len(sp500_tickers)} S&P 500 tickers")
    
    filtered_tickers = []
    filter_results = []
    
    for ticker in sp500_tickers:
        try:
            result = apply_ashraf_filters(ticker)
            filter_results.append(result)
            
            if result['pass']:
                filtered_tickers.append(ticker)
            
            # Respect API rate limits
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error filtering {ticker}: {e}")
            continue
    
    logger.info(f"📊 Filter Results: {len(filtered_tickers)}/{len(sp500_tickers)} S&P 500 tickers passed")
    
    # Log summary of failed filters
    failed_results = [r for r in filter_results if not r['pass']]
    if failed_results:
        logger.info(f"❌ Failed tickers ({len(failed_results)}):")
        for result in failed_results[:10]:  # Show first 10 failures
            logger.info(f"   {result['ticker']}: {result.get('reason', 'Unknown')}")
        if len(failed_results) > 10:
            logger.info(f"   ... and {len(failed_results) - 10} more")
    
    return filtered_tickers

def generate_master_tickerlist():
    """
    Generate the master ticker list by combining manual and filtered S&P 500 tickers.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("🚀 Starting Master Ticker List Generation")
    
    try:
        # 1. Load manual tickers (always included, no filters)
        manual_tickers = load_manual_tickers()
        logger.info(f"📝 Manual tickers: {len(manual_tickers)} loaded")
        
        # 2. Load S&P 500 tickers
        sp500_tickers = load_sp500_tickers()
        logger.info(f"📈 S&P 500 tickers: {len(sp500_tickers)} loaded")
        
        # 3. Apply Ashraf filters to S&P 500 tickers
        if sp500_tickers:
            filtered_sp500 = filter_sp500_tickers(sp500_tickers)
            logger.info(f"✅ Filtered S&P 500 tickers: {len(filtered_sp500)} passed filters")
        else:
            filtered_sp500 = []
            logger.warning("⚠️ No S&P 500 tickers to filter")
        
        # 4. Combine manual and filtered S&P 500 tickers
        all_tickers = manual_tickers + filtered_sp500
        
        # 5. Remove duplicates while preserving order (manual tickers first)
        master_tickers = []
        seen = set()
        for ticker in all_tickers:
            if ticker not in seen:
                master_tickers.append(ticker)
                seen.add(ticker)
        
        logger.info(f"🎯 Master ticker list generated: {len(master_tickers)} total tickers")
        logger.info(f"   Manual: {len(manual_tickers)} | Filtered S&P 500: {len(filtered_sp500)} | Total: {len(master_tickers)}")
        
        # 6. Create DataFrame and save to CSV
        df = pd.DataFrame({
            'ticker': master_tickers,
            'source': ['manual' if t in manual_tickers else 'sp500_filtered' for t in master_tickers],
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Save to local file
        output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_tickerlist.csv")
        df.to_csv(output_file, index=False)
        logger.info(f"💾 Master ticker list saved locally: {output_file}")
        
        # Also save to Spaces if configured
        spaces_path = "master_tickerlist.csv"
        upload_success = save_df_to_s3(df, spaces_path)
        if upload_success:
            logger.info(f"☁️ Master ticker list uploaded to Spaces: {spaces_path}")
        else:
            logger.warning(f"⚠️ Failed to upload to Spaces, but local file created: {output_file}")
        
        # 7. Display final list
        logger.info(f"📋 Final Master Ticker List ({len(master_tickers)} tickers):")
        for i, ticker in enumerate(master_tickers, 1):
            source = "MANUAL" if ticker in manual_tickers else "S&P500"
            logger.info(f"   {i:2d}. {ticker} ({source})")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error generating master ticker list: {e}")
        return False

if __name__ == "__main__":
    job_name = "generate_master_tickerlist"
    update_scheduler_status(job_name, "Running")
    
    try:
        success = generate_master_tickerlist()
        if success:
            update_scheduler_status(job_name, "Success")
            logger.info("✅ Master ticker list generation completed successfully")
        else:
            update_scheduler_status(job_name, "Fail", "Failed to generate master ticker list")
            logger.error("❌ Master ticker list generation failed")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        update_scheduler_status(job_name, "Fail", error_message)