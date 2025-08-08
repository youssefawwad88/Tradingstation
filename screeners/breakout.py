import sys
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, read_df_from_s3, save_df_to_s3, update_scheduler_status, format_to_two_decimal

def calculate_bollinger_bands(series, window=20, num_std=2):
    """Calculate Bollinger Bands manually"""
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)
    return upper_band, lower_band, rolling_mean

def calculate_ema(series, span=20):
    """Calculate Exponential Moving Average"""
    return series.ewm(span=span, adjust=False).mean()

def run_breakout_screener():
    """
    Scans for daily breakouts based on Bollinger Bands, volume, and other criteria.
    """
    logger.info("Starting Daily Breakout Screener")
    
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        print("No tickers in tickerlist.txt. Exiting screener.")
        return

    # Load the AVWAP anchor data
    anchor_df = read_df_from_s3('data/avwap_anchors.csv')
    if not anchor_df.empty:
        # CORRECTED: Use lowercase 'ticker' to match the file's column name
        if 'ticker' in anchor_df.columns:
            anchor_df = anchor_df.set_index('ticker')
        else:
            logger.warning("'ticker' column not found in avwap_anchors.csv. AVWAP confluence will not be calculated.")
            anchor_df = None # Set to None if column is missing
    else:
        anchor_df = None
        
    all_signals = []

    for ticker in tqdm(tickers, desc="Scanning for Breakouts"):
        try:
            daily_df = read_df_from_s3(f'data/daily/{ticker}_daily.csv')
            if daily_df is None or len(daily_df) < 20: # Need at least 20 days for indicators
                continue

            # --- Calculate Technical Indicators ---
            upper_band, lower_band, bb_middle = calculate_bollinger_bands(daily_df['close'], window=20, num_std=2)
            daily_df['BBU_20_2.0'] = upper_band
            daily_df['BBL_20_2.0'] = lower_band
            daily_df['EMA20'] = calculate_ema(daily_df['close'], span=20)
            daily_df['AvgVol20'] = daily_df['volume'].rolling(window=20).mean()
            daily_df['BodyAbs'] = abs(daily_df['close'] - daily_df['open'])
            daily_df['CandleRange'] = daily_df['high'] - daily_df['low']
            
            # Avoid division by zero for Body %
            daily_df['BodyPercent'] = daily_df.apply(
                lambda row: (abs(row['close'] - row['open']) / (row['high'] - row['low']) * 100) 
                if (row['high'] - row['low']) > 0 else 0, axis=1
            )

            # Get the latest candle data
            latest = daily_df.iloc[-1]
            
            # --- Core Screener Logic ---
            is_long_breakout = (latest['close'] > latest['EMA20']) and \
                               (latest['close'] > latest['BBU_20_2.0']) and \
                               (latest['volume'] > latest['AvgVol20'] * 1.15) and \
                               (latest['BodyPercent'] >= 60) and \
                               (latest['close'] > daily_df['high'].iloc[-6:-1].max())

            # (Add logic for short breakouts if needed)
            is_short_breakdown = False 

            setup_valid = is_long_breakout or is_short_breakdown
            
            if setup_valid:
                direction = "Long" if is_long_breakout else "Short"
                
                # Check for AVWAP confluence
                avwap_reclaimed = "N/A"
                if anchor_df is not None and ticker in anchor_df.index:
                    # A simple check if the close is above any anchor price
                    anchor_prices = anchor_df.loc[ticker, 'anchor_price']
                    if isinstance(anchor_prices, pd.Series):
                        max_anchor_price = anchor_prices.max()
                    else:
                        max_anchor_price = anchor_prices
                    if latest['close'] > max_anchor_price:
                         avwap_reclaimed = "Yes"
                    else:
                         avwap_reclaimed = "No"

                signal = {
                    'Ticker': ticker,
                    'Date': latest['timestamp'],
                    'Close': format_to_two_decimal(latest['close']),
                    'Volume': latest['volume'],
                    'Volume vs Avg %': f"{latest['volume'] / latest['AvgVol20']:.2%}",
                    'Direction': direction,
                    'Setup Valid?': True,
                    'Body % of Candle': format_to_two_decimal(latest['BodyPercent']),
                    'AVWAP Reclaimed?': avwap_reclaimed
                }
                all_signals.append(signal)

        except Exception as e:
            tqdm.write(f"Error processing {ticker} in breakout screener: {e}")

    if all_signals:
        signals_df = pd.DataFrame(all_signals)
        save_df_to_s3(signals_df, 'data/signals/breakout_signals.csv')
        logger.info(f"Found {len(signals_df)} breakout signals - saved to cloud")
    else:
        logger.info("No breakout signals found")

    logger.info("Daily Breakout Screener Finished")

if __name__ == "__main__":
    run_breakout_screener()
