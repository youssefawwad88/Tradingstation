import sys
import os
import pandas as pd
import pandas_ta as ta
from tqdm import tqdm

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, read_df_from_s3, save_df_to_s3, update_scheduler_status, format_to_two_decimal

def run_breakout_screener():
    """
    Scans for daily breakouts based on Bollinger Bands, volume, and other criteria.
    """
    print("--- Starting Daily Breakout Screener ---")
    
    tickers = read_tickerlist_from_s3()
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
            print("Warning: 'ticker' column not found in avwap_anchors.csv. AVWAP confluence will not be calculated.")
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
            daily_df.ta.bbands(length=20, append=True)
            daily_df['EMA20'] = ta.ema(daily_df['close'], length=20)
            daily_df['AvgVol20'] = daily_df['volume'].rolling(window=20).mean()
            daily_df['BodyAbs'] = abs(daily_df['close'] - daily_df['open'])
            daily_df['CandleRange'] = daily_df['high'] - daily_df['low']
            
            # Avoid division by zero for Body %
            daily_df['BodyPercent'] = (daily_df['BodyAbs'] / daily_df['CandleRange']).fillna(0) * 100

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
                    if latest['close'] > anchor_df.loc[ticker, 'anchor_price'].max():
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
        print(f"\nFound {len(signals_df)} breakout signals. Saved to cloud.")
    else:
        print("\nNo breakout signals found.")

    print("--- Daily Breakout Screener Finished ---")

if __name__ == "__main__":
    run_breakout_screener()
