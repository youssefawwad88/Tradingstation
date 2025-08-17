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

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, update_scheduler_status, format_to_two_decimal
from utils.data_storage import read_df_from_s3

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

def calculate_vwap_from_anchor(df, anchor_date):
    """Calculate AVWAP from anchor date using helper function"""
    try:
        from utils.helpers import calculate_vwap
        anchor_df = df[df['timestamp'] >= anchor_date].copy()
        if not anchor_df.empty:
            anchor_df['vwap'] = calculate_vwap(anchor_df)
            return anchor_df['vwap'].iloc[-1]
    except Exception:
        pass
    return None

def run_breakout_screener():
    """
    Daily Breakout/Breakdown Screener per specification:
    Uses EMA20, Bollinger Bands (20, 2), volume expansion, AVWAP reclaim, base breakout check.
    """
    logger.info("Starting Daily Breakout Screener")
    
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        logger.warning("No tickers in tickerlist.txt. Exiting screener.")
        return

    # Load the AVWAP anchor data
    anchor_df = read_df_from_s3('data/avwap_anchors.csv')
    anchor_dict = {}
    if not anchor_df.empty and 'Ticker' in anchor_df.columns and 'Anchor 1 Date' in anchor_df.columns:
        for _, row in anchor_df.iterrows():
            ticker_symbol = row['Ticker']
            anchor_date = row['Anchor 1 Date']
            if pd.notna(anchor_date):
                anchor_dict[ticker_symbol] = pd.to_datetime(anchor_date)
        logger.info(f"Loaded {len(anchor_dict)} AVWAP anchors")
    else:
        logger.warning("AVWAP anchors file not found or invalid format. AVWAP confluence will not be calculated.")
        
    all_signals = []

    for ticker in tqdm(tickers, desc="Scanning for Breakouts"):
        try:
            daily_df = read_df_from_s3(f'data/daily/{ticker}_daily.csv')
            if daily_df is None or len(daily_df) < 20: # Need at least 20 days for indicators
                continue
                
            # Ensure timestamp column is datetime
            daily_df['timestamp'] = pd.to_datetime(daily_df['timestamp'])
            daily_df = daily_df.sort_values('timestamp')

            # --- Calculate Technical Indicators ---
            # EMA20
            daily_df['EMA20'] = calculate_ema(daily_df['close'], span=20)
            
            # Standard deviation for Bollinger Bands
            daily_df['STD_DEV'] = daily_df['close'].rolling(window=20).std()
            daily_df['BB_Upper'] = daily_df['EMA20'] + 2 * daily_df['STD_DEV']
            daily_df['BB_Lower'] = daily_df['EMA20'] - 2 * daily_df['STD_DEV']
            
            # Volume metrics (avoid lookahead)
            daily_df['Avg_Vol_20D'] = daily_df['volume'].shift(1).rolling(window=20).mean()
            daily_df['Volume_vs_Avg_Pct'] = (daily_df['volume'] / daily_df['Avg_Vol_20D']) * 100

            # Get the latest candle data
            latest = daily_df.iloc[-1]
            
            # --- Directional inference ---
            direction = "None"
            if latest['close'] > latest['EMA20'] and latest['close'] > latest['BB_Upper']:
                direction = "Long"
            elif latest['close'] < latest['EMA20'] and latest['close'] < latest['BB_Lower']:
                direction = "Short"

            # Near breakout/breakdown calculations
            near_breakout = "No"
            near_breakdown = "No"
            if pd.notna(latest['BB_Upper']):
                near_breakout = "Yes" if latest['close'] > latest['BB_Upper'] * 0.98 else "No"
            if pd.notna(latest['BB_Lower']):
                near_breakdown = "Yes" if latest['close'] < latest['BB_Lower'] * 1.02 else "No"

            # --- Candle/body metrics (latest bar) ---
            candle_range = latest['high'] - latest['low']
            body_percent = 0
            if candle_range > 0:
                body_percent = (abs(latest['close'] - latest['open']) / candle_range) * 100

            # --- Breakout from base ---
            breakout_from_base = "No"
            if len(daily_df) >= 6:
                if direction == "Long":
                    # Check if close > max of prior 5 bars' high
                    prior_5_highs = daily_df['high'].iloc[-6:-1]
                    if len(prior_5_highs) == 5 and latest['close'] > prior_5_highs.max():
                        breakout_from_base = "Yes"
                elif direction == "Short":
                    # Check if close < min of prior 5 bars' low  
                    prior_5_lows = daily_df['low'].iloc[-6:-1]
                    if len(prior_5_lows) == 5 and latest['close'] < prior_5_lows.min():
                        breakout_from_base = "Yes"

            # --- AVWAP confirmation ---
            avwap_reclaimed = "No"
            if ticker in anchor_dict:
                anchor_date = anchor_dict[ticker]
                avwap_value = calculate_vwap_from_anchor(daily_df, anchor_date)
                if avwap_value is not None:
                    avwap_reclaimed = "Yes" if latest['close'] > avwap_value else "No"

            # --- Validation (positive conditions for clarity) ---
            volume_condition = latest['Volume_vs_Avg_Pct'] >= 115
            body_condition = body_percent >= 50
            base_condition = breakout_from_base == "Yes"
            
            # AVWAP confirmation condition per spec
            avwap_condition = False
            if direction == "Long":
                avwap_condition = avwap_reclaimed == "Yes"
            elif direction == "Short":
                avwap_condition = avwap_reclaimed == "No"

            setup_valid = False
            why_not_valid = []
            
            if direction != "None":
                if not volume_condition:
                    why_not_valid.append("Volume vs Avg % < 115")
                if not body_condition:
                    why_not_valid.append("Body % of Candle < 50")
                if not base_condition:
                    why_not_valid.append("No breakout from base")
                if not avwap_condition:
                    if direction == "Long":
                        why_not_valid.append("AVWAP not reclaimed")
                    else:
                        why_not_valid.append("AVWAP not rejected")
                
                setup_valid = len(why_not_valid) == 0
            else:
                why_not_valid.append("No valid direction")

            # --- Signal strength (★) ---
            stars = 1  # Base
            if latest['Volume_vs_Avg_Pct'] >= 200:
                stars += 2
            elif latest['Volume_vs_Avg_Pct'] >= 115:
                stars += 1
            
            if body_percent >= 60:
                stars += 1
                
            if avwap_condition:
                stars += 1
                
            if base_condition:
                stars += 1
                
            stars = min(5, stars)  # Clamp to 5

            # Create result
            signal = {
                'Date': latest['timestamp'].strftime('%Y-%m-%d') if hasattr(latest['timestamp'], 'strftime') else str(latest['timestamp']),
                'Ticker': ticker,
                'Close': format_to_two_decimal(latest['close']),
                'High': format_to_two_decimal(latest['high']),
                'Low': format_to_two_decimal(latest['low']),
                'Volume': f"{int(latest['volume']):,}",
                'EMA20': format_to_two_decimal(latest['EMA20']),
                'BB Upper': format_to_two_decimal(latest['BB_Upper']),
                'BB Lower': format_to_two_decimal(latest['BB_Lower']),
                'Volume vs Avg %': f"{latest['Volume_vs_Avg_Pct']:.1f}" if pd.notna(latest['Volume_vs_Avg_Pct']) else "N/A",
                'AVWAP Reclaimed?': avwap_reclaimed,
                'Direction': direction,
                'Setup Valid?': "TRUE" if setup_valid else "FALSE",
                'Body % of Candle': format_to_two_decimal(body_percent),
                'Breakout from Base?': breakout_from_base,
                'Why Not Valid?': "; ".join(why_not_valid) if why_not_valid else "N/A",
                'Near Breakout?': near_breakout,
                'Near Breakdown?': near_breakdown,
                'Signal Strength (★)': "★" * stars
            }
            all_signals.append(signal)

        except Exception as e:
            logger.error(f"Error processing {ticker} in breakout screener: {e}")

    if all_signals:
        signals_df = pd.DataFrame(all_signals)
        save_df_to_s3(signals_df, 'data/signals/breakout_signals.csv')
        logger.info(f"Found {len(signals_df)} breakout signals - saved to cloud")
    else:
        logger.info("No breakout signals found")

    logger.info("Daily Breakout Screener Finished")

if __name__ == "__main__":
    run_breakout_screener()
