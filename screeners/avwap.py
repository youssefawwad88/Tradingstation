import pandas as pd
import sys
import os
from datetime import datetime
import pytz
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- System Path Setup ---
# This makes sure the script can find the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_df_from_s3,
    save_df_to_s3,
    calculate_vwap,
    format_to_two_decimal
)

# --- Screener-Specific Configuration ---
VOLUME_SPIKE_THRESHOLD = 1.15  # 115%
STRONG_BODY_THRESHOLD = 60.0   # Min 60% body for a strong candle

def get_reclaim_quality(candle: pd.Series) -> str:
    """Analyzes a candle to determine the quality of a reclaim/rejection."""
    candle_range = candle['high'] - candle['low']
    if candle_range == 0:
        return "Weak"
    
    body_pct = (abs(candle['close'] - candle['open']) / candle_range) * 100
    
    if body_pct >= STRONG_BODY_THRESHOLD:
        return "Strong"
    elif body_pct >= 30:
        return "Moderate"
    else:
        return "Weak"

def run_avwap_screener():
    """
    AVWAP Reclaim Screener per specification:
    Detects reclaim or rejection of 1-2 anchored VWAPs and produces fields needed by trade planning.
    """
    logger.info("Running Anchored VWAP (AVWAP) Screener")
    
    # --- 1. Load AVWAP anchors ---
    anchor_df = read_df_from_s3("data/avwap_anchors.csv")
    if anchor_df.empty:
        logger.error("Anchor file not found in cloud storage. Please run the find_avwap_anchors.py job first.")
        return
        
    # Handle different possible column names
    ticker_col = None
    for col in ['Ticker', 'ticker', 'TICKER']:
        if col in anchor_df.columns:
            ticker_col = col
            break
    
    if ticker_col is None:
        logger.error("No ticker column found in AVWAP anchors file")
        return
        
    anchor_dict = {}
    for _, row in anchor_df.iterrows():
        ticker = row[ticker_col]
        anchor_1_date = row.get('Anchor 1 Date')
        anchor_2_date = row.get('Anchor 2 Date') 
        
        anchor_dict[ticker] = {
            'anchor_1': pd.to_datetime(anchor_1_date) if pd.notna(anchor_1_date) else None,
            'anchor_2': pd.to_datetime(anchor_2_date) if pd.notna(anchor_2_date) else None
        }
    
    all_results = []

    # --- 2. Process Each Ticker ---
    for ticker, anchors in anchor_dict.items():
        try:
            # Load daily data (or intraday if preferred)
            daily_df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            if daily_df is None or daily_df.empty:
                continue
            
            daily_df['timestamp'] = pd.to_datetime(daily_df['timestamp'])
            daily_df = daily_df.sort_values('timestamp')

            # --- 3. Core logic - Calculate AVWAPs ---
            latest = daily_df.iloc[-1]
            current_price = latest['close']
            
            avwap_1 = np.nan
            avwap_2 = np.nan
            
            # Calculate AVWAP 1
            if anchors['anchor_1'] is not None:
                anchor_1_df = daily_df[daily_df['timestamp'] >= anchors['anchor_1']].copy()
                if not anchor_1_df.empty:
                    anchor_1_df['vwap'] = calculate_vwap(anchor_1_df)
                    avwap_1 = anchor_1_df['vwap'].iloc[-1]
            
            # Calculate AVWAP 2  
            if anchors['anchor_2'] is not None:
                anchor_2_df = daily_df[daily_df['timestamp'] >= anchors['anchor_2']].copy()
                if not anchor_2_df.empty:
                    anchor_2_df['vwap'] = calculate_vwap(anchor_2_df)
                    avwap_2 = anchor_2_df['vwap'].iloc[-1]

            # --- 4. Direction determination ---
            direction = "None"
            reclaim_reject = "N/A"
            nearest_avwap = None
            distance_to_nearest = np.nan
            
            # Find nearest AVWAP
            valid_avwaps = []
            if not np.isnan(avwap_1):
                valid_avwaps.append(avwap_1)
            if not np.isnan(avwap_2):
                valid_avwaps.append(avwap_2)
                
            if valid_avwaps:
                # Find nearest AVWAP to current price
                distances = [abs(current_price - avwap) for avwap in valid_avwaps]
                nearest_avwap = valid_avwaps[distances.index(min(distances))]
                distance_to_nearest = abs(current_price - nearest_avwap) / nearest_avwap * 100
                
                # Determine reclaim/reject (simplified logic - could be enhanced with crossing detection)
                if current_price > nearest_avwap:
                    direction = "Long"
                    reclaim_reject = "Reclaim"
                elif current_price < nearest_avwap:
                    direction = "Short" 
                    reclaim_reject = "Reject"

            # --- 5. Volume metrics ---
            # Volume vs Avg % calculation (avoid lookahead)
            avg_vol_20d = daily_df['volume'].shift(1).rolling(window=20).mean().iloc[-1]
            volume_vs_avg_pct = (latest['volume'] / avg_vol_20d) * 100 if pd.notna(avg_vol_20d) and avg_vol_20d > 0 else 0

            # --- 6. Setup validation ---
            setup_valid = False
            conditions = []
            
            if direction != "None":
                # Basic conditions per spec
                volume_condition = volume_vs_avg_pct >= 115
                proximity_condition = distance_to_nearest <= 1.5 if not np.isnan(distance_to_nearest) else False
                
                conditions = [volume_condition, proximity_condition]
                setup_valid = all(conditions)

            # --- 7. Populate result ---
            result = {
                "Date": latest['timestamp'].strftime('%Y-%m-%d') if hasattr(latest['timestamp'], 'strftime') else str(latest['timestamp']),
                "Ticker": ticker,
                "Direction": direction,
                "Current Price": format_to_two_decimal(current_price),
                "AVWAP 1": format_to_two_decimal(avwap_1) if not np.isnan(avwap_1) else "N/A",
                "AVWAP 2": format_to_two_decimal(avwap_2) if not np.isnan(avwap_2) else "N/A",
                "Volume vs Avg %": format_to_two_decimal(volume_vs_avg_pct),
                "Distance to Nearest AVWAP %": format_to_two_decimal(distance_to_nearest) if not np.isnan(distance_to_nearest) else "N/A",
                "Reclaim/Reject?": reclaim_reject,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Notes": f"Nearest AVWAP: {format_to_two_decimal(nearest_avwap)}" if nearest_avwap else "No valid AVWAP"
            }
            all_results.append(result)

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")

    # --- 8. Final Processing & Save to Cloud ---
    if not all_results:
        logger.info("No AVWAP signals were generated.")
        return
        
    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=['Setup Valid?', 'Ticker'], ascending=[False, True], inplace=True)
    
    save_df_to_s3(final_df, 'data/signals/avwap_signals.csv')
    logger.info("AVWAP Screener finished. Results saved to cloud.")

if __name__ == "__main__":
    run_avwap_screener()
