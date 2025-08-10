import pandas as pd
import sys
import os
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- System Path Setup ---
# This makes sure the script can find the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    read_tickerlist_from_s3,
    read_df_from_s3,
    save_df_to_s3,
    calculate_vwap,
    format_to_two_decimal
)

# --- Screener-Specific Configuration ---
EMA_LONG_PERIOD = 50
EMA_MEDIUM_PERIOD = 21
EMA_SHORT_PERIOD = 8
VOLUME_SPIKE_THRESHOLD_PCT = 115

def run_ema_pullback_screener():
    """
    EMA Trend Pullback Screener per specification:
    Detect daily trend continuation pullbacks with EMA reclaim/reject behavior and volume confirmation.
    """
    logger.info("Running EMA Trend Pullback Screener")
    
    # --- 1. Load tickers and AVWAP anchors ---
    tickers = read_tickerlist_from_s3('tickerlist.txt')
    if not tickers:
        logger.warning("Ticker list from cloud is empty")
        return
        
    # Load AVWAP anchors for confluence
    anchor_df = read_df_from_s3('data/avwap_anchors.csv')
    anchor_dict = {}
    if not anchor_df.empty and 'Ticker' in anchor_df.columns and 'Anchor 1 Date' in anchor_df.columns:
        for _, row in anchor_df.iterrows():
            ticker_symbol = row['Ticker']
            anchor_date = row['Anchor 1 Date']
            if pd.notna(anchor_date):
                anchor_dict[ticker_symbol] = pd.to_datetime(anchor_date)
        
    all_results = []

    for ticker in tickers:
        try:
            # --- 2. Load Data and Calculate Indicators ---
            df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            if df is None or len(df) < EMA_LONG_PERIOD + 1:
                continue

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            # Calculate EMAs
            df['EMA50'] = df['close'].ewm(span=EMA_LONG_PERIOD, adjust=False).mean()
            df['EMA21'] = df['close'].ewm(span=EMA_MEDIUM_PERIOD, adjust=False).mean()
            df['EMA8'] = df['close'].ewm(span=EMA_SHORT_PERIOD, adjust=False).mean()
            
            # Volume metrics (avoid lookahead)
            df['Avg_Vol_20D'] = df['volume'].shift(1).rolling(window=20).mean()
            df['Volume_vs_Avg_Pct'] = (df['volume'] / df['Avg_Vol_20D']) * 100

            # --- 3. Candle metrics (latest and previous) ---
            latest = df.iloc[-1]
            previous = df.iloc[-2] if len(df) >= 2 else None
            
            trend_vs_ema50 = "Above" if latest['close'] > latest['EMA50'] else "Below"
            
            # Candle calculations
            candle_range = latest['high'] - latest['low']
            candle_body = abs(latest['close'] - latest['open'])
            body_percent = (candle_body / candle_range * 100) if candle_range > 0 else 0
            
            # Inside Candle check
            inside_candle = "No"
            if previous is not None:
                if latest['high'] < previous['high'] and latest['low'] > previous['low']:
                    inside_candle = "Yes"

            # --- 4. Pullback logic ---
            # Wick Confirmed check
            combined_wicks = candle_range - candle_body
            wick_confirmed = "Yes" if combined_wicks > 2 * candle_body else "No"
            
            # Signal Candle check
            signal_candle = "Yes" if 20 <= body_percent <= 60 else "No"
            
            # Direction and EMA Reclaim/Reject
            direction = "None"
            ema_reclaim_reject = "N/A"
            ema_distance_pct = np.nan
            
            # Long condition: trend above EMA50, open below EMA8, close above EMA8
            if trend_vs_ema50 == "Above" and latest['open'] < latest['EMA8'] and latest['close'] > latest['EMA8']:
                direction = "Long"
                ema_reclaim_reject = "Reclaim EMA8"
                ema_distance_pct = (latest['close'] - latest['EMA8']) / latest['EMA8'] * 100
                
            # Short condition: trend below EMA50, open above EMA21, close below EMA21  
            elif trend_vs_ema50 == "Below" and latest['open'] > latest['EMA21'] and latest['close'] < latest['EMA21']:
                direction = "Short"
                ema_reclaim_reject = "Reject EMA21"
                ema_distance_pct = (latest['EMA21'] - latest['close']) / latest['EMA21'] * 100

            # --- 5. Volume spike ---
            volume_spike = "Yes" if latest['Volume_vs_Avg_Pct'] >= 115 else "No"

            # --- 6. AVWAP Confluence ---
            avwap_confluence = "N/A"
            if ticker in anchor_dict:
                try:
                    anchor_date = anchor_dict[ticker]
                    anchor_df_filtered = df[df['timestamp'] >= anchor_date].copy()
                    if not anchor_df_filtered.empty:
                        anchor_df_filtered['vwap'] = calculate_vwap(anchor_df_filtered)
                        avwap_value = anchor_df_filtered['vwap'].iloc[-1]
                        distance_to_avwap = abs(latest['close'] - avwap_value) / avwap_value * 100
                        avwap_confluence = "Yes" if distance_to_avwap <= 1.0 else "No"
                except Exception:
                    avwap_confluence = "N/A"

            # --- 7. Validation ---
            conditions = [
                wick_confirmed == "Yes",
                ema_reclaim_reject != "N/A", 
                signal_candle == "Yes",
                volume_spike == "Yes",
                direction != "None"
            ]
            
            setup_valid = all(conditions)
            
            # Reason / Notes
            failing_conditions = []
            if wick_confirmed != "Yes":
                failing_conditions.append("Wick not confirmed")
            if ema_reclaim_reject == "N/A":
                failing_conditions.append("No EMA reclaim/reject")
            if signal_candle != "Yes":
                failing_conditions.append("Not a signal candle (20-60% body)")
            if volume_spike != "Yes":
                failing_conditions.append("No volume spike")
            if direction == "None":
                failing_conditions.append("No valid direction")
                
            reason_notes = "; ".join(failing_conditions) if failing_conditions else "N/A"

            # --- 8. Populate result ---
            result = {
                "Date": latest['timestamp'].strftime('%Y-%m-%d') if hasattr(latest['timestamp'], 'strftime') else str(latest['timestamp']),
                "Ticker": ticker,
                "Direction": direction,
                "Trend vs EMA50": trend_vs_ema50,
                "EMA50": format_to_two_decimal(latest['EMA50']),
                "EMA21": format_to_two_decimal(latest['EMA21']),
                "EMA8": format_to_two_decimal(latest['EMA8']),
                "Close Price": format_to_two_decimal(latest['close']),
                "Wick Confirmed?": wick_confirmed,
                "EMA Reclaim/Reject": ema_reclaim_reject,
                "Signal Candle?": signal_candle,
                "Volume": f"{int(latest['volume']):,}",
                "Avg 20D Volume": f"{int(latest['Avg_Vol_20D']):,}" if pd.notna(latest['Avg_Vol_20D']) else "N/A",
                "Volume vs Avg %": format_to_two_decimal(latest['Volume_vs_Avg_Pct']),
                "Volume Spike?": volume_spike,
                "Signal Direction": direction if setup_valid else "None",
                "Reason / Notes": reason_notes,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Body % of Candle": format_to_two_decimal(body_percent),
                "Inside Candle?": inside_candle,
                "EMA Distance %": format_to_two_decimal(ema_distance_pct),
                "AVWAP Confluence?": avwap_confluence
            }
            all_results.append(result)

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")

    # --- 9. Final Processing & Save to Cloud ---
    if not all_results:
        logger.info("No EMA Pullback signals were generated.")
        return

    final_df = pd.DataFrame(all_results)
    final_df.sort_values(by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True)
    
    save_df_to_s3(final_df, 'data/signals/ema_pullback_signals.csv')
    logger.info("EMA Pullback Screener finished. Results saved to cloud.")

if __name__ == "__main__":
    run_ema_pullback_screener()
