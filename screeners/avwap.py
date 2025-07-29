"""
Anchored VWAP (AVWAP) Reclaim Screener - Optimized Logic

This screener uses pre-calculated anchor dates to identify stocks that are
reclaiming or being rejected by a significant AVWAP level, with support for
both Long and Short signals and detailed quality scoring.
"""

import pandas as pd
import sys
import os
from datetime import datetime
import pytz
import numpy as np

# --- System Path Setup ---
PROJECT_ROOT = '/content/drive/MyDrive/trading-system'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import config, helpers

# --- Screener-Specific Configuration ---
VOLUME_SPIKE_THRESHOLD = 1.15  # 115%
AVWAP_DISTANCE_THRESHOLD = 6.0   # Max 6% distance from AVWAP
STRONG_BODY_THRESHOLD = 60.0   # Min 60% body for a strong candle

def get_reclaim_quality(candle: pd.Series) -> str:
    """Analyzes a candle to determine the quality of a reclaim/rejection."""
    candle_range = candle[config.HIGH_COL] - candle[config.LOW_COL]
    if candle_range == 0:
        return "Weak"
    
    body_pct = (abs(candle[config.CLOSE_COL] - candle[config.OPEN_COL]) / candle_range) * 100
    
    if body_pct >= STRONG_BODY_THRESHOLD:
        return "Strong"
    elif body_pct >= 30:
        return "Moderate"
    else:
        return "Weak"

def calculate_signal_strength(reclaim_count: int, volume_ok: bool, quality: str, distance_ok: bool) -> str:
    """Calculates a 1-5 star rating for the signal strength."""
    score = 0
    if reclaim_count > 0: score += reclaim_count # +1 for each anchor
    if volume_ok: score += 1
    if distance_ok: score += 1
    if quality == "Strong": score += 1
    
    return "★" * max(1, min(5, score)) # Ensure rating is between 1 and 5

def run_avwap_screener():
    """Main function to execute the AVWAP screening logic."""
    print("\n--- Running Anchored VWAP (AVWAP) Screener ---")
    
    # --- 1. Load Prerequisite Data ---
    anchor_file = config.DATA_DIR / "avwap_anchors.csv"
    if not anchor_file.exists():
        print(f"ERROR: Anchor file not found at {anchor_file}. Please run the find_avwap_anchors.py job first.")
        return
        
    anchor_df = pd.read_csv(anchor_file).set_index('Ticker')
    tickers = anchor_df.index.tolist()
    all_results = []
    ny_timezone = pytz.timezone('America/New_York')

    # --- 2. Process Each Ticker ---
    for ticker in tickers:
        try:
            # Use 30-minute intraday data as specified
            daily_file = config.DAILY_DIR / f"{ticker}_daily.csv"
            intraday_file = config.INTRADAY_30MIN_DIR / f"{ticker}_30min.csv"
            if not daily_file.exists() or not intraday_file.exists():
                continue
            
            daily_df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            intraday_df = pd.read_csv(intraday_file, index_col=0, parse_dates=True)
            
            if intraday_df.empty: continue

            # --- 3. Get Live Data & Base Metrics ---
            latest_candle = intraday_df.iloc[-1]
            current_price = latest_candle[config.CLOSE_COL]
            today_volume = intraday_df[intraday_df.index.date == datetime.now(ny_timezone).date()][config.VOLUME_COL].sum()
            avg_20d_volume = daily_df.tail(20)[config.VOLUME_COL].mean()
            volume_vs_avg_pct = (today_volume / avg_20d_volume) * 100 if avg_20d_volume > 0 else 0

            # --- 4. Initialize Result Row ---
            # FIX 1: Use the latest candle's date for the signal date.
            result = {"Ticker": ticker, "Date": latest_candle.name.strftime('%Y-%m-%d')}
            avwap_results = {}

            # --- 5. Calculate AVWAP for each confirmed anchor ---
            ticker_anchors = anchor_df.loc[ticker]
            for i in [1, 2]:
                if ticker_anchors.get(f'Anchor {i} Confirmed?') != 'Yes':
                    continue

                anchor_date = pd.to_datetime(ticker_anchors[f'Anchor {i} Date'])
                # Use .copy() to prevent SettingWithCopyWarning
                avwap_df = intraday_df[intraday_df.index.date >= anchor_date.date()].copy()
                
                if avwap_df.empty: continue
                
                avwap_df['AVWAP'] = helpers.calculate_vwap(avwap_df)
                avwap_value = avwap_df['AVWAP'].iloc[-1]
                
                avwap_results[i] = {
                    "date": anchor_date.strftime('%Y-%m-%d'),
                    "value": avwap_value,
                    "reclaimed": current_price > avwap_value,
                    "rejected": latest_candle[config.HIGH_COL] >= avwap_value and current_price < avwap_value
                }

            # --- 6. Determine Overall Setup & Signal ---
            reclaim_count = sum(1 for r in avwap_results.values() if r['reclaimed'])
            rejection_count = sum(1 for r in avwap_results.values() if r['rejected'])
            
            signal_direction, setup_type = "None", "Weak Signal"
            if reclaim_count > 0 and rejection_count == 0:
                signal_direction = "Long"
                setup_type = f"{'Double' if reclaim_count > 1 else 'Strong'} Reclaim"
            elif rejection_count > 0 and reclaim_count == 0:
                signal_direction = "Short"
                setup_type = f"{'Double' if rejection_count > 1 else 'Strong'} Rejection"

            # --- 7. Final Validation & Scoring ---
            reclaim_quality = get_reclaim_quality(latest_candle)
            is_volume_ok = volume_vs_avg_pct >= VOLUME_SPIKE_THRESHOLD * 100
            primary_avwap = avwap_results.get(1, {}).get('value', np.nan)
            is_distance_ok = (abs(current_price - primary_avwap) / primary_avwap * 100) <= AVWAP_DISTANCE_THRESHOLD if primary_avwap else False
            
            # FIX 2: Add strong candle body requirement for Breakout Potential.
            breakout_potential = (signal_direction != "None" and is_volume_ok and is_distance_ok and reclaim_quality == "Strong")
            setup_valid = breakout_potential # Setup is valid if it has breakout potential.
            
            signal_strength = calculate_signal_strength(reclaim_count + rejection_count, is_volume_ok, reclaim_quality, is_distance_ok)

            # --- 8. Populate Full Result Dictionary ---
            result.update({
                "Current Price": helpers.format_to_two_decimal(current_price),
                "Today’s Volume": f"{today_volume:,.0f}",
                "20D Avg Volume": f"{avg_20d_volume:,.0f}",
                "Volume vs Avg %": helpers.format_to_two_decimal(volume_vs_avg_pct),
                "AVWAP Distance %": helpers.format_to_two_decimal(abs(current_price - primary_avwap) / primary_avwap * 100) if primary_avwap else "N/A",
                "Setup Type": setup_type,
                "Signal Direction": signal_direction,
                "Reason / Notes": f"{setup_type} with {reclaim_quality} quality candle.",
                "Breakout Potential?": "Yes" if breakout_potential else "No",
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Reclaim Quality": reclaim_quality,
                "Adjusted Setup Valid?": "TRUE" if setup_valid else "FALSE", # Can add more logic here later
                "Signal Strength (★)": signal_strength
            })
            
            for i in [1, 2]:
                res = avwap_results.get(i)
                result[f'Anchor {i} Date'] = res['date'] if res else "N/A"
                result[f'AVWAP {i}'] = helpers.format_to_two_decimal(res['value']) if res else "N/A"
                result[f'Reclaimed {i}?'] = "Yes" if res and res['reclaimed'] else "No"
                result[f'Rejected {i}?'] = "Yes" if res and res['rejected'] else "No"

            all_results.append(result)

        except Exception as e:
            print(f"   - ERROR processing {ticker} for AVWAP: {e}")

    # --- 9. Final Processing & Save ---
    if not all_results:
        print("--- No AVWAP signals were generated. ---")
        return
        
    final_df = pd.DataFrame(all_results)
    # Reorder columns to match the final blueprint
    final_columns = [
        "Date", "Ticker", "Anchor 1 Date", "AVWAP 1", "Reclaimed 1?", "Rejected 1?",
        "Anchor 2 Date", "AVWAP 2", "Reclaimed 2?", "Rejected 2?", "Current Price",
        "Today’s Volume", "20D Avg Volume", "Volume vs Avg %", "AVWAP Distance %",
        "Setup Type", "Signal Direction", "Reason / Notes", "Breakout Potential?",
        "Setup Valid?", "Reclaim Quality", "Adjusted Setup Valid?", "Signal Strength (★)"
    ]
    final_df = final_df.reindex(columns=final_columns, fill_value="N/A")
    
    helpers.save_signal_to_csv(final_df, 'avwap')

if __name__ == "__main__":
    run_avwap_screener()
