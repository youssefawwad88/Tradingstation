import pandas as pd
import sys
import os
import numpy as np

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    list_files_in_s3_dir,
    read_df_from_s3,
    save_df_to_s3,
    format_to_two_decimal
)

# --- Trade Plan Logic ---

def is_valid_number(value):
    """Checks if a value is a valid, non-NaN number."""
    return isinstance(value, (int, float)) and not np.isnan(value)

def calculate_execution_zone(current_price, entry_price):
    """Calculates the execution zone based on price proximity to entry."""
    if not all(is_valid_number(v) for v in [current_price, entry_price]) or entry_price == 0:
        return "N/A"
    
    distance_pct = abs(current_price - entry_price) / entry_price * 100
    if distance_pct <= 1.5:
        return "Optimal Zone"
    elif 1.5 < distance_pct <= 5.0:
        return "Chased"
    else:
        return "Far"

def calculate_gapgo_trade_plan(row: pd.Series, intraday_df: pd.DataFrame) -> dict:
    """Calculates trade plan for a Gap & Go setup."""
    plan = {'Trade Type': 'Intraday', 'Holding Days': '0-1'}
    breakout_time_str = row.get('Breakout Time') # Assuming this column exists from the full-featured screener
    if not breakout_time_str or pd.isna(breakout_time_str): return {}

    breakout_datetime = pd.to_datetime(f"{row['Date']} {breakout_time_str}")
    breakout_candle = intraday_df.asof(breakout_datetime)
    
    if row['Direction'] == 'Long':
        plan['Entry'] = breakout_candle['high']
        plan['Stop Loss'] = breakout_candle['low']
    else: # Short
        plan['Entry'] = breakout_candle['low']
        plan['Stop Loss'] = breakout_candle['high']
    return plan

def calculate_orb_trade_plan(row: pd.Series) -> dict:
    """Calculates trade plan for an ORB setup."""
    plan = {'Trade Type': 'Intraday', 'Holding Days': '0-1'}
    if row['Direction'] == 'Long':
        plan['Entry'] = row['Opening Range High']
        plan['Stop Loss'] = row['Opening Range Low']
    else: # Short
        plan['Entry'] = row['Opening Range Low']
        plan['Stop Loss'] = row['Opening Range High']
    return plan

def calculate_swing_trade_plan(row: pd.Series, daily_df: pd.DataFrame) -> dict:
    """Generic trade plan for daily-chart-based swing trades."""
    plan = {'Trade Type': 'Swing', 'Holding Days': '2-10'}
    signal_candle = daily_df[daily_df['timestamp'] == row['Date']].iloc[0]
    
    if row['Direction'] == 'Long':
        plan['Entry'] = signal_candle['high']
        plan['Stop Loss'] = signal_candle['low']
    else: # Short
        plan['Entry'] = signal_candle['low']
        plan['Stop Loss'] = signal_candle['high']
    return plan


def run_master_dashboard():
    """Main function to consolidate signals and generate trade plans from cloud storage."""
    print("\n--- Running Master Dashboard (Cloud-Aware) ---")
    
    all_valid_signals = []
    
    # --- 1. List and Load All Signal Files from Cloud Storage ---
    signal_files = list_files_in_s3_dir('data/signals/')
    
    for screener_file in signal_files:
        if screener_file.endswith('_signals.csv'):
            try:
                strategy_name = screener_file.replace('_signals.csv', '').capitalize()
                print(f"  -> Processing signals for: {strategy_name}")
                
                df = read_df_from_s3(f"data/signals/{screener_file}")
                
                valid_df = df[df['Setup Valid?'] == 'TRUE'].copy()
                if valid_df.empty:
                    print(f"       - No valid setups found.")
                    continue
                
                valid_df['Strategy'] = strategy_name
                all_valid_signals.append(valid_df)
            except Exception as e:
                print(f"     - ERROR processing signal file {screener_file}: {e}")

    if not all_valid_signals:
        print("--- No valid signals found across all screeners. ---")
        return

    consolidated_df = pd.concat(all_valid_signals, ignore_index=True)
    trade_plans = []

    # --- 2. Generate Trade Plan for Each Valid Signal ---
    for index, row in consolidated_df.iterrows():
        try:
            plan = {}
            ticker = row['Ticker']
            strategy = row['Strategy']
            
            daily_df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            intraday_df = read_df_from_s3(f"data/intraday/{ticker}_1min.csv")
            if daily_df.empty: continue

            if strategy == 'Gapgo': plan = calculate_gapgo_trade_plan(row, intraday_df)
            elif strategy == 'Orb': plan = calculate_orb_trade_plan(row)
            else: # All other screeners are daily/swing based
                plan = calculate_swing_trade_plan(row, daily_df)
            
            if plan.get('Entry') and plan.get('Stop Loss'):
                risk_per_share = abs(plan['Entry'] - plan['Stop Loss'])
                if risk_per_share > 0:
                    plan['Risk/Share'] = risk_per_share
                    if row['Direction'] == 'Long':
                        plan['Target 1 (2R)'] = plan['Entry'] + (risk_per_share * 2)
                        plan['Target 2 (3R)'] = plan['Entry'] + (risk_per_share * 3)
                    else: # Short
                        plan['Target 1 (2R)'] = plan['Entry'] - (risk_per_share * 2)
                        plan['Target 2 (3R)'] = plan['Entry'] - (risk_per_share * 3)
                    
                    current_price = read_df_from_s3(f"data/intraday/{ticker}_1min.csv")['close'].iloc[-1]
                    plan['Current Price'] = current_price
                    plan['Execution Zone'] = calculate_execution_zone(current_price, plan['Entry'])
                    plan['Tradeable?'] = 'TRUE' if plan['Execution Zone'] != 'Far' else 'FALSE'
                    
                    full_signal_data = {**row.to_dict(), **plan}
                    trade_plans.append(full_signal_data)
        except Exception as e:
            print(f"   - ERROR generating trade plan for {row['Ticker']} ({row['Strategy']}): {e}")

    if not trade_plans:
        print("--- No actionable trade plans could be generated. ---")
        return

    # --- 3. Save Final Reports to Cloud Storage ---
    final_df = pd.DataFrame(trade_plans)
    
    trade_signals_cols = [
        "Date", "Ticker", "Strategy", "Direction", "Current Price", "Entry", 
        "Stop Loss", "Risk/Share", "Target 1 (2R)", "Target 2 (3R)", 
        "Execution Zone", "Tradeable?", "Trade Type", "Holding Days"
    ]
    
    # Ensure all required columns exist, fill missing with N/A
    for col in trade_signals_cols:
        if col not in final_df.columns:
            final_df[col] = "N/A"

    trade_signals_df = final_df[trade_signals_cols]

    save_df_to_s3(trade_signals_df, 'data/trade_signals.csv')
    print("--- Master Dashboard finished. Final trade plans saved to cloud. ---")

if __name__ == "__main__":
    run_master_dashboard()
